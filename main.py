# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, xlrd
import os
import re
import asyncio
import base64
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import numpy as np
import pandas as pd
import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

# --- ENV ---
MS_API = os.environ.get("MS_API", "https://api.moysklad.ru/api/remap/1.2")
MS_LOGIN = os.environ.get("MS_LOGIN")
MS_PASSWORD = os.environ.get("MS_PASSWORD")
MANUFACTURER_ATTR_NAME = "Производитель"

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Set MS_LOGIN and MS_PASSWORD environment variables.")

# --- APP ---
app = FastAPI(title="Import Supply — MoySklad")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent.resolve()
app.mount("/ui", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="ui")

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse("/ui/")

# --- HELPERS ---

def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _norm_code(s: Optional[str]) -> str:
    return _norm(s).replace(" ", "").upper()

def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json", "Accept-Encoding": "gzip"}

def _to_number(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            if not s:
                return None
            v = float(s)
        else:
            v = float(x)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except Exception:
        return None

async def _request_with_backoff(client: httpx.AsyncClient, method: str, url: str, **kw) -> httpx.Response:
    delay = 0.6
    for _ in range(6):
        resp = await client.request(method, url, **kw)
        if resp.status_code not in (429, 500, 502, 503, 504):
            return resp
        await asyncio.sleep(delay)
        delay *= 1.6
    return resp

async def find_single_meta(client: httpx.AsyncClient, entity: str, *, filter_expr: Optional[str] = None, search: Optional[str] = None):
    params = {"limit": 1}
    if filter_expr:
        params["filter"] = filter_expr
    if search:
        params["search"] = search
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/{entity}", params=params)
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

def meta_from_id(entity: str, _id: str) -> Dict[str, Any]:
    return {"meta": {"href": f"{MS_API}/entity/{entity}/{_id}", "type": entity, "mediaType": "application/json"}}

async def resolve_refs(
    client: httpx.AsyncClient, *, organization_name: Optional[str], store_name: Optional[str],
    agent_name: Optional[str], organization_id: Optional[str], store_id: Optional[str], agent_id: Optional[str],
    auto_create_agent: bool = True
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    refs: Dict[str, Dict[str, Any]] = {}
    created_agent = False

    # organization
    if organization_id:
        refs["organization"] = meta_from_id("organization", organization_id)
    elif organization_name:
        refs["organization"] = await find_single_meta(client, "organization", filter_expr=f"name={organization_name}") \
                             or await find_single_meta(client, "organization", search=organization_name)
    if not refs.get("organization"):
        raise HTTPException(400, detail="Не найдена организация (organization). Укажите имя или ID.")

    # store
    if store_id:
        refs["store"] = meta_from_id("store", store_id)
    elif store_name:
        refs["store"] = await find_single_meta(client, "store", filter_expr=f"name={store_name}") \
                     or await find_single_meta(client, "store", search=store_name)
    if not refs.get("store"):
        raise HTTPException(400, detail="Не найден склад (store). Укажите имя или ID.")

    # agent (может быть пуст для режима «все ЗП»)
    if agent_id:
        refs["agent"] = meta_from_id("counterparty", agent_id)
    elif agent_name:
        agent = await find_single_meta(client, "counterparty", filter_expr=f"name={agent_name}") \
              or await find_single_meta(client, "counterparty", search=agent_name)
        if not agent and auto_create_agent and agent_name.strip():
            r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/counterparty", json={"name": agent_name})
            if r.status_code in (409, 412):
                agent = await find_single_meta(client, "counterparty", filter_expr=f"name={agent_name}") \
                      or await find_single_meta(client, "counterparty", search=agent_name)
            else:
                r.raise_for_status()
                agent = {"meta": r.json()["meta"]}
            created_agent = True
        if not agent:
            raise HTTPException(400, detail="Не найден контрагент (agent).")
        refs["agent"] = agent

    return refs, created_agent

# --- Excel parsing ---

def parse_invoice_like_excel(fobj, filename: str) -> pd.DataFrame:
    ext = Path(filename).suffix.lower()
    engine = "openpyxl" if ext == ".xlsx" else ("xlrd" if ext == ".xls" else None)
    if engine is None:
        raise HTTPException(400, "Разрешены только .xlsx/.xls")

    raw = pd.read_excel(fobj, sheet_name=0, engine=engine)

    header_row_idx = None
    for i, row in raw.iterrows():
        vals = row.astype(str).tolist()
        if any("Артикул" in str(v) for v in vals) and any("Цена" in str(v) for v in vals):
            header_row_idx = i
            break
    if header_row_idx is None:
        raise HTTPException(400, detail="Не удалось найти строку заголовков (нужны колонки 'Артикул' и 'Цена').")

    header_row = raw.iloc[header_row_idx]
    name2col = {str(v).strip(): c for c, v in header_row.items() if pd.notna(v)}

    col_article = name2col.get("Артикул")
    col_name    = name2col.get("Товары (работы, услуги)") or name2col.get("Наименование") or name2col.get("Название")
    col_qty     = name2col.get("Кол.") or name2col.get("Кол-во") or name2col.get("Колич.") or name2col.get("Количество")
    col_unit    = name2col.get("Ед.") or name2col.get("Ед") or name2col.get("Ед.изм")
    col_price   = name2col.get("Цена")
    col_weight  = name2col.get("Вес") or name2col.get("Масса")
    col_cur     = name2col.get("Валюта")

    data = raw.iloc[header_row_idx + 1:].copy()

    stop_idx = None
    for i, row in data.iterrows():
        if col_name in data.columns:
            name_v = row[col_name]
            if isinstance(name_v, str) and name_v and name_v.strip().upper().startswith("ИТОГО"):
                stop_idx = i
                break
        if col_article in data.columns and pd.isna(row.get(col_article)) and (col_name in data.columns) and pd.isna(row.get(col_name)):
            stop_idx = i
            break
    if stop_idx is not None:
        data = data.loc[:stop_idx - 1]

    parsed = pd.DataFrame({
        "article": data[col_article] if col_article in data.columns else None,
        "name":    data[col_name]    if col_name    in data.columns else None,
        "qty":     data[col_qty]     if col_qty     in data.columns else 1,
        "unit":    data[col_unit]    if col_unit    in data.columns else None,
        "price":   data[col_price]   if col_price   in data.columns else None,
        "weight":  data[col_weight]  if col_weight  in data.columns else 0,
        "currency":data[col_cur]     if col_cur     in data.columns else None,
    })

    for c in ("article", "name", "unit", "currency"):
        parsed[c] = parsed[c].astype(str).str.strip().replace({"nan": None, "": None})
    parsed["qty"] = pd.to_numeric(parsed["qty"], errors="coerce").fillna(0)

    parsed = parsed[(parsed["qty"] > 0) & (parsed["article"].notna())].reset_index(drop=True)
    return parsed

# --- MS: products & purchase orders ---

async def find_product_by_code(client: httpx.AsyncClient, *, code: str) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product", params={"filter": f"code={code}", "limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def create_product_with_code(
    client: httpx.AsyncClient, *, code: str, name: Optional[str], unit_hint: Optional[str], manufacturer: Optional[str]
) -> Dict[str, Any]:
    # uom
    uom = None
    for f in [f"name=шт", "code=796"]:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"filter": f, "limit": 1})
        r.raise_for_status()
        rows = r.json().get("rows", [])
        if rows:
            uom = {"meta": rows[0]["meta"]}
            break
    payload = {"name": name or code, "code": code}
    if uom:
        payload["uom"] = uom

    # мягко проставим производитель, если атрибут доступен
    if manufacturer:
        meta_attr = None
        r1 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata/attributes")
        if r1.status_code == 200:
            for a in (r1.json().get("rows") or []):
                if str(a.get("name","")).strip().casefold() == MANUFACTURER_ATTR_NAME.casefold():
                    meta_attr = a.get("meta"); break
        if not meta_attr:
            r2 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata")
            if r2.status_code == 200:
                for a in (r2.json().get("attributes") or []):
                    if str(a.get("name","")).strip().casefold() == MANUFACTURER_ATTR_NAME.casefold():
                        meta_attr = a.get("meta"); break
        if meta_attr:
            payload["attributes"] = [{
                "meta": {"href": meta_attr.get("href"), "type": meta_attr.get("type", "attributemetadata"), "mediaType": "application/json"},
                "value": manufacturer
            }]

    r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/product", json=payload)
    if r.status_code in (401, 403):
        raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (товар)")
    r.raise_for_status()
    return {"meta": r.json()["meta"]}

async def resolve_product_by_code_or_create(
    client: httpx.AsyncClient, *, code: str, name: Optional[str], unit_hint: Optional[str], manufacturer: Optional[str], auto_create: bool
) -> Tuple[Optional[Dict[str, Any]], bool]:
    found = await find_product_by_code(client, code=code)
    if found:
        return found, False
    if not auto_create:
        return None, False
    return await create_product_with_code(client, code=code, name=name, unit_hint=unit_hint, manufacturer=manufacturer), True

async def fetch_purchase_orders_codes_for_agent(client: httpx.AsyncClient, agent_meta: Dict[str, Any], limit_docs: int = 30) -> set:
    codes: set = set()
    agent_href = agent_meta["meta"]["href"]
    r = await _request_with_backoff(
        client, "GET", f"{MS_API}/entity/purchaseorder",
        params={"filter": f"agent={agent_href}", "order": "updated,desc", "limit": limit_docs}
    )
    r.raise_for_status()
    rows = r.json().get("rows", [])
    for po in rows:
        pos_meta = po.get("positions", {}).get("meta", {})
        href = pos_meta.get("href")
        if not href:
            continue
        rr = await _request_with_backoff(client, "GET", href, params={"expand": "assortment", "limit": 1000})
        if rr.status_code == 403:
            continue
        rr.raise_for_status()
        for p in rr.json().get("rows", []):
            assort = p.get("assortment") or {}
            if assort.get("meta", {}).get("type") == "product":
                pr = assort
                code = pr.get("code")
                if not code:
                    href_pr = pr.get("meta", {}).get("href")
                    if href_pr:
                        r_pr = await _request_with_backoff(client, "GET", href_pr)
                        if r_pr.status_code == 200:
                            code = r_pr.json().get("code")
                if code:
                    codes.add(_norm_code(code))
    return codes

async def fetch_purchase_orders_codes_any(client: httpx.AsyncClient, limit_docs: int = 40) -> set:
    """Последние Z заказов поставщику по всем контрагентам → множество product.code."""
    codes: set = set()
    r = await _request_with_backoff(
        client, "GET", f"{MS_API}/entity/purchaseorder",
        params={"order": "updated,desc", "limit": limit_docs}
    )
    r.raise_for_status()
    orders = r.json().get("rows", [])
    for po in orders:
        pos_meta = po.get("positions", {}).get("meta", {})
        href = pos_meta.get("href")
        if not href:
            continue
        rr = await _request_with_backoff(client, "GET", href, params={"expand": "assortment", "limit": 1000})
        if rr.status_code == 403:
            continue
        rr.raise_for_status()
        for p in rr.json().get("rows", []):
            assort = p.get("assortment") or {}
            if assort.get("meta", {}).get("type") == "product":
                pr = assort
                code = pr.get("code")
                if not code:
                    href_pr = pr.get("meta", {}).get("href")
                    if href_pr:
                        r_pr = await _request_with_backoff(client, "GET", href_pr)
                        if r_pr.status_code == 200:
                            code = r_pr.json().get("code")
                if code:
                    codes.add(_norm_code(code))
    return codes

# --- SCHEMAS ---

class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]
    po_hits: List[Dict[str, Any]] = []
    calculated_prices: List[Dict[str, Any]] = []

# --- ENDPOINTS ---

@app.post("/import-invoice-preview/")
async def import_invoice_preview(
    file: UploadFile = File(...),
    organization_name: Optional[str] = None,
    store_name: Optional[str] = None,
    agent_name: Optional[str] = Query(None, description="Контрагент для сверки с ЗП (необязательно)"),
    organization_id: Optional[str] = None,
    store_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    auto_create_products: bool = True,
    auto_create_agent: bool = True,
    price_currency: str = Query("usd", description="usd|kgs (колонка 'Валюта' в файле приоритетнее)"),
    coef: float = 1.6,
    usd_rate: Optional[float] = None,
    shipping_per_kg_usd: float = 15.0,
):
    parsed = parse_invoice_like_excel(file.file, file.filename)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    # расчёт цен для превью
    calculated_prices: List[Dict[str, Any]] = []
    for rec in parsed.to_dict(orient="records"):
        cur = (str(rec.get("currency") or price_currency or "usd")).lower()
        price = _to_number(rec.get("price"))
        w = _to_number(rec.get("weight")) or 0.0
        if price is None:
            continue
        if cur == "usd":
            if usd_rate is None:
                continue
            kgs = (price * float(coef) + w * float(shipping_per_kg_usd)) * float(usd_rate)
        else:
            kgs = price * float(coef)
        vkgs = _to_number(kgs)
        if vkgs is not None:
            calculated_prices.append({"article": _norm(rec.get("article")), "price_kgs": int(round(vkgs))})

    # сверка с ЗП (агент → только его ЗП; иначе → все последние ЗП)
    po_hits: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        refs, _ = await resolve_refs(
            client,
            organization_name=organization_name, store_name=store_name,
            agent_name=agent_name, organization_id=organization_id,
            store_id=store_id, agent_id=agent_id, auto_create_agent=auto_create_agent,
        )
        try:
            if refs.get("agent"):
                po_codes = await fetch_purchase_orders_codes_for_agent(client, refs["agent"])
            else:
                po_codes = await fetch_purchase_orders_codes_any(client, limit_docs=40)
        except httpx.HTTPStatusError:
            po_codes = set()

    for rec in parsed.to_dict(orient="records"):
        art = _norm_code(rec.get("article"))
        po_hits.append({
            "article": _norm(rec.get("article")),
            "name": _norm(rec.get("name")),
            "in_po": (art in po_codes) if po_codes else False
        })

    return {
        "rows_total": len(parsed),
        "calculated_prices": calculated_prices,
        "po_hits": po_hits,
        "note": "Сверка по product.code. Если поставщик не указан — берём последние заказы поставщику по всем контрагентам."
    }

@app.post("/import-invoice-to-supply/", response_model=SupplyCreateResponse)
async def import_invoice_to_supply(
    file: UploadFile = File(...),
    organization_name: Optional[str] = None,
    store_name: Optional[str] = None,
    agent_name: Optional[str] = None,
    organization_id: Optional[str] = None,
    store_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    moment: Optional[str] = None,
    name: Optional[str] = None,
    vat_enabled: bool = True,
    vat_included: bool = True,
    dry_run: bool = False,
    auto_create_products: bool = True,
    auto_create_agent: bool = True,
    price_currency: str = "usd",
    coef: float = 1.6,
    usd_rate: Optional[float] = None,
    shipping_per_kg_usd: float = 15.0,
):
    parsed = parse_invoice_like_excel(file.file, file.filename)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    created_products: List[str] = []
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    not_found: List[str] = []
    positions: List[Dict[str, Any]] = []
    po_hits: List[Dict[str, Any]] = []
    calculated_prices: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name, store_name=store_name,
            agent_name=agent_name, organization_id=organization_id,
            store_id=store_id, agent_id=agent_id, auto_create_agent=auto_create_agent,
        )

        try:
            if refs.get("agent"):
                po_codes = await fetch_purchase_orders_codes_for_agent(client, refs["agent"])
            else:
                po_codes = await fetch_purchase_orders_codes_any(client, limit_docs=40)
        except httpx.HTTPStatusError:
            po_codes = set()

        for rec in parsed.to_dict(orient="records"):
            code = _norm_code(rec.get("article"))
            name_row = _norm(rec.get("name")) or code
            unit_hint = rec.get("unit")
            manufacturer = None

            meta, created_new = await resolve_product_by_code_or_create(
                client, code=code, name=name_row, unit_hint=unit_hint,
                manufacturer=manufacturer, auto_create=auto_create_products,
            )
            if not meta:
                not_found.append(code)
                continue

            if created_new:
                created_products.append(code)
                will_create.append({"code": code, "name": name_row})
            else:
                prod_id = meta["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"code": code, "name": name_row, "product_id": prod_id})

            qty = float(rec.get("qty") or 0)
            pos = {"assortment": meta, "quantity": qty}

            cur = (str(rec.get("currency") or price_currency or "usd")).lower()
            price = _to_number(rec.get("price"))
            weight = _to_number(rec.get("weight")) or 0.0

            if price is not None:
                if cur == "usd":
                    if usd_rate is not None:
                        kgs = (price * float(coef) + weight * float(shipping_per_kg_usd)) * float(usd_rate)
                        vkgs = _to_number(kgs)
                        if vkgs is not None:
                            pos["price"] = int(round(vkgs * 100))
                            calculated_prices.append({"article": _norm(rec.get("article")), "price_kgs": int(round(vkgs))})
                else:
                    kgs = price * float(coef)
                    vkgs = _to_number(kgs)
                    if vkgs is not None:
                        pos["price"] = int(round(vkgs * 100))
                        calculated_prices.append({"article": _norm(rec.get("article")), "price_kgs": int(round(vkgs))})

            positions.append(pos)

            in_po = code in po_codes if po_codes else False
            po_hits.append({"article": _norm(rec.get("article")), "name": name_row, "in_po": in_po})

        if not positions:
            raise HTTPException(400, detail=f"Ни одной позиции не сопоставлено/создано. Проблемные коды: {not_found[:20]}")

        if dry_run:
            return SupplyCreateResponse(
                created_positions=len(positions),
                not_found_items=not_found,
                created_products=created_products,
                created_agent=created_agent,
                will_create=will_create, will_use_existing=will_use_existing,
                supply_meta={"dryRun": True},
                po_hits=po_hits,
                calculated_prices=calculated_prices,
            )

        payload: Dict[str, Any] = {
            "applicable": True,
            "vatEnabled": bool(vat_enabled),
            "vatIncluded": bool(vat_included),
            **refs,
            "positions": positions,
        }
        if name and name.strip():
            payload["name"] = name.strip()
        if moment and moment.strip():
            payload["moment"] = moment.strip()

        r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/supply", json=payload)
        if r.status_code in (401, 403):
            raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (проверьте логин/пароль/права)")
        r.raise_for_status()
        supply = r.json()

    return SupplyCreateResponse(
        created_positions=len(positions),
        not_found_items=not_found,
        created_products=created_products,
        created_agent=created_agent,
        will_create=will_create,
        will_use_existing=will_use_existing,
        supply_meta=supply["meta"],
        po_hits=po_hits,
        calculated_prices=calculated_prices,
    )