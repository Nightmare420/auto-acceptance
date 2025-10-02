# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, xlrd
import os
import re
import time
import base64
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
import numpy as np
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from decimal import Decimal, ROUND_HALF_UP

# ---------- ENV ----------
MS_API = os.environ.get("MS_API", "https://api.moysklad.ru/api/remap/1.2")
MS_LOGIN = os.environ.get("MS_LOGIN")
MS_PASSWORD = os.environ.get("MS_PASSWORD")

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Set MS_LOGIN and MS_PASSWORD environment variables.")

def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }

# ---------- UTILS ----------
def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _norm_low(s: Optional[str]) -> str:
    return _norm(s).casefold()

async def _request_with_backoff(client: httpx.AsyncClient, method: str, url: str, **kw) -> httpx.Response:
    delay = 0.5
    for _ in range(6):
        r = await client.request(method, url, **kw)
        if r.status_code != 429:
            return r
        await asyncio.sleep(delay)
        delay = min(delay * 2, 6)
    r.raise_for_status()
    return r

# ---------- EXCEL ----------
def read_invoice_excel(file, filename: str) -> pd.DataFrame:
    ext = Path(filename).suffix.lower()
    engine = "openpyxl" if ext == ".xlsx" else ("xlrd" if ext == ".xls" else None)
    if not engine:
        raise HTTPException(400, "Разрешены только .xlsx/.xls")

    raw = pd.read_excel(file, sheet_name=0, engine=engine)

    header_row_idx = None
    for i, row in raw.iterrows():
        vals = [str(v) for v in row.values]
        if any("Артикул" in v for v in vals) and any("Цена" in v for v in vals):
            header_row_idx = i
            break
    if header_row_idx is None:
        raise HTTPException(400, "Не удалось найти строку заголовков (нужны «Артикул» и «Цена»).")

    header_row = raw.iloc[header_row_idx]
    name2col = {str(v).strip(): c for c, v in header_row.items() if pd.notna(v)}

    col_article = name2col.get("Артикул")
    col_name    = name2col.get("Товары (работы, услуги)") or name2col.get("Наименование")
    col_qty     = name2col.get("Кол.") or name2col.get("Кол-во") or name2col.get("Колич.")
    col_unit    = name2col.get("Ед.") or name2col.get("Ед")
    col_price   = name2col.get("Цена")
    col_curr    = name2col.get("Валюта")  # опционально

    data = raw.iloc[header_row_idx + 1:].copy()

    stop_idx = None
    for i, row in data.iterrows():
        n = row[col_name] if col_name in data.columns else None
        if isinstance(n, str) and ("ИТОГО" in n.upper() or "ПРЕДОПЛАТА" in n.upper()):
            stop_idx = i
            break
        if (col_article in data.columns) and pd.isna(row[col_article]) and pd.isna(n):
            stop_idx = i
            break
    if stop_idx is not None:
        data = data.loc[:stop_idx - 1]

    df = pd.DataFrame({
        "article": data[col_article] if col_article in data.columns else None,
        "name":    data[col_name]    if col_name    in data.columns else None,
        "qty":     data[col_qty]     if col_qty     in data.columns else 1,
        "unit":    data[col_unit]    if col_unit    in data.columns else None,
        "price":   data[col_price]   if col_price   in data.columns else None,
        "currency":data[col_curr]    if col_curr    in data.columns else None,
    })

    df["article"] = df["article"].astype(str).str.strip()
    df["name"]    = df["name"].astype(str).str.strip()
    df["qty"]     = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    df["price"]   = pd.to_numeric(df["price"], errors="coerce")
    df = df[(df["qty"] > 0) & (df["article"].notna()) & (df["article"] != "")]
    return df.reset_index(drop=True)

# ---------- MS lookups ----------
async def prefetch_products_by_code(client: httpx.AsyncClient, codes: Set[str]) -> Dict[str, Dict[str,Any]]:
    out: Dict[str, Dict[str,Any]] = {}
    for code in {c for c in (c.strip() for c in codes) if c}:
        url = f"{MS_API}/entity/product"
        r = await _request_with_backoff(client, "GET", url, params={"filter": f"code={code}", "limit": 1})
        rows = r.json().get("rows", [])
        if rows:
            out[_norm_low(code)] = {"meta": rows[0]["meta"], "id": rows[0]["id"]}
        await asyncio.sleep(0.05)
    return out

async def fetch_po_codes_for_agent_with_details(
    client: httpx.AsyncClient, agent_name: Optional[str], days: int = 90
) -> Tuple[Set[str], Dict[str, List[Dict[str, Any]]]]:
    """
    Возвращает:
      codes: множество всех кодов из ЗП
      details: { code_lower: [ {po_id, po_name, po_href, qty}, ... ] }
    """
    codes: Set[str] = set()
    details: Dict[str, List[Dict[str, Any]]] = {}

    agent_meta = None
    if agent_name:
        url = f"{MS_API}/entity/counterparty"
        r = await _request_with_backoff(client, "GET", url, params={"search": agent_name, "limit": 1})
        rows = r.json().get("rows", [])
        if rows:
            agent_meta = rows[0]["meta"]

    params = {"limit": 100, "expand": "positions.assortment"}
    if agent_meta:
        params["filter"] = f'agent={agent_meta["href"]}'

    next_href = f"{MS_API}/entity/purchaseorder"
    until_ts = time.time() - days * 86400

    while next_href:
        r = await _request_with_backoff(client, "GET", next_href, params=params if next_href.endswith("purchaseorder") else None)
        data = r.json()
        for row in data.get("rows", []):
            try:
                ts = time.mktime(time.strptime(row.get("updated", "")[:19], "%Y-%m-%d %H:%M:%S"))
                if ts < until_ts:
                    continue
            except Exception:
                pass

            po_id = row.get("id")
            po_name = row.get("name")
            po_href = row.get("meta", {}).get("uuidHref")

            for p in (row.get("positions", {}).get("rows") or []):
                a = p.get("assortment") or {}
                code = a.get("code")
                if not code:
                    continue
                code_key = _norm_low(code)
                codes.add(code_key)

                qty = float(p.get("quantity") or 0)
                details.setdefault(code_key, []).append({
                    "po_id": po_id,
                    "po_name": po_name,
                    "po_href": po_href,
                    "qty": qty,
                })

        next_href = data.get("meta", {}).get("nextHref")
        await asyncio.sleep(0.05)

    return codes, details

# ---------- PRICE ----------
def calc_price_kgs(
    price_raw: Optional[float],
    currency_ui: str,
    coef: float,
    usd_rate: Optional[float],
    shipping_per_kg_usd: Optional[float],
    weight_kg: float,
) -> Optional[float]:
    """
    USD: (price * coef + weight * shipping_per_kg_usd) * usd_rate
    KGS: price * coef
    """
    try:
        p = float(price_raw)
    except (TypeError, ValueError):
        return None
    if np.isnan(p):
        return None

    c = float(coef or 1.0)

    if (currency_ui or "").lower() == "usd":
        r = float(usd_rate or 0.0)
        ship = float(shipping_per_kg_usd or 0.0)
        w = float(weight_kg or 0.0)
        kgs = (p * c + w * ship) * r
        return float(Decimal(str(kgs)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    kgs = p * c
    return float(Decimal(str(kgs)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

# ---------- API ----------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent.resolve()
app.mount("/ui", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="ui")

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/ui/")

class PreviewRow(BaseModel):
    article: str
    name: Optional[str] = None
    qty: float
    unit: Optional[str] = None
    price_raw: Optional[float] = None
    price_kgs: Optional[float] = None
    product_id: Optional[str] = None
    will_create: bool = False
    po_hit: bool = False

@app.post("/import-invoice-preview/")
async def import_invoice_preview(
    file: UploadFile = File(...),
    organization_name: Optional[str] = None,
    store_name: Optional[str] = None,
    agent_name: Optional[str] = None,
    auto_create_products: bool = True,
    auto_create_agent: bool = True,
    price_currency: str = "usd",
    coef: float = 1.6,
    usd_rate: Optional[float] = None,
    shipping_per_kg_usd: Optional[float] = 15.0,
    po_days: int = 90,
):
    df = read_invoice_excel(file.file, file.filename)

    rows: List[PreviewRow] = []
    po_codes: Set[str] = set()
    po_details: Dict[str, List[Dict[str, Any]]] = {}

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        # товары по code=article
        codes = {_norm(r["article"]) for _, r in df.iterrows()}
        prod_cache = await prefetch_products_by_code(client, codes)

        # коды + детали ЗП
        if agent_name:
            po_codes, po_details = await fetch_po_codes_for_agent_with_details(client, agent_name, po_days)

        po_matches_table: List[Dict[str, Any]] = []

        for _, r in df.iterrows():
            article = _norm(r["article"])
            name    = _norm(r.get("name"))
            qty     = float(r.get("qty") or 0)
            unit    = _norm(r.get("unit"))
            price   = r.get("price")

            code_key = _norm_low(article)
            found = prod_cache.get(code_key)
            product_id = found.get("id") if found else None
            will_create = not bool(found)
            po_hit = code_key in po_codes

            # цена без веса (0) — фронт пересчитает после ввода веса
            price_kgs = calc_price_kgs(price, price_currency, coef, usd_rate, shipping_per_kg_usd, 0.0)

            rows.append(PreviewRow(
                article=article, name=name, qty=qty, unit=unit,
                price_raw=None if (price is None or np.isnan(price)) else float(price),
                price_kgs=None if price_kgs is None else round(price_kgs, 2),
                product_id=product_id, will_create=will_create, po_hit=po_hit
            ))

            if po_hit:
                po_rows = po_details.get(code_key) or []
                po_matches_table.append({
                    "article": article,
                    "name": name,
                    "qty_in_file": qty,
                    "orders": po_rows,  # [{po_id, po_name, po_href, qty}]
                })

    return {
        "rows_total": len(rows),
        "po_agent": agent_name,
        "will_create_count": sum(1 for x in rows if x.will_create),
        "will_use_existing_count": sum(1 for x in rows if not x.will_create),
        "rows": [r.model_dump() for r in rows],
        "po_matches": po_matches_table,  # 👈 новый раздел для фронта
        "note": "Вес вводится на фронте; цена в KGS пересчитывается локально по формуле.",
    }

class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]

async def resolve_refs(client: httpx.AsyncClient, *, organization_name: Optional[str], store_name: Optional[str],
                       agent_name: Optional[str], auto_create_agent: bool) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    def meta_from(entity: str, href: str) -> Dict[str, Any]:
        return {"meta": {"href": href, "type": entity, "mediaType": "application/json"}}

    refs: Dict[str, Dict[str, Any]] = {}
    created_agent = False

    # организация
    if organization_name:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/organization", params={"search": organization_name, "limit": 1})
        rows = r.json().get("rows", [])
        if not rows:
            raise HTTPException(400, "Не найдена организация.")
        refs["organization"] = meta_from("organization", rows[0]["meta"]["href"])
    else:
        raise HTTPException(400, "Укажите организацию.")

    # склад
    if store_name:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/store", params={"search": store_name, "limit": 1})
        rows = r.json().get("rows", [])
        if not rows:
            raise HTTPException(400, "Не найден склад.")
        refs["store"] = meta_from("store", rows[0]["meta"]["href"])
    else:
        raise HTTPException(400, "Укажите склад.")

    # контрагент
    if agent_name:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/counterparty", params={"search": agent_name, "limit": 1})
        rows = r.json().get("rows", [])
        if not rows and auto_create_agent:
            r2 = await _request_with_backoff(client, "POST", f"{MS_API}/entity/counterparty", json={"name": agent_name})
            rows = [r2.json()]
            created_agent = True
        if not rows:
            raise HTTPException(400, "Не найден контрагент.")
        refs["agent"] = meta_from("counterparty", rows[0]["meta"]["href"])
    else:
        raise HTTPException(400, "Укажите поставщика (контрагента).")

    return refs, created_agent

@app.post("/import-invoice-to-supply/", response_model=SupplyCreateResponse)
async def import_invoice_to_supply(
    file: UploadFile = File(...),

    # читаем из формы (или из query — FastAPI тоже подхватит)
    organization_name: Optional[str] = Form(None),
    store_name: Optional[str] = Form(None),
    agent_name: Optional[str] = Form(None),

    moment: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    vat_enabled: bool = Form(True),
    vat_included: bool = Form(True),

    auto_create_products: bool = Form(True),
    auto_create_agent: bool = Form(True),

    # ценовые настройки
    price_currency: str = Form("usd"),
    coef: float = Form(1.6),
    usd_rate: Optional[float] = Form(None),
    shipping_per_kg_usd: Optional[float] = Form(15.0),

    # данные от фронта
    weights: Optional[str] = Form(None),     # JSON: {"0": 0.5, "1": 1.2, ...}
    prices_kgs: Optional[str] = Form(None),  # JSON: {"0": 1234, "1": 550, ...}
):
    import json

    df = read_invoice_excel(file.file, file.filename)
    if df.empty:
        raise HTTPException(400, "Не обнаружены строки с товарами.")

    # распарсим клиентские веса/цены
    weights_map: Dict[int, float] = {}
    if weights:
        try:
            tmp = json.loads(weights) or {}
            for k, v in tmp.items():
                weights_map[int(k)] = float(v or 0)
        except Exception:
            pass

    prices_map: Dict[int, float] = {}
    if prices_kgs:
        try:
            tmp = json.loads(prices_kgs) or {}
            for k, v in tmp.items():
                prices_map[int(k)] = float(v or 0)
        except Exception:
            pass

    not_found: List[str] = []
    created_products: List[str] = []
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    positions: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        # ссылки на организацию/склад/контрагента (создадим контрагента при необходимости)
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name,
            store_name=store_name,
            agent_name=agent_name,
            auto_create_agent=auto_create_agent,
        )

        # заранее найдём товары по коду = артикулу
        codes = {_norm(r["article"]) for _, r in df.iterrows()}
        prod_cache = await prefetch_products_by_code(client, codes)

        # сформируем позиции для Приёмки
        for idx, r in df.iterrows():
            article = _norm(r["article"])
            name_row = _norm(r.get("name")) or article
            qty = float(r.get("qty") or 0)
            price_raw = r.get("price")

            code_key = _norm_low(article)
            found = prod_cache.get(code_key)
            meta = None

            if found:
                meta = found["meta"]
                will_use_existing.append({
                    "article": article,
                    "name": name_row,
                    "product_id": found["id"],
                })
            else:
                if not auto_create_products:
                    not_found.append(article)
                    continue
                # создаём товар (code = article)
                payload_product = {
                    "name": name_row,
                    "code": article,
                }
                # единица измерения — возьмём любую первую
                r_u = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"limit": 1})
                rows_u = r_u.json().get("rows", [])
                if rows_u:
                    payload_product["uom"] = {"meta": rows_u[0]["meta"]}
                r_c = await _request_with_backoff(client, "POST", f"{MS_API}/entity/product", json=payload_product)
                meta = {"meta": r_c.json()["meta"]}
                created_products.append(article)
                will_create.append({"article": article, "name": name_row})

            # цена позиции
            weight = float(weights_map.get(idx, 0.0))
            price_client = prices_map.get(idx)  # если фронт прислал готовую цену в сомах
            if price_client is not None and price_client >= 0:
                price_kgs = price_client
            else:
                price_kgs = calc_price_kgs(price_raw, price_currency, coef, usd_rate, shipping_per_kg_usd, weight)
                if price_kgs is None:
                    price_kgs = 0.0

            pos = {
                "assortment": meta,
                "quantity": qty,
                "price": int(round(float(price_kgs) * 100)),  # цена в копейках
            }
            positions.append(pos)

        if not positions:
            raise HTTPException(400, "Ни одной позиции не удалось сопоставить/создать.")

        payload_supply: Dict[str, Any] = {
            "applicable": True,
            "vatEnabled": bool(vat_enabled),
            "vatIncluded": bool(vat_included),
            **refs,
            "positions": positions,
        }
        if name and str(name).strip():
            payload_supply["name"] = str(name).strip()
        if moment and str(moment).strip():
            payload_supply["moment"] = str(moment).strip()

        # --- создаём Приёмку
        url = f"{MS_API}/entity/supply"
        r = await _request_with_backoff(client, "POST", url, json=payload_supply)

        # права
        if r.status_code in (401, 403):
            raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад")

        # показать причину 4xx/5xx
        if r.status_code >= 400:
            msg = None
            try:
                body = r.json()
                if isinstance(body, dict):
                    errs = body.get("errors") or []
                    if errs:
                        parts = []
                        for e in errs:
                            txt = e.get("error") or e.get("message") or "Ошибка"
                            if e.get("code"):
                                txt += f" (code {e['code']})"
                            parts.append(txt)
                        msg = "; ".join(parts)
                    elif body.get("message"):
                        msg = body["message"]
            except Exception:
                pass
            if not msg:
                msg = r.text
            raise HTTPException(status_code=r.status_code, detail=f"МС отклонил запрос: {msg}")

        # успех
        supply = r.json()

    return SupplyCreateResponse(
        created_positions=len(positions),
        not_found_items=not_found,
        created_products=created_products,
        created_agent=created_agent,
        will_create=will_create,
        will_use_existing=will_use_existing,
        supply_meta=supply["meta"],
    )