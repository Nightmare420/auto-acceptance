# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, xlrd
import os, re, time, base64, asyncio, json
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
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json", "Accept-Encoding": "gzip"}

# ---------- UTILS ----------
def _norm(s: Optional[str]) -> str:
    if s is None: return ""
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
        raise HTTPException(400, "Не удалось найти строку заголовков (нужны колонки «Артикул» и «Цена»).")

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

# ---------- MS LOOKUPS ----------
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

async def fetch_po_details_for_agent(
    client: httpx.AsyncClient, agent_name: Optional[str], days: int = 90
) -> Dict[str, Dict[str, Any]]:
    """
    Возвращает:
    {
      code_lower: {
        "orders": [{"number": "...", "href": "..."}],
        "qty": <float>,
        "ms_name": "<assortment.name>"
      }
    }
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not agent_name:
        return out

    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/counterparty",
                                    params={"search": agent_name, "limit": 1})
    rows = r.json().get("rows", [])
    agent_meta = rows[0]["meta"] if rows else None

    params = {"limit": 100, "expand": "positions.assortment"}
    if agent_meta:
        params["filter"] = f'agent={agent_meta["href"]}'

    next_href = f"{MS_API}/entity/purchaseorder"
    until_ts = time.time() - days * 86400

    while next_href:
        r = await _request_with_backoff(client, "GET", next_href,
                                        params=params if next_href.endswith("purchaseorder") else None)
        data = r.json()
        for po in data.get("rows", []):
            try:
                ts = time.mktime(time.strptime(po.get("updated", "")[:19], "%Y-%m-%d %H:%M:%S"))
                if ts < until_ts:
                    continue
            except Exception:
                pass

            po_name = po.get("name") or ""
            po_meta = po.get("meta") or {}
            po_href = po_meta.get("uuidHref") or po_meta.get("href") or ""

            for p in (po.get("positions", {}).get("rows") or []):
                a = p.get("assortment") or {}
                code = _norm(a.get("code"))
                if not code:
                    continue
                key = _norm_low(code)
                qty = float(p.get("quantity") or 0)
                ms_name = a.get("name") or ""

                bucket = out.setdefault(key, {"orders": [], "qty": 0.0, "ms_name": ""})
                bucket["qty"] += qty
                if ms_name and not bucket["ms_name"]:
                    bucket["ms_name"] = ms_name
                if po_name and po_href:
                    if not any(o.get("href") == po_href for o in bucket["orders"]):
                        bucket["orders"].append({"number": po_name, "href": po_href})

        next_href = data.get("meta", {}).get("nextHref")
        await asyncio.sleep(0.05)

    return out

# ---------- PRICE ----------
def calc_price_kgs(
    price_raw: Optional[float],
    currency_ui: str,
    coef: float,
    usd_rate: Optional[float],
    shipping_per_kg_usd: Optional[float],
    weight_kg: float,
) -> Optional[float]:
    """USD: (price*coef + weight*ship) * usd_rate;  KGS: price*coef"""
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
    else:
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

    rows_payload: List[Dict[str, Any]] = []
    po_details: Dict[str, Dict[str, Any]] = {}

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        # товары по code=article
        codes = { _norm(r["article"]) for _, r in df.iterrows() }
        prod_cache = await prefetch_products_by_code(client, codes)

        # детали по заказам поставщика
        if agent_name:
            po_details = await fetch_po_details_for_agent(client, agent_name, po_days)

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

            po_info = po_details.get(code_key) or {}
            po_hit = bool(po_info)
            ms_name = po_info.get("ms_name") or None

            price_kgs = calc_price_kgs(price, price_currency, coef, usd_rate, shipping_per_kg_usd, 0.0)

            row_obj = PreviewRow(
                article=article, name=name, qty=qty, unit=unit,
                price_raw=None if (price is None or np.isnan(price)) else float(price),
                price_kgs=None if price_kgs is None else round(price_kgs, 2),
                product_id=product_id, will_create=will_create, po_hit=po_hit
            )
            d = row_obj.model_dump()
            if ms_name:
                d["ms_name"] = ms_name
            rows_payload.append(d)

    return {
        "rows_total": len(rows_payload),
        "po_agent": agent_name,
        "will_create_count": sum(1 for x in rows_payload if x["will_create"]),
        "will_use_existing_count": sum(1 for x in rows_payload if not x["will_create"]),
        "rows": rows_payload,
        # данные для нижней таблицы «совпадения с ЗП»
        "po_matches": [
            {
                "article": code,                      # будем использовать как ключ
                "name_from_ms": v.get("ms_name") or "",
                "orders": v.get("orders") or [],      # [{number, href}]
                "qty_in_po": v.get("qty") or 0,
            }
            for code, v in po_details.items()
        ],
        "note": "Вес вводится на фронте; цена в KGS пересчитывается локально по формуле.",
    }

# ====== Ниже — эндпоинт создания Приёмки (без изменений вашей логики) ======
def _meta_from(entity: str, href: str) -> Dict[str, Any]:
    return {"meta": {"href": href, "type": entity, "mediaType": "application/json"}}

async def resolve_refs(client: httpx.AsyncClient, *, organization_name: Optional[str], store_name: Optional[str],
                       agent_name: Optional[str], auto_create_agent: bool) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    refs: Dict[str, Dict[str, Any]] = {}
    created_agent = False

    if not organization_name:
        raise HTTPException(400, "Укажите организацию.")
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/organization", params={"search": organization_name, "limit": 1})
    rows = r.json().get("rows", [])
    if not rows: raise HTTPException(400, "Не найдена организация.")
    refs["organization"] = _meta_from("organization", rows[0]["meta"]["href"])

    if not store_name:
        raise HTTPException(400, "Укажите склад.")
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/store", params={"search": store_name, "limit": 1})
    rows = r.json().get("rows", [])
    if not rows: raise HTTPException(400, "Не найден склад.")
    refs["store"] = _meta_from("store", rows[0]["meta"]["href"])

    if not agent_name:
        raise HTTPException(400, "Укажите поставщика (контрагента).")
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/counterparty", params={"search": agent_name, "limit": 1})
    rows = r.json().get("rows", [])
    if not rows and auto_create_agent:
        r2 = await _request_with_backoff(client, "POST", f"{MS_API}/entity/counterparty", json={"name": agent_name})
        rows = [r2.json()]
        created_agent = True
    if not rows: raise HTTPException(400, "Не найден контрагент.")
    refs["agent"] = _meta_from("counterparty", rows[0]["meta"]["href"])

    return refs, created_agent

class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]

@app.post("/import-invoice-to-supply/", response_model=SupplyCreateResponse)
async def import_invoice_to_supply(
    file: UploadFile = File(...),

    organization_name: Optional[str] = Form(None),
    store_name: Optional[str] = Form(None),
    agent_name: Optional[str] = Form(None),

    moment: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    vat_enabled: bool = Form(True),
    vat_included: bool = Form(True),

    auto_create_products: bool = Form(True),
    auto_create_agent: bool = Form(True),

    price_currency: str = Form("usd"),
    coef: float = Form(1.6),
    usd_rate: Optional[float] = Form(None),
    shipping_per_kg_usd: Optional[float] = Form(15.0),

    weights: Optional[str] = Form(None),     # JSON: {"0": 0.5, "1": 1.2, ...}
    prices_kgs: Optional[str] = Form(None),  # JSON: {"0": 1234, "1": 550, ...}
):
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
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name,
            store_name=store_name,
            agent_name=agent_name,
            auto_create_agent=auto_create_agent,
        )

        codes = {_norm(r["article"]) for _, r in df.iterrows()}
        prod_cache = await prefetch_products_by_code(client, codes)

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
                will_use_existing.append({"article": article, "name": name_row, "product_id": found["id"]})
            else:
                if not auto_create_products:
                    not_found.append(article)
                    continue
                payload_product = {"name": name_row, "code": article}
                r_u = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"limit": 1})
                rows_u = r_u.json().get("rows", [])
                if rows_u:
                    payload_product["uom"] = {"meta": rows_u[0]["meta"]}
                r_c = await _request_with_backoff(client, "POST", f"{MS_API}/entity/product", json=payload_product)
                meta = {"meta": r_c.json()["meta"]}
                created_products.append(article)
                will_create.append({"article": article, "name": name_row})

            weight = float(weights_map.get(idx, 0.0))
            price_client = prices_map.get(idx)
            if price_client is not None and price_client >= 0:
                price_kgs = price_client
            else:
                price_kgs = calc_price_kgs(price_raw, price_currency, coef, usd_rate, shipping_per_kg_usd, weight)
                if price_kgs is None:
                    price_kgs = 0.0

            positions.append({
                "assortment": meta,
                "quantity": qty,
                "price": int(round(float(price_kgs) * 100)),
            })

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

        url = f"{MS_API}/entity/supply"
        r = await _request_with_backoff(client, "POST", url, json=payload_supply)

        if r.status_code in (401, 403):
            raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад")

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
                            if e.get("code"): txt += f" (code {e['code']})"
                            parts.append(txt)
                        msg = "; ".join(parts)
                    elif body.get("message"):
                        msg = body["message"]
            except Exception:
                pass
            if not msg: msg = r.text
            raise HTTPException(status_code=r.status_code, detail=f"МС отклонил запрос: {msg}")

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
