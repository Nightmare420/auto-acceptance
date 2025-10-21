# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, xlrd
import os, re, time, base64, asyncio
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
from fastapi import FastAPI, File, UploadFile, HTTPException, Form

MS_API = os.environ.get("MS_API", "https://api.moysklad.ru/api/remap/1.2")
MS_LOGIN = os.environ.get("MS_LOGIN")
MS_PASSWORD = os.environ.get("MS_PASSWORD")

AMERICA_PRICE_TYPE_EXTCODE = "345befb9-8ffb-42ac-86ca-e24f76de1310"
CIS_PRICE_TYPE_EXTCODE      = "cbcf493b-55bc-11d9-848a-00112f43529a"
DONE_NAMES: Set[str] = {"выполнен", "выполнено", "выполнена", "исполнен", "completed", "done"}

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Set MS_LOGIN and MS_PASSWORD environment variables.")

def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }

def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _norm_low(s: Optional[str]) -> str:
    return _norm(s).casefold()

def is_done_state(state: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(state, dict):
        return False
    name  = (_norm(state.get("name")) or "").casefold()
    stype = (_norm(state.get("stateType")) or "").casefold()
    return (name in DONE_NAMES) or (stype == "successful")

def normalize_assortment_meta(meta: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(meta, dict):
        return None
    m = None
    if "href" in meta:
        m = meta
    elif "meta" in meta and isinstance(meta["meta"], dict) and "href" in meta["meta"]:
        m = meta["meta"]
    if not m:
        return None
    href = m.get("href")
    if not href:
        return None
    typ = m.get("type") or "product"
    mt  = m.get("mediaType") or "application/json"
    return {"meta": {"href": href, "type": typ, "mediaType": mt}}

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

async def get_product_attr_meta_by_name(client: httpx.AsyncClient, name: str) -> Optional[Dict[str, Any]]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata/attributes", params={"limit": 1000})
    rows = r.json().get("rows") or []
    target = (name or "").strip().lower()
    for a in rows:
        if isinstance(a, dict) and (a.get("name") or "").strip().lower() == target and isinstance(a.get("meta"), dict):
            return {"meta": a["meta"], "type": a.get("type")}
    return None

async def upsert_product_attr(client: httpx.AsyncClient, product_id: str, attr_meta: Dict[str, Any], value: Any) -> None:
    payload = {"attributes": [{"meta": attr_meta["meta"], "value": value}]}
    await _request_with_backoff(client, "PUT", f"{MS_API}/entity/product/{product_id}", json=payload)

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
    col_curr    = name2col.get("Валюта")
    col_manuf = (
        name2col.get("Производитель")
        or name2col.get("Производ.")
        or name2col.get("Производ")
        or next((c for k, c in name2col.items() if _norm(k).lower().startswith("производ")), None)
    )

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
        "article":      data[col_article] if col_article in data.columns else None,
        "name":         data[col_name]    if col_name    in data.columns else None,
        "qty":          data[col_qty]     if col_qty     in data.columns else 1,
        "unit":         data[col_unit]    if col_unit    in data.columns else None,
        "price":        data[col_price]   if col_price   in data.columns else None,
        "currency":     data[col_curr]    if col_curr    in data.columns else None,
        "manufacturer": data[col_manuf] if col_manuf in data.columns else None,
    })

    df["article"]      = df["article"].astype(str).str.strip()
    df["name"]         = df["name"].astype(str).str.strip()
    df["qty"]          = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    df["price"]        = pd.to_numeric(df["price"], errors="coerce")
    if "manufacturer" in df.columns:
        df["manufacturer"] = df["manufacturer"].astype(str).str.strip().replace({"nan": ""})

    df = df[(df["qty"] > 0) & (df["article"].notna()) & (df["article"] != "")]
    return df.reset_index(drop=True)

async def prefetch_products_by_code(client: httpx.AsyncClient, codes: Set[str]) -> Dict[str, Dict[str,Any]]:
    out: Dict[str, Dict[str,Any]] = {}
    for code in {c for c in (c.strip() for c in codes) if c}:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product", params={"filter": f"code={code}", "limit": 1})
        rows = r.json().get("rows", [])
        if rows:
            out[_norm_low(code)] = {"meta": rows[0]["meta"], "id": rows[0]["id"], "type": "product", "archived": bool(rows[0].get("archived"))}
        else:
            found = await find_assortment_by_code(client, code)
            if found:
                out[_norm_low(code)] = {"meta": found["meta"], "id": found["id"], "type": found["type"], "archived": found.get("archived", False)}
        await asyncio.sleep(0.05)
    return out

async def find_assortment_by_code(client: httpx.AsyncClient, code: str) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    r = await _request_with_backoff(
        client, "GET", f"{MS_API}/entity/assortment",
        params={"filter": f"code={code}", "limit": 1}
    )
    rows = r.json().get("rows", [])
    if not rows:
        return None
    row = rows[0]
    meta = row.get("meta") or {}
    return {
        "type": meta.get("type"),
        "meta": meta,
        "id": row.get("id"),
        "archived": bool(row.get("archived")),
        "name": row.get("name") or "",
    }

async def fetch_po_index_for_agent(
    client: httpx.AsyncClient, agent_name: Optional[str], days: int = 90
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    params = {"limit": 100, "expand": "positions.assortment,state"}
    next_href = f"{MS_API}/entity/purchaseorder"
    until_ts = time.time() - days * 86400

    completed_states = {
        "выполнен", "выполнено", "выполнена", "исполнен", "исполнено",
        "completed", "done", "closed", "закрыт", "закрыто"
    }

    while next_href:
        r = await _request_with_backoff(
            client, "GET", next_href,
            params=params if next_href.endswith("purchaseorder") else None
        )
        data = r.json()

        for po in data.get("rows", []):
            try:
                ts = time.mktime(time.strptime((po.get("updated", "") or "")[:19], "%Y-%m-%d %H:%M:%S"))
                if ts < until_ts:
                    continue
            except Exception:
                pass

            state_name = ""
            state_obj = po.get("state") or {}
            if isinstance(state_obj, dict):
                state_name = (state_obj.get("name") or "").strip()
            if state_name.casefold() in completed_states:
                continue

            po_number  = po.get("name") or po.get("description") or ""
            po_href    = (po.get("meta") or {}).get("href", "")
            po_created = po.get("created") or ""
            po_moment  = po.get("moment")  or ""
            po_updated = po.get("updated") or ""

            for p in (po.get("positions", {}).get("rows") or []):
                a = p.get("assortment") or {}
                code = (a.get("code") or "").strip()
                if not code:
                    continue

                key = _norm_low(code)
                bucket = out.setdefault(
                    key,
                    {"name_from_ms": a.get("name") or "", "orders": [], "qty_in_po": 0}
                )
                bucket["qty_in_po"] += 1

                if po_number or po_href:
                    if not any(o.get("number") == po_number and o.get("href") == po_href for o in bucket["orders"]):
                        bucket["orders"].append({
                            "number":  po_number,
                            "href":    po_href,
                            "state":   state_name or "",
                            "created": po_created,
                            "moment":  po_moment,
                            "updated": po_updated,
                        })

        next_href = (data.get("meta") or {}).get("nextHref")
        await asyncio.sleep(0.03)

    return out

def _q2(x: float) -> float:
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def calc_sale_kgs(price_raw: Optional[float], currency_ui: str, coef: float,
                  usd_rate: Optional[float], shipping_per_kg_usd: Optional[float],
                  weight_kg: float) -> Optional[float]:
    try:
        p = float(price_raw)
    except (TypeError, ValueError):
        return None
    if np.isnan(p):
        return None
    coef = float(coef or 1.0)
    if (currency_ui or "").lower() == "usd":
        r    = float(usd_rate or 0.0)
        ship = float(shipping_per_kg_usd or 0.0)
        w    = float(weight_kg or 0.0)
        return _q2((p * coef + w * ship) * r)
    return _q2(p * coef)

def calc_cost_kgs(price_raw: Optional[float], currency_ui: str, usd_rate: Optional[float]) -> Optional[float]:
    try:
        p = float(price_raw)
    except (TypeError, ValueError):
        return None
    if np.isnan(p):
        return None
    if (currency_ui or "").lower() == "usd":
        r = float(usd_rate or 0.0)
        if not r:
            return None
        return _q2(p * r)
    return _q2(p)

async def get_kgs_currency_meta(client: httpx.AsyncClient) -> Dict[str, Any]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/currency", params={"limit": 100})
    for row in r.json().get("rows", []):
        iso = (row.get("isoCode") or row.get("code") or "").upper()
        name = (row.get("name") or "").lower()
        if iso == "KGS" or "сом" in name:
            return {"meta": row["meta"]}
    rows = r.json().get("rows", [])
    if not rows:
        raise HTTPException(400, "Не найдена валюта KGS в аккаунте.")
    return {"meta": rows[0]["meta"]}

async def get_price_type_meta_by_external_code(
    client: httpx.AsyncClient,
    external_code: Optional[str] = None,
    fallback_name: Optional[str] = None,
) -> Dict[str, Any]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/context/companysettings/pricetype")
    items = r.json()
    if external_code:
        for pt in items:
            if pt.get("externalCode") == external_code:
                return {"meta": pt["meta"]}
    if fallback_name:
        fname = (fallback_name or "").strip().lower()
        for pt in items:
            if (pt.get("name") or "").strip().lower() == fname:
                return {"meta": pt["meta"]}
    raise HTTPException(400, f"Не найден прайс-тип (externalCode={external_code!r}, name={fallback_name!r}).")

async def update_product_prices(
    client: httpx.AsyncClient,
    product_id: str,
    cost_kgs: Optional[float],
    sale_kgs: Optional[float],
    kgs_currency_meta: Dict[str, Any],
    america_price_type_meta: Dict[str, Any],
) -> None:
    if cost_kgs is None and sale_kgs is None:
        return

    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/{product_id}")
    prod = r.json()
    sale_prices = (prod.get("salePrices") or [])[:]

    new_list = []
    added = False
    for sp in sale_prices:
        pt_meta = sp.get("priceType", {}).get("meta", {})
        if pt_meta and pt_meta.get("href") == america_price_type_meta["meta"]["href"]:
            if sale_kgs is not None:
                new_list.append({
                    "value": int(round(sale_kgs * 100)),
                    "currency": kgs_currency_meta["meta"],
                    "priceType": america_price_type_meta["meta"],
                })
            else:
                new_list.append(sp)
            added = True
        else:
            new_list.append(sp)

    if not added and sale_kgs is not None:
        new_list.append({
            "value": int(round(sale_kgs * 100)),
            "currency": kgs_currency_meta["meta"],
            "priceType": america_price_type_meta["meta"],
        })

    payload: Dict[str, Any] = {"salePrices": new_list}
    if cost_kgs is not None:
        payload["buyPrice"] = {
            "value": int(round(cost_kgs * 100)),
            "currency": kgs_currency_meta["meta"],
        }

    await _request_with_backoff(client, "PUT", f"{MS_API}/entity/product/{product_id}", json=payload)

async def update_existing_product_prices_if_needed(
    client: httpx.AsyncClient,
    product_id: str,
    sale_kgs: Optional[float],
    kgs_currency_meta: Dict[str, Any],
    america_price_type_meta: Dict[str, Any],
    cis_price_type_meta: Dict[str, Any],
) -> None:
    if sale_kgs is None:
        return

    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/{product_id}")
    prod = r.json()

    weight = prod.get("weight")
    try:
        weight_val = float(weight) if weight is not None else 0.0
    except Exception:
        weight_val = 0.0

    sale_prices = list(prod.get("salePrices") or [])
    href_america = america_price_type_meta["meta"]["href"]
    href_cis     = cis_price_type_meta["meta"]["href"]

    def _find_idx_by_href(href: str) -> int:
        return next((i for i, sp in enumerate(sale_prices)
                     if sp.get("priceType", {}).get("meta", {}).get("href") == href), -1)

    def _value_of(sp: Dict[str, Any]) -> int:
        try:
            return int(sp.get("value") or 0)
        except Exception:
            return 0

    idx_america = _find_idx_by_href(href_america)
    idx_cis     = _find_idx_by_href(href_cis)

    changed = False

    if weight_val == 0.0:
        new_sp_am = {
            "value": int(round(float(sale_kgs) * 100)),
            "currency": kgs_currency_meta["meta"],
            "priceType": america_price_type_meta["meta"],
        }
        if idx_america >= 0:
            if sale_prices[idx_america] != new_sp_am:
                sale_prices[idx_america] = new_sp_am
                changed = True
        else:
            sale_prices.append(new_sp_am)
            changed = True

    if idx_cis >= 0:
        if _value_of(sale_prices[idx_cis]) == 0:
            new_sp_cis = {
                "value": int(round(float(sale_kgs) * 100)),
                "currency": kgs_currency_meta["meta"],
                "priceType": cis_price_type_meta["meta"],
            }
            if sale_prices[idx_cis] != new_sp_cis:
                sale_prices[idx_cis] = new_sp_cis
                changed = True
    else:
        new_sp_cis = {
            "value": int(round(float(sale_kgs) * 100)),
            "currency": kgs_currency_meta["meta"],
            "priceType": cis_price_type_meta["meta"],
        }
        sale_prices.append(new_sp_cis)
        changed = True

    if changed:
        await _request_with_backoff(
            client, "PUT", f"{MS_API}/entity/product/{product_id}",
            json={"salePrices": sale_prices}
        )

async def get_uom_sht_meta(client: httpx.AsyncClient) -> Dict[str, Any]:
    for f in ("code=796", "name=шт"):
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"filter": f, "limit": 1})
        rows = r.json().get("rows", [])
        if rows:
            return {"meta": rows[0]["meta"]}
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"limit": 1})
    rows = r.json().get("rows", [])
    if rows:
        return {"meta": rows[0]["meta"]}
    raise HTTPException(400, "Не удалось получить ЕИ 'шт'.")

async def ensure_product_uom_and_weight(client: httpx.AsyncClient, product_id: str, weight_kg: Optional[float]) -> None:
    payload: Dict[str, Any] = {}
    try:
        payload["uom"] = (await get_uom_sht_meta(client))
    except Exception:
        pass
    if weight_kg is not None:
        try:
            w = float(weight_kg)
            if np.isfinite(w) and w >= 0:
                payload["weight"] = w
        except Exception:
            pass
    if payload:
        await _request_with_backoff(client, "PUT", f"{MS_API}/entity/product/{product_id}", json=payload)

app = FastAPI()
app.add_middleware(
    CORSMiddleware(
        allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
    )
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
    cost_kgs: Optional[float] = None
    sale_kgs: Optional[float] = None
    price_kgs: Optional[float] = None
    product_id: Optional[str] = None
    will_create: bool = False
    po_hit: bool = False
    manufacturer: Optional[str] = None

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
    po_matches_list: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        codes = {_norm(r["article"]) for _, r in df.iterrows()}
        prod_cache = await prefetch_products_by_code(client, codes)

        po_index: Dict[str, Dict[str, Any]] = await fetch_po_index_for_agent(client, agent_name, po_days)

        for _, r in df.iterrows():
            article = _norm(r["article"])
            name    = _norm(r.get("name"))
            qty     = float(r.get("qty") or 0)
            unit    = _norm(r.get("unit"))
            price   = r.get("price")
            manufacturer = _norm(r.get("manufacturer"))

            code_key = _norm_low(article)
            found = prod_cache.get(code_key)
            product_id = found.get("id") if found else None
            will_create = not bool(found)
            po_hit = code_key in po_index

            sale0 = calc_sale_kgs(price, price_currency, coef, usd_rate, shipping_per_kg_usd, 0.0)
            cost  = calc_cost_kgs(price, price_currency, usd_rate)

            rows.append(PreviewRow(
                article=article, name=name, qty=qty, unit=unit,
                price_raw=None if (price is None or np.isnan(price)) else float(price),
                sale_kgs=None if sale0 is None else float(sale0),
                cost_kgs=None if cost  is None else float(cost),
                price_kgs=None if sale0 is None else float(sale0),
                product_id=product_id,
                will_create=will_create,
                po_hit=po_hit,
                manufacturer=manufacturer or None,
            ))

            manufacturer_map: Dict[str, str] = {}
            for _, r in df.iterrows():
                k = _norm_low(_norm(r["article"]))
                manufacturer_map[k] = _norm(r.get("manufacturer"))

        seen: Set[str] = set()
        for _, r in df.iterrows():
            article = _norm(r["article"])
            key = _norm_low(article)
            if key in seen:
                continue
            seen.add(key)
            info = po_index.get(key)
            if not info:
                continue
            po_matches_list.append({
                "article": article,
                "name_from_ms": info.get("name_from_ms") or "",
                "orders": info.get("orders") or [],
                "qty_in_po": int(info.get("qty_in_po") or 0),
                "manufacturer": manufacturer_map.get(key) or "",
            })

    return {
        "rows_total": len(rows),
        "po_agent": agent_name,
        "po_matches_count": len(po_matches_list),
        "po_matches": po_matches_list,
        "will_create_count": sum(1 for x in rows if x.will_create),
        "will_use_existing_count": sum(1 for x in rows if not x.will_create),
        "rows": [r.model_dump() for r in rows],
        "note": "Вес вводится на фронте; цена продажи в KGS пересчитывается локально по формуле. Себестоимость = цена * курс.",
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

    if organization_name:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/organization", params={"search": organization_name, "limit": 1})
        rows = r.json().get("rows", [])
        if not rows: raise HTTPException(400, "Не найдена организация.")
        refs["organization"] = meta_from("organization", rows[0]["meta"]["href"])
    else:
        raise HTTPException(400, "Укажите организацию.")

    if store_name:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/store", params={"search": store_name, "limit": 1})
        rows = r.json().get("rows", [])
        if not rows: raise HTTPException(400, "Не найден склад.")
        refs["store"] = meta_from("store", rows[0]["meta"]["href"])
    else:
        raise HTTPException(400, "Укажите склад.")

    if agent_name:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/counterparty", params={"search": agent_name, "limit": 1})
        rows = r.json().get("rows", [])
        if not rows and auto_create_agent:
            r2 = await _request_with_backoff(client, "POST", f"{MS_API}/entity/counterparty", json={"name": agent_name})
            rows = [r2.json()]
            created_agent = True
        if not rows: raise HTTPException(400, "Не найден контрагент.")
        refs["agent"] = meta_from("counterparty", rows[0]["meta"]["href"])
    else:
        raise HTTPException(400, "Укажите поставщика (контрагента).")

    return refs, created_agent

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
    weights: Optional[str] = Form(None),
    prices_kgs: Optional[str] = Form(None),
):
    import json

    df = read_invoice_excel(file.file, file.filename)
    if df.empty:
        raise HTTPException(400, "Не обнаружены строки с товарами.")

    weights_map: Dict[int, float] = {}
    if weights:
        try:
            for k, v in (json.loads(weights) or {}).items():
                weights_map[int(k)] = float(v or 0)
        except Exception:
            pass

    sale_map: Dict[int, float] = {}
    if prices_kgs:
        try:
            for k, v in (json.loads(prices_kgs) or {}).items():
                sale_map[int(k)] = float(v or 0)
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
            organization_name=organization_name, store_name=store_name,
            agent_name=agent_name, auto_create_agent=auto_create_agent
        )

        producer_attr = await get_product_attr_meta_by_name(client, "Производитель")

        kgs_meta = await get_kgs_currency_meta(client)
        america_pt = await get_price_type_meta_by_external_code(
            client,
            AMERICA_PRICE_TYPE_EXTCODE,
            fallback_name="Цена продажи Америка",
        )

        cis_pt = await get_price_type_meta_by_external_code(
            client,
            CIS_PRICE_TYPE_EXTCODE,
            fallback_name="Цена продажи СНГ",
        )

        codes = {_norm(r["article"]) for _, r in df.iterrows()}
        prod_cache = await prefetch_products_by_code(client, codes)

        for idx, r in df.iterrows():
            article = _norm(r["article"])
            name_row = _norm(r.get("name")) or article
            qty = float(r.get("qty") or 0)
            price_raw = r.get("price")
            manufacturer = _norm(r.get("manufacturer"))
            if manufacturer.lower() in ("nan", "none", "null"):
                manufacturer = ""

            code_key = _norm_low(article)
            found = prod_cache.get(code_key)
            product_id = found["id"] if found else None
            meta = found["meta"] if found else None

            weight   = float(weights_map.get(idx, 0.0))
            sale_kgs = sale_map.get(idx)
            if sale_kgs is None:
                sale_kgs = calc_sale_kgs(price_raw, price_currency, coef, usd_rate, shipping_per_kg_usd, weight) or 0.0
            cost_kgs = calc_cost_kgs(price_raw, price_currency, usd_rate) or 0.0
            target_pt = america_pt if (price_currency or "").lower() == "usd" else cis_pt

            print(f"[ROW] code={article!r} name={name_row!r} qty={qty} "
                  f"found={'YES' if found else 'NO'} weight={weight} "
                  f"sale_kgs={sale_kgs} cost_kgs={cost_kgs}")

            if found:
                will_use_existing.append({"article": article, "name": name_row, "product_id": product_id})
                await update_existing_product_prices_if_needed(
                    client=client,
                    product_id=product_id,
                    sale_kgs=sale_kgs,
                    kgs_currency_meta=kgs_meta,
                    america_price_type_meta=america_pt,
                    cis_price_type_meta=cis_pt,
                )
            else:
                if not auto_create_products:
                    not_found.append(article)
                    continue

                payload_product: Dict[str, Any] = {
                    "name": name_row,
                    "code": article,
                    "buyPrice": {
                        "value": int(round(cost_kgs * 100)),
                        "currency": kgs_meta,
                    },
                    "salePrices": [{
                        "value": int(round(sale_kgs * 100)),
                        "currency": kgs_meta,
                        "priceType": target_pt,
                    }],
                }
                if np.isfinite(weight) and weight >= 0:
                    payload_product["weight"] = float(weight)
                payload_product["uom"] = await get_uom_sht_meta(client)
                if producer_attr and manufacturer:
                    payload_product["attributes"] = [{
                        "meta": producer_attr["meta"],
                        "value": manufacturer
                    }]

                print(f"[CREATE PRODUCT] code={article} name={name_row} PT={'AMERICA' if target_pt==america_pt else 'CIS'}")

                r_c = await _request_with_backoff(client, "POST", f"{MS_API}/entity/product", json=payload_product)
                try:
                    data_c = r_c.json()
                except Exception:
                    data_c = None
                print(f"[CREATE RESP] status={r_c.status_code} body={data_c}")

                if 200 <= r_c.status_code < 300 and isinstance(data_c, dict) and "meta" in data_c:
                    meta = data_c["meta"]
                    product_id = data_c.get("id")
                    created_products.append(article)
                    will_create.append({"article": article, "name": name_row})
                else:
                    r_find = await _request_with_backoff(
                        client, "GET", f"{MS_API}/entity/product",
                        params={"filter": f"code={article}", "limit": 1}
                    )
                    rows_find = r_find.json().get("rows", [])
                    if rows_find:
                        meta = rows_find[0]["meta"]
                        product_id = rows_find[0]["id"]
                        will_use_existing.append({"article": article, "name": name_row, "product_id": product_id})
                    else:
                        msg = "неизвестная ошибка"
                        if isinstance(data_c, dict):
                            if data_c.get("errors"):
                                parts = []
                                for e in data_c["errors"]:
                                    t = e.get("error") or e.get("message") or "Ошибка"
                                    if e.get("code"):
                                        t += f" (code {e['code']})"
                                    parts.append(t)
                                msg = "; ".join(parts)
                            elif data_c.get("message"):
                                msg = data_c["message"]
                        raise HTTPException(400, f"Не удалось создать товар {article}: {msg}")

            q = float(qty or 0)
            if not np.isfinite(q) or q <= 0:
                q = 1.0

            assortment = normalize_assortment_meta(meta)
            if not assortment:
                continue

            positions.append({
                "assortment": assortment,
                "quantity": q,
                "price": int(round((cost_kgs or 0.0) * 100)),
            })

        if not positions:
            raise HTTPException(400, "Ни одной позиции не удалось сопоставить/создать.")

        payload_supply: Dict[str, Any] = {
            "applicable": True,
            "vatEnabled": bool(vat_enabled),
            "vatIncluded": bool(vat_included),
            **refs,
        }
        if name and str(name).strip():
            payload_supply["name"] = str(name).strip()
        if moment and str(moment).strip():
            payload_supply["moment"] = str(moment).strip()

        r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/supply", json=payload_supply)
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
        supply_id = supply.get("id")
        if not supply_id:
            raise HTTPException(500, "МС вернул ответ без id приёмки.")

        r_pos = await _request_with_backoff(
            client,
            "POST",
            f"{MS_API}/entity/supply/{supply_id}/positions",
            json=positions,
        )
        if r_pos.status_code >= 400:
            msg = None
            try:
                body = r_pos.json()
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
            if not msg:
                msg = r_pos.text
            raise HTTPException(status_code=r_pos.status_code, detail=f"МС отклонил позиции: {msg}")
        print(f"[POS RESP] status={r_pos.status_code} text={r_pos.text[:500]}")

    return SupplyCreateResponse(
        created_positions=len(positions),
        not_found_items=not_found,
        created_products=created_products,
        created_agent=created_agent,
        will_create=will_create,
        will_use_existing=will_use_existing,
        supply_meta=supply["meta"],
    )