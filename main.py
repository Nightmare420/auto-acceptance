# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, xlrd
import os
import io
import csv
import json
import math
import time
import base64
import asyncio
import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Iterable, Set

import httpx
import numpy as np
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

# ---------- env & app ----------

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

MS_API = os.environ.get("MS_API", "https://api.moysklad.ru/api/remap/1.2")
MS_LOGIN = os.environ.get("MS_LOGIN")
MS_PASSWORD = os.environ.get("MS_PASSWORD")
MANUFACTURER_ATTR_NAME = "Производитель"

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Set MS_LOGIN and MS_PASSWORD environment variables.")

def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }

app = FastAPI(title="MoySklad Supply Import")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent.resolve()
app.mount("/ui", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="ui")

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/ui/")

# ---------- utils ----------

_sem = asyncio.Semaphore(6)  # мягкий лимит одновременных сетевых вызовов

async def _request_with_backoff(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: Optional[dict] = None,
    json_: Optional[dict] = None,
    max_retries: int = 4,
) -> httpx.Response:
    """429/5xx — экспоненциальный бэкофф с jitter."""
    attempt = 0
    while True:
        async with _sem:
            resp = await client.request(method, url, params=params, json=json_)
        if resp.status_code < 400:
            return resp
        # допускаем 401/403 сразу, чтобы было понятно про права
        if resp.status_code in (401, 403):
            resp.raise_for_status()
        if resp.status_code in (408, 409, 412, 425, 429, 500, 502, 503, 504) and attempt < max_retries:
            wait = (2 ** attempt) * 0.6 + (0.2 * np.random.random())
            await asyncio.sleep(wait)
            attempt += 1
            continue
        resp.raise_for_status()

def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _norm_lower(s: Optional[str]) -> str:
    return _norm(s).casefold()

def _to_number(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None

# ---------- product metadata helpers ----------

async def _fetch_product_attrs(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    r1 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata/attributes")
    rows = r1.json().get("rows") or []
    out.extend([a for a in rows if isinstance(a, dict)])

    r2 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata")
    rows2 = r2.json().get("attributes") or []
    for a in rows2:
        if isinstance(a, dict) and not any(x.get("id") == a.get("id") for x in out):
            out.append(a)
    return out

async def get_or_create_manufacturer_attr_meta(client: httpx.AsyncClient, auto_create: bool = True) -> Dict[str, Any]:
    target = _norm_lower(MANUFACTURER_ATTR_NAME)
    attrs = await _fetch_product_attrs(client)
    for a in attrs:
        if _norm_lower(a.get("name")) == target and str(a.get("type", "")).casefold() in ("string", "text"):
            return a

    if not auto_create:
        raise HTTPException(400, detail=f"В товарах нет поля '{MANUFACTURER_ATTR_NAME}' (string).")

    r = await _request_with_backoff(
        client, "POST", f"{MS_API}/entity/product/metadata/attributes",
        json_={"name": MANUFACTURER_ATTR_NAME, "type": "string"},
    )
    return r.json()

async def resolve_uom_meta(client: httpx.AsyncClient, unit_hint: Optional[str]) -> Dict[str, Any]:
    candidates = []
    if unit_hint:
        uh = str(unit_hint).strip()
        candidates.extend([f"name={uh}", f"code={uh}"])
    candidates.extend(["name=шт", "code=796"])  # запасные варианты
    for f in candidates:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"filter": f, "limit": 1})
        rows = r.json().get("rows", [])
        if rows:
            return {"meta": rows[0]["meta"]}
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"limit": 1})
    rows = r.json().get("rows", [])
    if rows:
        return {"meta": rows[0]["meta"]}
    raise HTTPException(400, detail="Не удалось определить единицу измерения (uom).")

# ---------- product by CODE (article from file -> code in MS) ----------

async def find_product_by_code(client: httpx.AsyncClient, code: str) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    r = await _request_with_backoff(
        client, "GET", f"{MS_API}/entity/product",
        params={"filter": f"code={code}", "limit": 1},
    )
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def create_product_with_code(
    client: httpx.AsyncClient, *, name: str, code: str, manufacturer: Optional[str], unit_hint: Optional[str]
) -> Dict[str, Any]:
    uom_meta = await resolve_uom_meta(client, unit_hint)
    payload: Dict[str, Any] = {
        "name": name or code or "Товар",
        "uom": uom_meta,
        "code": str(code),
    }
    if manufacturer:
        attr = await get_or_create_manufacturer_attr_meta(client, auto_create=True)
        attr_meta = attr.get("meta") or {}
        payload["attributes"] = [{
            "meta": {
                "href": attr_meta.get("href"),
                "type": attr_meta.get("type", "attributemetadata"),
                "mediaType": attr_meta.get("mediaType", "application/json"),
            },
            "value": str(manufacturer),
        }]

    r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/product", json_=payload)
    return {"meta": r.json()["meta"]}

async def resolve_product_by_code_or_create(
    client: httpx.AsyncClient,
    *,
    code: str,
    name: Optional[str],
    manufacturer: Optional[str],
    unit_hint: Optional[str],
    auto_create: bool
) -> Tuple[Optional[Dict[str, Any]], bool]:
    found = await find_product_by_code(client, code)
    if found:
        return found, False
    if not auto_create:
        return None, False
    meta = await create_product_with_code(
        client, name=name or code, code=code, manufacturer=manufacturer, unit_hint=unit_hint
    )
    return meta, True

# ---------- purchase orders scan (codes) ----------

async def fetch_po_codes_recent(client: httpx.AsyncClient, days_back: int = 60) -> Set[str]:
    """Сканируем ЗП за последние N дней, собираем product.code всех позиций."""
    codes: Set[str] = set()
    # Режем по страницам. Фильтр по дате (moment>=) упрощает объём.
    since = time.strftime("%Y-%m-%d", time.gmtime(time.time() - days_back * 86400))
    base = f"{MS_API}/entity/purchaseorder"
    params = {"limit": 100, "expand": "positions.assortment", "filter": f"moment>={since}"}
    offset = 0
    while True:
        r = await _request_with_backoff(client, "GET", base, params={**params, "offset": offset})
        data = r.json()
        rows = data.get("rows") or []
        if not rows:
            break
        for po in rows:
            pos = (po.get("positions") or {}).get("rows") or []
            for p in pos:
                assort = p.get("assortment") or {}
                code = assort.get("code")
                if code:
                    codes.add(str(code).strip())
        offset += data.get("meta", {}).get("limit", 100)
        if offset >= data.get("meta", {}).get("size", 0):
            break
    return codes

# ---------- Excel/CSV parsing ----------

def _read_table_from_upload(file: UploadFile) -> pd.DataFrame:
    name = file.filename or ""
    ext = Path(name).suffix.lower()
    if ext == ".csv":
        # читаем CSV в pandas, но сначала в память (FastAPI UploadFile — поток)
        raw = file.file.read()
        df = pd.read_csv(io.BytesIO(raw))
        return df
    # Excel
    engine = "openpyxl" if ext == ".xlsx" else "xlrd"
    return pd.read_excel(file.file, sheet_name=0, engine=engine)

def parse_invoice_like_table(df: pd.DataFrame) -> pd.DataFrame:
    # Определяем строку заголовка по признакам
    header_row_idx = None
    for i, row in df.iterrows():
        vals = row.astype(str).tolist()
        if any("Артикул" in str(v) for v in vals) and any("Цена" in str(v) for v in vals):
            header_row_idx = i
            break
    if header_row_idx is None:
        raise HTTPException(400, detail="Не удалось найти строку заголовков (Артикул/Цена).")

    header_row = df.iloc[header_row_idx]
    name2col = {str(v).strip(): c for c, v in header_row.items() if pd.notna(v)}

    col_code = name2col.get("Артикул") or name2col.get("Код") or name2col.get("Article") or name2col.get("Code")
    col_name = name2col.get("Товары (работы, услуги)") or name2col.get("Наименование") or name2col.get("Name")
    col_qty  = name2col.get("Кол.") or name2col.get("Кол-во") or name2col.get("Колич.") or name2col.get("Qty")
    col_unit = name2col.get("Ед.") or name2col.get("Ед") or name2col.get("Unit")
    col_price = name2col.get("Цена") or name2col.get("Price")
    col_sum = name2col.get("Сумма")
    col_mnf = name2col.get("Производитель") or name2col.get("Бренд") or name2col.get("Brand")
    col_currency = name2col.get("Валюта") or name2col.get("Currency")
    col_weight = name2col.get("Вес") or name2col.get("Weight")

    data = df.iloc[header_row_idx + 1:].copy()

    parsed = pd.DataFrame({
        "article": data[col_code] if col_code in data.columns else None,
        "name":    data[col_name] if col_name in data.columns else None,
        "qty":     data[col_qty] if col_qty in data.columns else 1,
        "unit":    data[col_unit] if col_unit in data.columns else None,
        "price":   data[col_price] if col_price in data.columns else None,
        "sum":     data[col_sum] if col_sum in data.columns else None,
        "manufacturer": data[col_mnf] if col_mnf in data.columns else None,
        "currency": data[col_currency] if col_currency in data.columns else None,
        "weight":  data[col_weight] if col_weight in data.columns else None,
    })

    for c in ("article", "name", "currency", "manufacturer", "unit"):
        parsed[c] = parsed[c].astype(str).str.strip().replace({"nan": None, "": None})
    parsed["qty"] = pd.to_numeric(parsed["qty"], errors="coerce").fillna(0)
    parsed["price"] = pd.to_numeric(parsed["price"], errors="coerce")
    parsed["weight"] = pd.to_numeric(parsed["weight"], errors="coerce")
    parsed = parsed[(parsed["qty"] > 0) & (parsed["article"].notna())]
    return parsed.reset_index(drop=True)

# ---------- price calculation ----------

def price_kgs_for_row(currency: str,
                      price: Optional[float],
                      weight: Optional[float],
                      coef: float,
                      usd_rate: Optional[float],
                      shipping_per_kg_usd: float) -> Optional[float]:
    """
    USD: (price*coef + weight*shipping_per_kg_usd) * usd_rate
    KGS: price*coef
    """
    if price is None:
        return None
    cur = (currency or "usd").strip().lower()
    w = float(weight or 0)
    if cur == "usd":
        if usd_rate is None:
            return None
        return (float(price) * float(coef) + w * float(shipping_per_kg_usd)) * float(usd_rate)
    else:
        return float(price) * float(coef)

# ---------- models ----------

class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]
    po_scan_note: Optional[str] = None

# ---------- endpoints ----------

@app.get("/ms-product-attrs")
async def ms_product_attrs():
    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        both = await _fetch_product_attrs(client)
        return [
            {"id": a.get("id"), "name": a.get("name"), "type": a.get("type"), "href": (a.get("meta") or {}).get("href")}
            for a in both if isinstance(a, dict)
        ]

@app.post("/import-invoice-preview/")
async def import_invoice_preview(
    file: UploadFile = File(...),
    organization_name: Optional[str] = None,
    store_name: Optional[str] = None,
    agent_name: Optional[str] = None,
    organization_id: Optional[str] = None,
    store_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    auto_create_products: bool = True,
    auto_create_agent: bool = True,
    price_currency: str = "usd",
    coef: float = 1.6,
    usd_rate: Optional[float] = None,
    shipping_per_kg_usd: float = 15.0,
    weights_json: Optional[str] = None,
):
    # 1) читаем таблицу
    try:
        df = _read_table_from_upload(file)
    finally:
        try:
            file.file.seek(0)
        except Exception:
            pass
    parsed = parse_invoice_like_table(df)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    # 2) разбираем веса с фронта
    weights_override: Dict[str, float] = {}
    if weights_json:
        try:
            obj = json.loads(weights_json)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if v in (None, ""):
                        continue
                    try:
                        weights_override[str(k).strip()] = float(v)
                    except Exception:
                        pass
        except Exception:
            pass

    # 3) сканируем ЗП — собираем коды (code) из заказов поставщику
    po_codes: Optional[Set[str]] = None
    po_note: Optional[str] = None
    try:
        async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
            po_codes = await fetch_po_codes_recent(client, days_back=60)
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (401, 403):
            po_note = "Нет прав на просмотр заказов поставщику — сверка отключена."
        else:
            po_note = f"Сверка ЗП недоступна ({e.response.status_code})."
    except Exception:
        po_note = "Сверка заказов временно недоступна."

    # 4) формируем строки превью
    rows_out: List[Dict[str, Any]] = []
    for rec in parsed.to_dict(orient="records"):
        code = str(rec.get("article") or "").strip()  # станет product.code
        cur_v = (rec.get("currency") or price_currency or "usd")
        price_v = _to_number(rec.get("price"))
        file_weight = _to_number(rec.get("weight"))
        weight_v = weights_override.get(code, file_weight)
        kgs = price_kgs_for_row(str(cur_v), price_v, weight_v, coef, usd_rate, shipping_per_kg_usd)
        in_po = None if po_codes is None else (code in po_codes)

        rows_out.append({
            "article": code,
            "name": _norm(rec.get("name")),
            "qty": _to_number(rec.get("qty")) or 0.0,
            "unit": _norm(rec.get("unit")),
            "currency": str(cur_v).lower(),
            "price_raw": price_v,
            "weight": weight_v,
            "price_kgs": int(round(kgs)) if kgs is not None else None,
            "in_po": in_po,
        })

    return {
        "rows_total": len(rows_out),
        "rows": rows_out,
        "po_scan_note": po_note,
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
    weights_json: Optional[str] = None,
):
    # 1) читаем таблицу
    try:
        df = _read_table_from_upload(file)
    finally:
        try:
            file.file.seek(0)
        except Exception:
            pass
    parsed = parse_invoice_like_table(df)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    # 2) разбираем веса с фронта
    weights_override: Dict[str, float] = {}
    if weights_json:
        try:
            obj = json.loads(weights_json)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if v in (None, ""):
                        continue
                    try:
                        weights_override[str(k).strip()] = float(v)
                    except Exception:
                        pass
        except Exception:
            pass

    created_products: List[str] = []
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    not_found: List[str] = []
    positions: List[Dict[str, Any]] = []

    # 3) ссылки organization/store/agent
    async with httpx.AsyncClient(timeout=90.0, headers=ms_headers()) as client:
        # organization
        if organization_id:
            org_meta = {"meta": {"href": f"{MS_API}/entity/organization/{organization_id}", "type": "organization", "mediaType": "application/json"}}
        else:
            r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/organization", params={"search": organization_name, "limit": 1})
            rows = r.json().get("rows", [])
            if not rows:
                raise HTTPException(400, detail="Не найдена организация.")
            org_meta = {"meta": rows[0]["meta"]}

        # store
        if store_id:
            store_meta = {"meta": {"href": f"{MS_API}/entity/store/{store_id}", "type": "store", "mediaType": "application/json"}}
        else:
            r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/store", params={"search": store_name, "limit": 1})
            rows = r.json().get("rows", [])
            if not rows:
                raise HTTPException(400, detail="Не найден склад.")
            store_meta = {"meta": rows[0]["meta"]}

        # agent (контрагент)
        created_agent = False
        if agent_id:
            agent_meta = {"meta": {"href": f"{MS_API}/entity/counterparty/{agent_id}", "type": "counterparty", "mediaType": "application/json"}}
        else:
            r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/counterparty", params={"search": agent_name, "limit": 1})
            rows = r.json().get("rows", [])
            if not rows and auto_create_agent and agent_name:
                r2 = await _request_with_backoff(client, "POST", f"{MS_API}/entity/counterparty", json_={"name": agent_name})
                agent_meta = {"meta": r2.json()["meta"]}
                created_agent = True
            elif rows:
                agent_meta = {"meta": rows[0]["meta"]}
            else:
                raise HTTPException(400, detail="Не найден контрагент. Укажите имя/ID или разрешите авто-создание.")

        # 4) подготовка позиций
        for rec in parsed.to_dict(orient="records"):
            code = str(rec.get("article") or "").strip()
            name_row = rec.get("name") or code
            manufacturer = rec.get("manufacturer")
            unit_hint = rec.get("unit")
            qty = float(_to_number(rec.get("qty")) or 0)
            price_raw = _to_number(rec.get("price"))
            # валюта на уровне строки приоритетнее общего переключателя
            cur = (rec.get("currency") or price_currency or "usd")
            file_weight = _to_number(rec.get("weight"))
            weight = weights_override.get(code, file_weight)

            meta, created_new = await resolve_product_by_code_or_create(
                client,
                code=code,
                name=name_row,
                manufacturer=manufacturer,
                unit_hint=unit_hint,
                auto_create=auto_create_products,
            )
            if not meta:
                not_found.append(code)
                continue

            if created_new:
                created_products.append(code)
                will_create.append({"code": code, "manufacturer": manufacturer, "name": name_row})
            else:
                product_id = meta["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"code": code, "manufacturer": manufacturer, "name": name_row, "product_id": product_id})

            kgs = price_kgs_for_row(str(cur), price_raw, weight, coef, usd_rate, shipping_per_kg_usd)

            pos = {"assortment": meta, "quantity": qty}
            if kgs is not None:
                pos["price"] = int(round(float(kgs) * 100))  # копейки

            positions.append(pos)

        if not positions:
            raise HTTPException(400, detail=f"Ни одной позиции не сопоставлено/создано. Проблемные коды: {not_found[:20]}")

        if dry_run:
            return SupplyCreateResponse(
                created_positions=len(positions),
                not_found_items=not_found,
                created_products=created_products,
                created_agent=created_agent,
                will_create=will_create,
                will_use_existing=will_use_existing,
                supply_meta={"dryRun": True},
            )

        payload: Dict[str, Any] = {
            "applicable": True,
            "vatEnabled": bool(vat_enabled),
            "vatIncluded": bool(vat_included),
            "organization": org_meta,
            "store": store_meta,
            "agent": agent_meta,
            "positions": positions,
        }
        if name is not None and str(name).strip():
            payload["name"] = str(name).strip()
        if moment is not None and str(moment).strip():
            payload["moment"] = str(moment).strip()

        r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/supply", json_=payload)
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