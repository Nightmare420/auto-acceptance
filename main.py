from __future__ import annotations
# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, (optional) xlrd, lxml
import os
import re
import base64
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urlencode
from io import BytesIO, StringIO

import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

MS_API: str = os.environ.get("MS_API", "https://api.moysklad.ru/api/remap/1.2")
MS_LOGIN: str = os.environ.get("MS_LOGIN", "")
MS_PASSWORD: str = os.environ.get("MS_PASSWORD", "")
MANUFACTURER_ATTR_NAME = "Производитель"

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Set MS_LOGIN and MS_PASSWORD environment variables.")

app = FastAPI()
logger = logging.getLogger("uvicorn.error")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent.resolve()
app.mount("/ui", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="ui")

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/ui/")

@app.get("/health", include_in_schema=False)
async def health():
    return {"ok": True}

# ---------------- helpers ----------------
def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }

def _norm_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _norm_name_cf(s: Optional[str]) -> str:
    return _norm_text(s).casefold()

def _is_unique_name_error(resp: httpx.Response) -> bool:
    try:
        data = resp.json()
        return any(e.get("code") == 3006 for e in data.get("errors", []))
    except Exception:
        return False

def meta_from_id(entity: str, _id: str) -> Dict[str, Any]:
    return {"meta": {"href": f"{MS_API}/entity/{entity}/{_id}", "type": entity, "mediaType": "application/json"}}

def _id_from_meta_href(href: str) -> str:
    return href.rstrip("/").split("/")[-1]

def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _norm_currency(v: Optional[str], default: str = "kgs") -> str:
    if not v:
        return default
    s = _norm_text(v).lower()
    if s in {"usd", "us$", "$", "доллар", "доллары", "долл"}:
        return "usd"
    if s in {"kgs", "сом", "сомы"}:
        return "kgs"
    return default

# ---------------- Excel/CSV parsing ----------------
def _sniff_excel_kind(data: bytes) -> str:
    if data[:4] == b'PK\x03\x04':      # xlsx (zip)
        return "xlsx"
    if data[:8] == b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1':  # OLE xls
        return "xls"
    head = data[:2048].lstrip().lower()
    if head.startswith(b'<?xml') or head.startswith(b'<html') or b'<table' in head:
        return "html"
    return "unknown"

def _normalize_invoice_df(raw) -> "pd.DataFrame":
    import pandas as pd
    header_row_idx = None
    for i, row in raw.iterrows():
        vals = row.astype(str).tolist()
        if ("№" in vals) and any("Артикул" in str(v) for v in vals) and any("Цена" in str(v) for v in vals):
            header_row_idx = i
            break
    if header_row_idx is None:
        raise HTTPException(400, detail="Не удалось найти строку заголовков (№/Артикул/Цена).")

    header_row = raw.iloc[header_row_idx]
    name2col = {str(v).strip(): c for c, v in header_row.items() if pd.notna(v)}

    col_mnf = name2col.get("Производитель") or name2col.get("Производ.") or name2col.get("Производ") or name2col.get("Бренд")
    col_article = name2col.get("Артикул")
    col_name = name2col.get("Товары (работы, услуги)")
    col_qty = name2col.get("Кол.") or name2col.get("Кол-во") or name2col.get("Колич.")
    col_unit = name2col.get("Ед.") or name2col.get("Ед")
    col_price = name2col.get("Цена")
    col_sum = name2col.get("Сумма")
    col_weight = name2col.get("Вес") or name2col.get("Вес, кг") or name2col.get("Кг") or name2col.get("Масса") or name2col.get("Масса, кг")
    col_currency = name2col.get("Валюта") or name2col.get("Currency") or name2col.get("CUR")

    data = raw.iloc[header_row_idx + 1:].copy()

    stop_idx = None
    for i, row in data.iterrows():
        name_v = row[col_name] if col_name in data.columns else None
        if isinstance(name_v, str):
            up = name_v.upper()
            if "ИТОГО" in up or "ПРЕДОПЛАТА" in up:
                stop_idx = i
                break
        if pd.isna(name_v) and (col_article in data.columns) and pd.isna(row.get(col_article)):
            stop_idx = i
            break
    if stop_idx is not None:
        data = data.loc[:stop_idx - 1]

    parsed = pd.DataFrame({
        "manufacturer": data[col_mnf] if col_mnf in data.columns else None,
        "article": data[col_article] if col_article in data.columns else None,
        "name": data[col_name] if col_name in data.columns else None,
        "qty": data[col_qty] if col_qty in data.columns else 1,
        "unit": data[col_unit] if col_unit in data.columns else None,
        "price": data[col_price] if col_price in data.columns else None,
        "sum": data[col_sum] if col_sum in data.columns else None,
        "weight": data[col_weight] if (col_weight in data.columns) else 0,
        "currency": data[col_currency] if (col_currency in data.columns) else None,
    })

    for col in ("manufacturer", "article", "name", "currency"):
        parsed[col] = parsed[col].astype(str).str.strip().replace({"nan": None, "": None})
    parsed["qty"] = pd.to_numeric(parsed["qty"], errors="coerce").fillna(0)
    parsed["price"] = pd.to_numeric(parsed["price"], errors="coerce")
    parsed["weight"] = pd.to_numeric(parsed["weight"], errors="coerce").fillna(0)
    parsed = parsed[(parsed["qty"] > 0) & (parsed["article"].notna())]
    return parsed.reset_index(drop=True)

def parse_invoice_like_excel(upload, *, prefer_xlsx_csv: bool = True) -> "pd.DataFrame":
    """
    Надёжно читаем .xlsx и .csv. .xls пробуем, но если “кривой” — просим пересохранить в .xlsx.
    """
    import pandas as pd
    try:
        upload.seek(0)
    except Exception:
        pass
    data = upload.read()
    kind = _sniff_excel_kind(data)

    if kind == "xlsx":
        return _normalize_invoice_df(pd.read_excel(BytesIO(data), sheet_name=0, engine="openpyxl"))
    if kind == "html":
        for enc in ("utf-8", "cp1251", "windows-1251", "latin-1"):
            try:
                html_text = data.decode(enc)
                break
            except UnicodeDecodeError:
                html_text = None
        if html_text is None:
            html_text = data.decode("utf-8", errors="ignore")
        try:
            import pandas as pd
            tables = pd.read_html(StringIO(html_text))
            if not tables:
                raise ValueError("HTML не содержит таблиц")
            raw = max(tables, key=lambda df: df.shape[0] * df.shape[1])
            return _normalize_invoice_df(raw)
        except Exception as e:
            logger.exception("HTML-like Excel parse failed: %s", e)
            raise HTTPException(400, detail="Не удалось прочитать HTML-таблицу. Сохраните файл как .xlsx и загрузите снова.")

    if kind == "xls":
        if not prefer_xlsx_csv:
            try:
                raw = pd.read_excel(BytesIO(data), sheet_name=0, engine="xlrd")
                return _normalize_invoice_df(raw)
            except Exception as e:
                logger.exception("XLS parse failed: %s", e)
                raise HTTPException(400, detail="Файл .xls не читается надёжно. Откройте в Excel и сохраните как .xlsx, затем загрузите.")
        else:
            raise HTTPException(400, detail="Формат .xls не поддерживается надёжно. Сохраните файл как .xlsx или .csv и загрузите снова.")

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("cp1251", errors="ignore")

    try:
        import pandas as pd
        df = pd.read_csv(StringIO(text), sep=None, engine="python", header=None)
        return _normalize_invoice_df(df)
    except Exception as e:
        logger.exception("CSV parse failed: %s", e)
        raise HTTPException(400, detail="Не удалось прочитать файл. Поддерживаются .xlsx (рекомендуется) и .csv.")

async def find_single_meta(client: httpx.AsyncClient, entity: str, filter_expr: str) -> Optional[Dict[str, Any]]:
    r = await client.get(f"{MS_API}/entity/{entity}", params={"filter": filter_expr, "limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def search_single_meta(client: httpx.AsyncClient, entity: str, search: str) -> Optional[Dict[str, Any]]:
    r = await client.get(f"{MS_API}/entity/{entity}", params={"search": search, "limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def find_product_by_code(client: httpx.AsyncClient, *, code: str) -> Optional[Dict[str, Any]]:
    code = _norm_text(code)
    if not code:
        return None
    r = await client.get(f"{MS_API}/entity/product", params={"filter": f"code={code}", "limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def _fetch_product_attrs(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    r1 = await client.get(f"{MS_API}/entity/product/metadata/attributes")
    r1.raise_for_status()
    rows = r1.json().get("rows")
    if isinstance(rows, list):
        out.extend([a for a in rows if isinstance(a, dict)])
    r2 = await client.get(f"{MS_API}/entity/product/metadata")
    r2.raise_for_status()
    attrs2 = r2.json().get("attributes")
    if isinstance(attrs2, list):
        for a in attrs2:
            if isinstance(a, dict) and not any(x.get("id") == a.get("id") for x in out):
                out.append(a)
    return out

async def get_or_create_manufacturer_attr_meta(client: httpx.AsyncClient, auto_create: bool = True) -> Dict[str, Any]:
    target = _norm_name_cf(MANUFACTURER_ATTR_NAME)
    attrs = await _fetch_product_attrs(client)
    for a in attrs:
        if _norm_name_cf(a.get("name")) == target and str(a.get("type", "")).casefold() in ("string", "text"):
            return a
    if not auto_create:
        raise HTTPException(400, detail=f"В товарах нет поля '{MANUFACTURER_ATTR_NAME}' (string).")
    r = await client.post(f"{MS_API}/entity/product/metadata/attributes",
                          json={"name": MANUFACTURER_ATTR_NAME, "type": "string"})
    if r.status_code in (409, 412) and _is_unique_name_error(r):
        attrs = await _fetch_product_attrs(client)
        for a in attrs:
            if _norm_name_cf(a.get("name")) == target:
                if str(a.get("type", "")).casefold() not in ("string", "text"):
                    raise HTTPException(400, detail=f"Поле '{MANUFACTURER_ATTR_NAME}' существует, но тип '{a.get('type')}'. Нужен 'string'.")
                return a
        raise HTTPException(400, detail=f"Поле '{MANUFACTURER_ATTR_NAME}' уже существует, но API не вернуло его метаданные.")
    r.raise_for_status()
    return r.json()

async def resolve_uom_meta(client: httpx.AsyncClient, unit_hint: Optional[str]) -> Dict[str, Any]:
    candidates = []
    if unit_hint:
        unit_hint = _norm_text(unit_hint)
        candidates.extend([f"name={unit_hint}", f"code={unit_hint}"])
    candidates.extend(["name=шт", "code=796"])
    for f in candidates:
        r = await client.get(f"{MS_API}/entity/uom", params={"filter": f, "limit": 1})
        r.raise_for_status()
        rows = r.json().get("rows", [])
        if rows:
            return {"meta": rows[0]["meta"]}
    r = await client.get(f"{MS_API}/entity/uom", params={"limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    if rows:
        return {"meta": rows[0]["meta"]}
    raise HTTPException(400, detail="Не удалось определить единицу измерения (uom).")

async def create_product_with_article_and_manufacturer(
    client: httpx.AsyncClient, *, name: str, article: str, manufacturer: Optional[str], unit_hint: Optional[str]
) -> Dict[str, Any]:
    uom_meta = await resolve_uom_meta(client, unit_hint)
    payload: Dict[str, Any] = {
        "name": name or article or "Товар",
        "uom": uom_meta,
        "code": _norm_text(article) or None,
    }
    if payload.get("code") is None:
        payload.pop("code")
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
    r = await client.post(f"{MS_API}/entity/product", json=payload)
    if r.status_code in (401, 403):
        raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (товар)")
    r.raise_for_status()
    return {"meta": r.json()["meta"]}

async def resolve_product_by_article_or_create(
    client: httpx.AsyncClient,
    *, article: str, name: Optional[str], manufacturer: Optional[str], unit_hint: Optional[str],
    auto_create: bool
) -> Tuple[Optional[Dict[str, Any]], bool]:
    found = await find_product_by_code(client, code=article)
    if found:
        return found, False
    if not auto_create:
        return None, False
    meta = await create_product_with_article_and_manufacturer(
        client, name=name or article, article=article, manufacturer=manufacturer, unit_hint=unit_hint
    )
    return meta, True

async def _create_or_get_counterparty_by_name(client: httpx.AsyncClient, name: str) -> Dict[str, Any]:
    r = await client.post(f"{MS_API}/entity/counterparty", json={"name": name})
    if r.status_code in (409, 412) and _is_unique_name_error(r):
        ex = await find_single_meta(client, "counterparty", f"name={name}") or await search_single_meta(client, "counterparty", name)
        if ex:
            return ex
        raise HTTPException(400, detail=f"Контрагент '{name}' уже существует, но не найден через API.")
    if r.status_code in (401, 403):
        raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (контрагент)")
    r.raise_for_status()
    return {"meta": r.json()["meta"]}

async def resolve_refs(
    client: httpx.AsyncClient,
    *, organization_name: Optional[str], store_name: Optional[str], agent_name: Optional[str],
    organization_id: Optional[str], store_id: Optional[str], agent_id: Optional[str],
    auto_create_agent: bool,
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    refs: Dict[str, Dict[str, Any]] = {}
    created_agent = False
    if organization_id:
        refs["organization"] = meta_from_id("organization", organization_id)
    elif organization_name:
        refs["organization"] = await find_single_meta(client, "organization", f"name={organization_name}") \
                               or await search_single_meta(client, "organization", organization_name)
    if not refs.get("organization"):
        raise HTTPException(400, detail="Не найдена организация (organization). Укажите имя или ID.")

    if store_id:
        refs["store"] = meta_from_id("store", store_id)
    elif store_name:
        refs["store"] = await find_single_meta(client, "store", f"name={store_name}") \
                        or await search_single_meta(client, "store", store_name)
    if not refs.get("store"):
        raise HTTPException(400, detail="Не найден склад (store). Укажите имя или ID.")

    if agent_id:
        refs["agent"] = meta_from_id("counterparty", agent_id)
    elif agent_name:
        agent_meta = await find_single_meta(client, "counterparty", f"name={agent_name}") \
                     or await search_single_meta(client, "counterparty", agent_name)
        if not agent_meta and auto_create_agent:
            agent_meta = await _create_or_get_counterparty_by_name(client, agent_name)
            created_agent = True
        if not agent_meta:
            raise HTTPException(400, detail="Не найден контрагент (agent). Укажите имя/ID или разрешите авто-создание.")
        refs["agent"] = agent_meta
    else:
        raise HTTPException(400, detail="Не указан контрагент (agent_name или agent_id).")
    return refs, created_agent

async def prefetch_products_by_code(client: httpx.AsyncClient, codes: List[str], max_concurrency: int = 15) -> Dict[str, Dict]:
    sem = asyncio.Semaphore(max_concurrency)
    out: Dict[str, Dict] = {}

    async def _one(code: str):
        c = _norm_text(code)
        if not c:
            return
        async with sem:
            meta = await find_product_by_code(client, code=c)
            if meta:
                out[c] = meta

    await asyncio.gather(*(_one(c) for c in set(codes)))
    return out

async def _get_product_code_cached(client: httpx.AsyncClient, product_href: str, cache: Dict[str, Optional[str]]) -> Optional[str]:
    pid = _id_from_meta_href(product_href)
    if pid in cache:
        return cache[pid]
    r = await client.get(product_href)
    r.raise_for_status()
    code = (r.json() or {}).get("code")
    cache[pid] = _norm_text(code) if isinstance(code, str) else None
    return cache[pid]

async def find_supplier_orders_positions_by_code(
    client: httpx.AsyncClient, *, agent_meta: Dict[str, Any],
    limit_orders: int = 100, max_concurrency: int = 15
) -> Dict[str, List[Dict]]:
    params = {
        "filter": f'agent={agent_meta["meta"]["href"]}',
        "limit": limit_orders,
        "expand": "positions",
    }
    r = await client.get(f"{MS_API}/entity/purchaseorder?{urlencode(params)}")
    r.raise_for_status()
    rows = (r.json() or {}).get("rows", [])
    product_cache: Dict[str, Optional[str]] = {}
    code_map: Dict[str, List[Dict]] = {}
    sem = asyncio.Semaphore(max_concurrency)

    async def process_po(po: dict):
        po_id = _id_from_meta_href(po.get("meta", {}).get("href", ""))
        po_name = po.get("name") or po_id
        positions = po.get("positions", {}).get("rows", []) if isinstance(po.get("positions"), dict) else []
        for pos in positions:
            ass = pos.get("assortment", {})
            href = (ass.get("meta") or {}).get("href")
            if not href:
                continue
            async with sem:
                code = await _get_product_code_cached(client, href, product_cache)
            if not code:
                continue
            qty = float(pos.get("quantity") or 0)
            if qty <= 0:
                continue
            code_map.setdefault(code, []).append({
                "po_id": po_id,
                "po_name": po_name,
                "qty": qty,
                "position_id": pos.get("id"),
            })

    await asyncio.gather(*(process_po(po) for po in rows))
    return code_map

# ---------------- API models ----------------
class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]

# ---------------- endpoints ----------------
@app.get("/ms-product-attrs")
async def ms_product_attrs():
    async with httpx.AsyncClient(timeout=30.0, headers=ms_headers()) as client:
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
    usd_rate: Optional[float] = None,
    price_currency: Optional[str] = None,
    coef: float = 1.6,
    shipping_per_kg_usd: float = 15.0,
):
    import pandas as pd

    parsed = parse_invoice_like_excel(file.file, prefer_xlsx_csv=True)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        refs, _ = await resolve_refs(
            client,
            organization_name=organization_name, store_name=store_name, agent_name=agent_name,
            organization_id=organization_id, store_id=store_id, agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )
        _ = await get_or_create_manufacturer_attr_meta(client, auto_create=True)

        po_code_map = await find_supplier_orders_positions_by_code(client, agent_meta=refs["agent"])

        unique_codes = [str(x).strip() for x in parsed["article"].dropna().astype(str)]
        cache = await prefetch_products_by_code(client, unique_codes, max_concurrency=15)

        will_create, will_use_existing = [], []
        matches_by_article: Dict[str, List[Dict]] = {}
        calc_prices: List[Dict[str, Any]] = []

        for rec in parsed.to_dict(orient="records"):
            article = _norm_text(rec["article"])
            manufacturer = rec.get("manufacturer")
            name = rec.get("name") or article

            if cache.get(article):
                product_id = _id_from_meta_href(cache[article]["meta"]["href"])
                will_use_existing.append({"article": article, "manufacturer": manufacturer, "name": name, "product_id": product_id})
            else:
                will_create.append({"article": article, "manufacturer": manufacturer, "name": name})

            mos = po_code_map.get(article)
            if mos:
                matches_by_article[article] = mos[:10]

            row_currency = _norm_currency(rec.get("currency"), default=_norm_currency(price_currency))
            price_val = _to_float(rec.get("price")) or 0.0
            weight_val = _to_float(rec.get("weight")) or 0.0
            kgs_val: Optional[float] = None
            if row_currency == "usd":
                if usd_rate:
                    kgs_val = ((price_val * coef) + (weight_val * shipping_per_kg_usd)) * float(usd_rate)
            else:
                kgs_val = price_val * coef
            if kgs_val is not None:
                calc_prices.append({"article": article, "price_kgs": round(kgs_val, 2)})

    return {
        "rows_total": int(parsed.shape[0]),
        "will_create_count": len(will_create),
        "will_use_existing_count": len(will_use_existing),
        "will_create": will_create[:50],
        "will_use_existing": will_use_existing[:50],
        "matches_by_article": matches_by_article,   
        "calculated_prices": calc_prices[:100],     
        "note": "Показаны первые 50 позиций каждого списка.",
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
    usd_rate: Optional[float] = None,
    price_currency: Optional[str] = None,
    coef: float = 1.6,
    shipping_per_kg_usd: float = 15.0,
    purchase_order_id: Optional[str] = None,
):
    import numpy as np
    import pandas as pd

    parsed = parse_invoice_like_excel(file.file, prefer_xlsx_csv=True)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    created_products: List[str] = []
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    not_found: List[str] = []
    positions: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name, store_name=store_name, agent_name=agent_name,
            organization_id=organization_id, store_id=store_id, agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )
        _ = await get_or_create_manufacturer_attr_meta(client, auto_create=True)

        unique_codes = [str(x).strip() for x in parsed["article"].dropna().astype(str)]
        cache = await prefetch_products_by_code(client, unique_codes, max_concurrency=15)

        for rec in parsed.to_dict(orient="records"):
            article = _norm_text(rec["article"])
            name_row = rec.get("name") or article
            manufacturer = rec.get("manufacturer")
            unit_hint = rec.get("unit")
            qty = float(rec["qty"])
            price_val = _to_float(rec.get("price")) or 0.0
            weight_val = _to_float(rec.get("weight")) or 0.0

            meta = cache.get(article)
            created_new = False
            if not meta:
                meta, created_new = await resolve_product_by_article_or_create(
                    client,
                    article=article, name=name_row, manufacturer=manufacturer,
                    unit_hint=unit_hint, auto_create=auto_create_products,
                )
                if meta:
                    cache[article] = meta

            if not meta:
                not_found.append(article)
                continue

            if created_new:
                created_products.append(article)
                will_create.append({"article": article, "manufacturer": manufacturer, "name": name_row})
            else:
                product_id = _id_from_meta_href(meta["meta"]["href"])
                will_use_existing.append({"article": article, "manufacturer": manufacturer, "name": name_row, "product_id": product_id})

            row_currency = _norm_currency(rec.get("currency"), default=_norm_currency(price_currency))
            price_kgs: Optional[float] = None
            if row_currency == "usd":
                if usd_rate is None:
                    raise HTTPException(400, detail=f"Для позиции {article} валюта=USD, но не передан параметр usd_rate.")
                intermediate_usd = (price_val * coef) + (weight_val * shipping_per_kg_usd)
                price_kgs = intermediate_usd * float(usd_rate)
            else:
                price_kgs = price_val * coef

            pos = {"assortment": meta, "quantity": qty}
            if price_kgs is not None and not (isinstance(price_kgs, float) and np.isnan(price_kgs)):
                try:
                    pos["price"] = int(round(float(price_kgs) * 100))
                except Exception:
                    pass
            positions.append(pos)

        if not positions:
            raise HTTPException(400, detail=f"Ни одной позиции не сопоставлено/создано. Проблемные артикулы: {not_found[:20]}")

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
            **refs,
            "positions": positions,
        }
        if purchase_order_id:
            payload["purchaseOrder"] = meta_from_id("purchaseorder", purchase_order_id)
        if name and _norm_text(name):
            payload["name"] = _norm_text(name)
        if moment and _norm_text(moment):
            payload["moment"] = _norm_text(moment)

        r = await client.post(f"{MS_API}/entity/supply", json=payload)
        if r.status_code in (401, 403):
            raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (проверьте логин/пароль)")
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
    )
