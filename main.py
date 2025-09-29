# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, xlrd
import os
import base64
import re
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Set, DefaultDict
from collections import defaultdict

import httpx
import numpy as np
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

# .env для локалки
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

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent.resolve()
app.mount("/ui", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="ui")

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/ui/")

# ---------- helpers ----------

def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json", "Accept-Encoding": "gzip"}

def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _is_unique_name_error(resp: httpx.Response) -> bool:
    try:
        data = resp.json()
        return any(e.get("code") == 3006 for e in data.get("errors", []))
    except Exception:
        return False

async def _request_with_backoff(client: httpx.AsyncClient, method: str, url: str, **kw) -> httpx.Response:
    # мягкий бэкофф под 429
    for attempt in range(5):
        r = await client.request(method, url, **kw)
        if r.status_code != 429:
            return r
        await asyncio.sleep(0.4 * (attempt + 1))
    r.raise_for_status()
    return r

async def find_single_meta(client: httpx.AsyncClient, entity: str, filter_expr: str) -> Optional[Dict[str, Any]]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/{entity}", params={"filter": filter_expr, "limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def search_single_meta(client: httpx.AsyncClient, entity: str, search: str) -> Optional[Dict[str, Any]]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/{entity}", params={"search": search, "limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

def meta_from_id(entity: str, _id: str) -> Dict[str, Any]:
    return {"meta": {"href": f"{MS_API}/entity/{entity}/{_id}", "type": entity, "mediaType": "application/json"}}

async def _fetch_product_attrs(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    r1 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata/attributes")
    r1.raise_for_status()
    rows = r1.json().get("rows")
    if isinstance(rows, list):
        out.extend([a for a in rows if isinstance(a, dict)])

    r2 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata")
    r2.raise_for_status()
    attrs2 = r2.json().get("attributes")
    if isinstance(attrs2, list):
        for a in attrs2:
            if isinstance(a, dict) and not any(x.get("id") == a.get("id") for x in out):
                out.append(a)
    return out

async def get_or_create_manufacturer_attr_meta(client: httpx.AsyncClient, auto_create: bool = True) -> Dict[str, Any]:
    target = _norm(MANUFACTURER_ATTR_NAME).casefold()
    attrs = await _fetch_product_attrs(client)
    for a in attrs:
        if _norm(a.get("name")).casefold() == target and str(a.get("type", "")).casefold() in ("string", "text"):
            return a

    if not auto_create:
        raise HTTPException(400, detail=f"В товарах нет поля '{MANUFACTURER_ATTR_NAME}' (string).")

    r = await client.post(f"{MS_API}/entity/product/metadata/attributes", json={"name": MANUFACTURER_ATTR_NAME, "type": "string"})
    if r.status_code in (409, 412) and _is_unique_name_error(r):
        attrs = await _fetch_product_attrs(client)
        for a in attrs:
            if _norm(a.get("name")).casefold() == target:
                if str(a.get("type", "")).casefold() not in ("string", "text"):
                    raise HTTPException(400, detail=f"Поле '{MANUFACTURER_ATTR_NAME}' существует, но тип '{a.get('type')}', нужен 'string'.")
                return a
        raise HTTPException(400, detail=f"Поле '{MANUFACTURER_ATTR_NAME}' уже существует, но метаданные не найдены.")
    r.raise_for_status()
    return r.json()

async def resolve_uom_meta(client: httpx.AsyncClient, unit_hint: Optional[str]) -> Dict[str, Any]:
    candidates = []
    if unit_hint:
        unit_hint = str(unit_hint).strip()
        candidates.extend([f"name={unit_hint}", f"code={unit_hint}"])
    candidates.extend(["name=шт", "code=796"])
    for f in candidates:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"filter": f, "limit": 1})
        r.raise_for_status()
        rows = r.json().get("rows", [])
        if rows:
            return {"meta": rows[0]["meta"]}
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    if rows:
        return {"meta": rows[0]["meta"]}
    raise HTTPException(400, detail="Не удалось определить единицу измерения (uom).")

# ---------- ключ: работаем ТОЛЬКО с product.code ----------

async def create_product_with_code_and_manufacturer(
    client: httpx.AsyncClient, *, name: str, code: str, manufacturer: Optional[str], unit_hint: Optional[str]
) -> Dict[str, Any]:
    uom_meta = await resolve_uom_meta(client, unit_hint)
    payload: Dict[str, Any] = {
        "name": name or code or "Товар",
        "uom": uom_meta,
        "code": _norm(code) if code else None,
    }
    if not payload.get("code"):
        payload.pop("code", None)

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
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        raise HTTPException(r.status_code, r.text)
    return {"meta": r.json()["meta"]}

async def find_product_by_code(client: httpx.AsyncClient, *, code: str) -> Optional[Dict[str, Any]]:
    code = _norm(code)
    if not code:
        return None
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product", params={"filter": f"code={code}", "limit": 1})
    r.raise_for_status()
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def resolve_product_by_code_or_create(
    client: httpx.AsyncClient,
    *, code: str, name: Optional[str], manufacturer: Optional[str], unit_hint: Optional[str], auto_create: bool
) -> Tuple[Optional[Dict[str, Any]], bool]:
    code = _norm(code)
    found = await find_product_by_code(client, code=code)
    if found:
        return found, False
    if not auto_create:
        return None, False
    meta = await create_product_with_code_and_manufacturer(
        client, name=name or code, code=code, manufacturer=manufacturer, unit_hint=unit_hint
    )
    return meta, True

# ---------- контрагент/организация/склад ----------

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
    organization_id: Optional[str], store_id: Optional[str], agent_id: Optional[str], auto_create_agent: bool,
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
        # агент необязателен для создания Приёмки, но обязателен для сверки с ЗП
        pass

    return refs, created_agent

# ---------- парсим Excel ----------

def parse_invoice_like_excel(upload, filename: str, engine_hint: Optional[str] = None) -> pd.DataFrame:
    ext = Path(filename).suffix.lower()
    if engine_hint:
        engine = engine_hint
    else:
        if ext == ".xlsx":
            engine = "openpyxl"
        elif ext == ".xls":
            engine = "xlrd"
        else:
            raise HTTPException(400, "Разрешены только .xlsx/.xls")

    raw = pd.read_excel(upload, sheet_name=0, engine=engine)
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
    col_weight = name2col.get("Вес") or name2col.get("Вес кг")  # если появится

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
        "weight": data[col_weight] if col_weight in data.columns else None,
    })

    for col in ("manufacturer", "article", "name"):
        parsed[col] = parsed[col].astype(str).str.strip().replace({"nan": None, "": None})
    parsed["qty"] = pd.to_numeric(parsed["qty"], errors="coerce").fillna(0)
    parsed["price"] = pd.to_numeric(parsed["price"], errors="coerce")
    parsed["weight"] = pd.to_numeric(parsed["weight"], errors="coerce")
    parsed = parsed[(parsed["qty"] > 0) & (parsed["article"].notna())]
    return parsed.reset_index(drop=True)

# ---------- тянем коды из заказов поставщику (по агенту) ----------

async def collect_po_codes_for_agent(
    client: httpx.AsyncClient, *, agent_meta: Optional[Dict[str, Any]], limit_orders: int = 50
) -> Tuple[Set[str], DefaultDict[str, List[str]], List[Dict[str, str]]]:
    """
    Возвращает:
      - множество кодов из позиций заказов поставщику этого агента
      - мапу: code -> [имена ЗП], чтобы красиво показать где найдено
      - список использованных ЗП [{id, name}]
    """
    codes: Set[str] = set()
    code2orders: DefaultDict[str, List[str]] = defaultdict(list)
    used_orders: List[Dict[str, str]] = []

    if not agent_meta:
        return codes, code2orders, used_orders  # сверку не выполняем

    agent_href = (agent_meta.get("meta") or {}).get("href")
    if not agent_href:
        return codes, code2orders, used_orders

    params = {
        "filter": f"agent={agent_href}",
        "limit": limit_orders,
        "expand": "positions.assortment",
        "order": "moment,desc"
    }
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/purchaseorder", params=params)
    r.raise_for_status()
    rows = r.json().get("rows", [])

    for po in rows:
        po_id = po.get("id")
        po_name = po.get("name") or po_id
        used_orders.append({"id": po_id, "name": po_name})
        positions_meta = (po.get("positions") or {}).get("rows")
        if positions_meta is None:
            # если не раскрылись — дотянем одной ручкой
            pos_href = ((po.get("positions") or {}).get("meta") or {}).get("href")
            if pos_href:
                pr = await _request_with_backoff(client, "GET", f"{pos_href}", params={"expand": "assortment", "limit": 1000})
                pr.raise_for_status()
                positions_meta = pr.json().get("rows", [])
            else:
                positions_meta = []

        for pos in positions_meta:
            a = pos.get("assortment") or {}
            code = _norm(a.get("code"))
            if code:
                codes.add(code)
                # не дублируем имя ЗП в списке
                if not code2orders[code] or code2orders[code][-1] != po_name:
                    code2orders[code].append(po_name)

    return codes, code2orders, used_orders

# ---------- модели ответа ----------

class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]
    # доп-инфо:
    po_seen: List[Dict[str, Any]] = []
    purchase_orders_used: List[Dict[str, str]] = []

# ---------- ручки ----------

@app.get("/ms-product-attrs")
async def ms_product_attrs():
    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        both = await _fetch_product_attrs(client)
        return [{"id": a.get("id"), "name": a.get("name"), "type": a.get("type"), "href": (a.get("meta") or {}).get("href")}
                for a in both if isinstance(a, dict)]

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
):
    # читаем файл
    parsed = parse_invoice_like_excel(file.file, file.filename)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        # контекст (организация/склад/агент)
        refs, _ = await resolve_refs(
            client,
            organization_name=organization_name, store_name=store_name, agent_name=agent_name,
            organization_id=organization_id, store_id=store_id, agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )
        # поле производитель обеспечим
        _ = await get_or_create_manufacturer_attr_meta(client, auto_create=True)

        # сверка по заказам поставщику (если указан агент)
        po_codes: Set[str] = set()
        code2orders: DefaultDict[str, List[str]] = defaultdict(list)
        purchase_orders_used: List[Dict[str, str]] = []
        try:
            po_codes, code2orders, purchase_orders_used = await collect_po_codes_for_agent(client, agent_meta=refs.get("agent"))
        except httpx.HTTPStatusError as e:
            # не падаем из-за 403/429 — просто пометим, что сверка не выполнена
            po_codes, code2orders, purchase_orders_used = set(), defaultdict(list), []

        will_create, will_use_existing = [], []
        po_seen_rows: List[Dict[str, Any]] = []

        for rec in parsed.to_dict(orient="records"):
            code = _norm(rec.get("article"))   # из файла «Артикул» = наш будущий/текущий code
            name = rec.get("name") or code
            manufacturer = rec.get("manufacturer")
            unit_hint = rec.get("unit")

            found = await find_product_by_code(client, code=code)
            if found:
                product_id = found["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"article": code, "manufacturer": manufacturer, "name": name, "product_id": product_id})
            else:
                will_create.append({"article": code, "manufacturer": manufacturer, "name": name})

            in_po = code in po_codes
            po_seen_rows.append({
                "article": code,
                "name": name,
                "in_po": in_po,
                "orders": code2orders.get(code, []),
            })

    # расчёт цен (KGS) только для отображения превью — ровно по текущим правилам
    calculated_prices: List[Dict[str, Any]] = []
    cur = (price_currency or "usd").lower()
    if cur == "usd":
        if not usd_rate:
            raise HTTPException(400, detail="Для валюты USD укажите параметр usd_rate.")
        for rec in parsed.to_dict(orient="records"):
            price_usd = rec.get("price")
            w = rec.get("weight") or 0.0
            if price_usd is None or (isinstance(price_usd, float) and np.isnan(price_usd)):
                continue
            kgs = (float(price_usd) * float(coef) + float(w or 0.0) * float(shipping_per_kg_usd)) * float(usd_rate)
            calculated_prices.append({"article": _norm(rec.get("article")), "price_kgs": int(round(kgs))})
    else:
        for rec in parsed.to_dict(orient="records"):
            price = rec.get("price")
            if price is None or (isinstance(price, float) and np.isnan(price)):
                continue
            kgs = float(price) * float(coef)
            calculated_prices.append({"article": _norm(rec.get("article")), "price_kgs": int(round(kgs))})

    return {
        "rows_total": len(parsed),
        "will_create_count": len(will_create),
        "will_use_existing_count": len(will_use_existing),
        "will_create": will_create[:200],
        "will_use_existing": will_use_existing[:200],
        "po_seen": po_seen_rows,                       # ← каждая строка и найдено ли в ЗП
        "purchase_orders_used": purchase_orders_used,  # ← какие ЗП смотрели
        "calculated_prices": calculated_prices[:200],
        "_note": "Показаны первые 200 позиций каждого списка.",
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
    po_seen_rows: List[Dict[str, Any]] = []
    purchase_orders_used: List[Dict[str, str]] = []

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name, store_name=store_name, agent_name=agent_name,
            organization_id=organization_id, store_id=store_id, agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )
        _ = await get_or_create_manufacturer_attr_meta(client, auto_create=True)

        # сверка по ЗП (опционально, если агент есть)
        po_codes: Set[str] = set()
        code2orders: DefaultDict[str, List[str]] = defaultdict(list)
        try:
            po_codes, code2orders, purchase_orders_used = await collect_po_codes_for_agent(client, agent_meta=refs.get("agent"))
        except httpx.HTTPStatusError:
            pass

        cur = (price_currency or "usd").lower()
        for rec in parsed.to_dict(orient="records"):
            code = _norm(rec.get("article"))
            name_row = rec.get("name") or code
            manufacturer = rec.get("manufacturer")
            unit_hint = rec.get("unit")
            qty = float(rec["qty"])
            price = rec.get("price")
            weight = rec.get("weight") or 0.0

            meta, created_new = await resolve_product_by_code_or_create(
                client, code=code, name=name_row, manufacturer=manufacturer, unit_hint=unit_hint,
                auto_create=auto_create_products,
            )
            if not meta:
                not_found.append(code)
                continue

            if created_new:
                created_products.append(code)
                will_create.append({"article": code, "manufacturer": manufacturer, "name": name_row})
            else:
                product_id = meta["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"article": code, "manufacturer": manufacturer, "name": name_row, "product_id": product_id})

            pos = {"assortment": meta, "quantity": qty}
            # цена пересчётом
            try:
                if cur == "usd":
                    if usd_rate is None:
                        raise HTTPException(400, detail="Для валюты USD укажите параметр usd_rate.")
                    price_usd = float(price) if price is not None and not (isinstance(price, float) and np.isnan(price)) else None
                    if price_usd is not None:
                        kgs = (price_usd * float(coef) + float(weight or 0.0) * float(shipping_per_kg_usd)) * float(usd_rate)
                        pos["price"] = int(round(kgs * 100))
                else:
                    price_kgs = float(price) if price is not None and not (isinstance(price, float) and np.isnan(price)) else None
                    if price_kgs is not None:
                        kgs = price_kgs * float(coef)
                        pos["price"] = int(round(kgs * 100))
            except Exception:
                pass
            positions.append(pos)

            # per-row «замечен ли в ЗП»
            in_po = code in po_codes
            po_seen_rows.append({"article": code, "name": name_row, "in_po": in_po, "orders": code2orders.get(code, [])})

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
                po_seen=po_seen_rows,
                purchase_orders_used=purchase_orders_used,
            )

        payload: Dict[str, Any] = {
            "applicable": True, "vatEnabled": bool(vat_enabled), "vatIncluded": bool(vat_included),
            **refs, "positions": positions,
        }
        if name: payload["name"] = str(name).strip()
        if moment: payload["moment"] = str(moment).strip()

        r = await client.post(f"{MS_API}/entity/supply", json=payload)
        if r.status_code in (401, 403):
            raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (проверьте логин/пароль)")
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            raise HTTPException(r.status_code, r.text)
        supply = r.json()

    return SupplyCreateResponse(
        created_positions=len(positions),
        not_found_items=not_found,
        created_products=created_products,
        created_agent=created_agent,
        will_create=will_create,
        will_use_existing=will_use_existing,
        supply_meta=supply["meta"],
        po_seen=po_seen_rows,
        purchase_orders_used=purchase_orders_used,
    )