# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, numpy, pydantic, python-multipart, xlrd
import os
import io
import re
import base64
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import httpx
import numpy as np
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

# --- .env локально (не обязателен на проде/Render) ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- Конфиг / секреты из окружения ---
MS_API = os.environ.get("MS_API", "https://api.moysklad.ru/api/remap/1.2")
MS_LOGIN = os.environ.get("MS_LOGIN")
MS_PASSWORD = os.environ.get("MS_PASSWORD")
if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Set MS_LOGIN and MS_PASSWORD environment variables.")

MANUFACTURER_ATTR_NAME = "Производитель"

# --- FastAPI и статика ---
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

# --- Вспомогалки ---
def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json", "Accept-Encoding": "gzip"}

def _norm_name(s: Optional[str]) -> str:
    if s is None: return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip().casefold()

def meta_from_id(entity: str, _id: str) -> Dict[str, Any]:
    return {"meta": {"href": f"{MS_API}/entity/{entity}/{_id}", "type": entity, "mediaType": "application/json"}}

def _is_unique_name_error(resp: httpx.Response) -> bool:
    try:
        data = resp.json()
        return any(e.get("code") == 3006 for e in data.get("errors", []))
    except Exception:
        return False

# ---------- Ретраи / защита от 429 ----------
async def _request_with_backoff(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_retries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    **kwargs
) -> httpx.Response:
    attempt = 0
    while True:
        resp = await client.request(method, url, **kwargs)
        if resp.status_code < 400 or resp.status_code in (400, 404):
            return resp

        if resp.status_code in (429, 503) and attempt < max_retries:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    delay = min(float(ra), max_delay)
                except Exception:
                    delay = min(base_delay * (2 ** attempt), max_delay)
            else:
                delay = min(base_delay * (2 ** attempt), max_delay)
            await asyncio.sleep(delay)
            attempt += 1
            continue

        resp.raise_for_status()

# ---------- МойСклад: атрибуты/ед.изм ----------
async def _fetch_product_attrs(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    r1 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata/attributes")
    rows = r1.json().get("rows")
    if isinstance(rows, list):
        out.extend([a for a in rows if isinstance(a, dict)])

    r2 = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product/metadata")
    attrs2 = r2.json().get("attributes")
    if isinstance(attrs2, list):
        for a in attrs2:
            if isinstance(a, dict) and not any(x.get("id") == a.get("id") for x in out):
                out.append(a)
    return out

async def get_or_create_manufacturer_attr_meta(client: httpx.AsyncClient, auto_create: bool = True) -> Dict[str, Any]:
    target = _norm_name(MANUFACTURER_ATTR_NAME)

    attrs = await _fetch_product_attrs(client)
    for a in attrs:
        if _norm_name(a.get("name")) == target and str(a.get("type", "")).casefold() in ("string", "text"):
            return a

    if not auto_create:
        raise HTTPException(400, detail=f"В товарах нет поля '{MANUFACTURER_ATTR_NAME}' (string).")

    r = await _request_with_backoff(
        client, "POST", f"{MS_API}/entity/product/metadata/attributes",
        json={"name": MANUFACTURER_ATTR_NAME, "type": "string"}
    )

    if r.status_code in (409, 412) and _is_unique_name_error(r):
        attrs = await _fetch_product_attrs(client)
        for a in attrs:
            if _norm_name(a.get("name")) == target:
                if str(a.get("type", "")).casefold() not in ("string", "text"):
                    raise HTTPException(400, detail=f"Поле '{MANUFACTURER_ATTR_NAME}' существует, но его тип '{a.get('type')}'. Нужен тип 'string'.")
                return a
        raise HTTPException(400, detail=f"Поле '{MANUFACTURЕР_ATTR_NAME}' уже существует, но API не вернуло его метаданные.")

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
        rows = r.json().get("rows", [])
        if rows:
            return {"meta": rows[0]["meta"]}
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"limit": 1})
    rows = r.json().get("rows", [])
    if rows:
        return {"meta": rows[0]["meta"]}
    raise HTTPException(400, detail="Не удалось определить единицу измерения (uom).")

# ---------- Поиск/создание товара: СРАВНИВАЕМ ПО CODE ----------
async def find_product_by_code(client: httpx.AsyncClient, *, code: str) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    r = await _request_with_backoff(
        client, "GET", f"{MS_API}/entity/product",
        params={"filter": f"code={code}", "limit": 1}
    )
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def create_product_with_code_and_manufacturer(
    client: httpx.AsyncClient, *, name: str, code: str, manufacturer: Optional[str], unit_hint: Optional[str]
) -> Dict[str, Any]:
    uom_meta = await resolve_uom_meta(client, unit_hint)
    payload: Dict[str, Any] = {"name": name or code or "Товар", "uom": uom_meta, "code": str(code)}
    if manufacturer:
        attr = await get_or_create_manufacturer_attr_meta(client, auto_create=True)
        attr_meta = attr.get("meta") or {}
        payload["attributes"] = [{
            "meta": {"href": attr_meta.get("href"), "type": attr_meta.get("type", "attributemetadata"), "mediaType": attr_meta.get("mediaType", "application/json")},
            "value": str(manufacturer),
        }]

    r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/product", json=payload)
    if r.status_code in (401, 403):
        raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (товар)")
    r.raise_for_status()
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
    found = await find_product_by_code(client, code=code)
    if found:
        return found, False
    if not auto_create:
        return None, False
    meta = await create_product_with_code_and_manufacturer(
        client, name=name or code, code=code, manufacturer=manufacturer, unit_hint=unit_hint
    )
    return meta, True

# ---------- Контрагенты/ссылки ----------
async def find_single_meta(client: httpx.AsyncClient, entity: str, filter_expr: str) -> Optional[Dict[str, Any]]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/{entity}", params={"filter": filter_expr, "limit": 1})
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def search_single_meta(client: httpx.AsyncClient, entity: str, search: str) -> Optional[Dict[str, Any]]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/{entity}", params={"search": search, "limit": 1})
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def _create_or_get_counterparty_by_name(client: httpx.AsyncClient, name: str) -> Dict[str, Any]:
    r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/counterparty", json={"name": name})
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
    *,
    organization_name: Optional[str],
    store_name: Optional[str],
    agent_name: Optional[str],
    organization_id: Optional[str],
    store_id: Optional[str],
    agent_id: Optional[str],
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

# ---------- Чтение файла ----------
def _read_xlsx_xls(file, engine: Optional[str]) -> pd.DataFrame:
    return pd.read_excel(file, sheet_name=0, engine=engine)

def _read_csv(file) -> pd.DataFrame:
    # пробуем несколько кодировок
    raw = file.read()
    for enc in ("utf-8-sig", "cp1251", "utf-8"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, sep=None, engine="python")
        except Exception:
            continue
    # fallback
    return pd.read_csv(io.BytesIO(raw))

def parse_invoice_like_table(df: pd.DataFrame) -> pd.DataFrame:
    raw = df
    # ищем строку заголовков
    header_row_idx = None
    for i, row in raw.iterrows():
        vals = row.astype(str).tolist()
        if ("№" in vals) and any("Артикул" in str(v) for v in vals) and any("Цена" in str(v) for v in vals):
            header_row_idx = i
            break
    if header_row_idx is None:
        # если не нашли — предполагаем, что первая строка уже заголовки
        header_row_idx = 0

    header_row = raw.iloc[header_row_idx]
    name2col = {str(v).strip(): c for c, v in header_row.items() if pd.notna(v)}

    col_mnf   = name2col.get("Производитель") or name2col.get("Производ.") or name2col.get("Производ") or name2col.get("Бренд")
    col_code  = name2col.get("Артикул")  # в файле — это артикул, мы его будем сравнивать с code в МС
    col_name  = name2col.get("Товары (работы, услуги)") or name2col.get("Наименование") or name2col.get("Название")
    col_qty   = name2col.get("Кол.") or name2col.get("Кол-во") or name2col.get("Колич.") or name2col.get("Количество")
    col_unit  = name2col.get("Ед.") or name2col.get("Ед") or name2col.get("Единица")
    col_price = name2col.get("Цена")
    col_sum   = name2col.get("Сумма")
    col_curr  = name2col.get("Валюта")  # если есть — приоритетнее переключателя
    col_w     = name2col.get("Вес") or name2col.get("Вес, кг") or name2col.get("Масса")

    data = raw.iloc[header_row_idx + 1:].copy()

    # отрезаем "ИТОГО" и пустые
    stop_idx = None
    for i, row in data.iterrows():
        name_v = row[col_name] if col_name in data.columns else None
        if isinstance(name_v, str):
            up = name_v.upper()
            if "ИТОГО" in up or "ПРЕДОПЛАТА" in up:
                stop_idx = i
                break
        if pd.isna(name_v) and (col_code in data.columns) and pd.isna(row.get(col_code)):
            stop_idx = i
            break
    if stop_idx is not None:
        data = data.loc[:stop_idx - 1]

    parsed = pd.DataFrame({
        "manufacturer": data[col_mnf] if col_mnf in data.columns else None,
        "article": data[col_code] if col_code in data.columns else None,   # <- из файла
        "name": data[col_name] if col_name in data.columns else None,
        "qty": data[col_qty] if col_qty in data.columns else 1,
        "unit": data[col_unit] if col_unit in data.columns else None,
        "price": data[col_price] if col_price in data.columns else None,
        "sum": data[col_sum] if col_sum in data.columns else None,
        "currency": data[col_curr] if col_curr in data.columns else None,
        "weight": data[col_w] if col_w in data.columns else 0,
    })

    for col in ("manufacturer", "article", "name", "currency"):
        parsed[col] = parsed[col].astype(str).str.strip().replace({"nan": None, "": None})

    parsed["qty"] = pd.to_numeric(parsed["qty"], errors="coerce").fillna(0)
    parsed["price"] = pd.to_numeric(parsed["price"], errors="coerce")
    parsed["weight"] = pd.to_numeric(parsed["weight"], errors="coerce").fillna(0)
    parsed = parsed[(parsed["qty"] > 0) & (parsed["article"].notna())]
    return parsed.reset_index(drop=True)

def parse_upload_to_df(upload: UploadFile) -> Tuple[pd.DataFrame, str]:
    ext = Path(upload.filename).suffix.lower()
    if ext == ".xlsx":
        return _read_xlsx_xls(upload.file, engine="openpyxl"), "xlsx"
    elif ext == ".xls":
        return _read_xlsx_xls(upload.file, engine="xlrd"), "xls"
    elif ext == ".csv":
        return _read_csv(upload.file), "csv"
    else:
        raise HTTPException(400, "Разрешены только .xlsx/.xls/.csv")

# ---------- Параллельный префетч по CODE с низким конкарренси ----------
async def prefetch_products_by_code(
    client: httpx.AsyncClient,
    codes: List[str],
    max_concurrency: int = 3
) -> Dict[str, Dict[str, Any]]:
    sem = asyncio.Semaphore(max_concurrency)
    cache: Dict[str, Dict[str, Any]] = {}

    async def _one(c: str):
        async with sem:
            await asyncio.sleep(0.05)  # небольшой джиттер
            meta = await find_product_by_code(client, code=c)
            if meta:
                cache[c] = meta

    await asyncio.gather(*(_one(c) for c in set(codes)))
    return cache

# ---------- Расчёт цены ----------
def compute_price_kgs(row: Dict[str, Any], *, default_currency: str, coef: float, usd_rate: Optional[float], shipping_per_kg_usd: Optional[float]) -> Optional[float]:
    """
    Правила:
    - Если валюта USD: итог = (price * coef) + (weight * shippingUSD); затем * usd_rate
    - Если валюта KGS: итог = price * coef
    Возвращаем сумму в СОМах (float) или None, если нет цены.
    """
    price = row.get("price")
    if price is None or (isinstance(price, float) and np.isnan(price)):
        return None

    currency = (row.get("currency") or "").strip().lower()
    if currency not in ("usd", "kgs"):
        currency = default_currency.lower()

    weight = float(row.get("weight") or 0)

    if currency == "usd":
        if not usd_rate:
            return None
        ship = float(shipping_per_kg_usd or 0)
        base_usd = price * float(coef) + weight * ship
        return float(base_usd) * float(usd_rate)
    else:
        return float(price) * float(coef)

# ---------- Схемы ответов ----------
class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]

# ---------- Эндпоинты ----------
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
    price_currency: str = "usd",   # "usd" | "kgs"
    usd_rate: Optional[float] = None,
    coef: float = 1.6,
    shipping_per_kg_usd: float = 15.0,
    auto_create_products: bool = True,
    auto_create_agent: bool = True,
):
    df_raw, _ = parse_upload_to_df(file)
    parsed = parse_invoice_like_table(df_raw)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    codes = [str(r["article"]) for r in parsed.to_dict(orient="records")]
    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        # в превью только префетчим и смотрим что найдётся
        cache = await prefetch_products_by_code(client, codes, max_concurrency=3)

    will_create, will_use_existing = [], []
    calc_prices = []

    for rec in parsed.to_dict(orient="records"):
        article = str(rec["article"])
        name = rec.get("name") or article
        manufacturer = rec.get("manufacturer")

        if article in cache:
            product_id = cache[article]["meta"]["href"].rstrip("/").split("/")[-1]
            will_use_existing.append({"article": article, "manufacturer": manufacturer, "name": name, "product_id": product_id})
        else:
            will_create.append({"article": article, "manufacturer": manufacturer, "name": name})

        price_kgs = compute_price_kgs(
            rec,
            default_currency=price_currency,
            coef=coef,
            usd_rate=usd_rate,
            shipping_per_kg_usd=shipping_per_kg_usd,
        )
        if price_kgs is not None:
            calc_prices.append({"article": article, "price_kgs": round(float(price_kgs), 2)})

    return {
        "rows_total": len(parsed),
        "will_create_count": len(will_create),
        "will_use_existing_count": len(will_use_existing),
        "will_create": will_create[:200],
        "will_use_existing": will_use_existing[:200],
        "calculated_prices": calc_prices[:500],
        "note": "Артикул из файла сравнивается с 'code' в МойСклад. Цены пересчитаны в сомах по заданным правилам.",
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
    price_currency: str = "usd",
    usd_rate: Optional[float] = None,
    coef: float = 1.6,
    shipping_per_kg_usd: float = 15.0,
    auto_create_products: bool = True,
    auto_create_agent: bool = True,
):
    df_raw, _ = parse_upload_to_df(file)
    parsed = parse_invoice_like_table(df_raw)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    created_products: List[str] = []
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    not_found: List[str] = []
    positions: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=90.0, headers=ms_headers()) as client:
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name, store_name=store_name, agent_name=agent_name,
            organization_id=organization_id, store_id=store_id, agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )
        _ = await get_or_create_manufacturer_attr_meta(client, auto_create=True)

        # префетч, чтобы меньше долбить API
        codes = [str(r["article"]) for r in parsed.to_dict(orient="records")]
        cache = await prefetch_products_by_code(client, codes, max_concurrency=3)

        for rec in parsed.to_dict(orient="records"):
            code = str(rec["article"])
            name_row = rec.get("name") or code
            manufacturer = rec.get("manufacturer")
            unit_hint = rec.get("unit")
            qty = float(rec["qty"])

            meta = cache.get(code)
            created_new = False
            if not meta:
                meta, created_new = await resolve_product_by_code_or_create(
                    client, code=code, name=name_row, manufacturer=manufacturer, unit_hint=unit_hint, auto_create=auto_create_products
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

            # цена в сомах по правилам
            price_kgs = compute_price_kgs(
                rec,
                default_currency=price_currency,
                coef=coef,
                usd_rate=usd_rate,
                shipping_per_kg_usd=shipping_per_kg_usd,
            )

            pos = {"assortment": meta, "quantity": qty}
            if price_kgs is not None:
                pos["price"] = int(round(float(price_kgs) * 100))
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
        if name is not None and str(name).strip():
            payload["name"] = str(name).strip()
        if moment is not None and str(moment).strip():
            payload["moment"] = str(moment).strip()

        r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/supply", json=payload)
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