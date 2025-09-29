# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, xlrd, numpy, pydantic, python-multipart
import os
import io
import json
import time
import math
import base64
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import httpx
import numpy as np
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# -------------------- env & app --------------------

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

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
        "User-Agent": "Jarvis West-Parts Importer/1.0",
    }

app = FastAPI(title="WestParts MS importer", version="2.0.0")
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
async def root():
    return RedirectResponse("/ui/")

# -------------------- utils --------------------

def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = " ".join(s.split())
    return s.strip().casefold()

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default

async def _request_with_backoff(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
) -> httpx.Response:
    """Тихие ретраи для 429/5xx, лёгкая пауза на 403."""
    backoff = 0.8
    for attempt in range(max_retries):
        resp = await client.request(method, url, params=params, json=json_body)
        if resp.status_code < 400:
            return resp

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            # бэкофф
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 6.0)
            continue

        if resp.status_code == 403:
            # бывает при временном запрете — небольшая пауза и одна повторная попытка
            time.sleep(0.7)
            continue

        # другие ошибки — сразу наверх
        resp.raise_for_status()

    # последний ответ
    resp.raise_for_status()
    return resp  # never

# -------------------- excel parsing --------------------

def _pick_engine(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    if ext == ".xlsx":
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    raise HTTPException(400, "Разрешены только .xlsx/.xls")

def parse_invoice(file_like, filename: str) -> pd.DataFrame:
    engine = _pick_engine(filename)
    raw = pd.read_excel(file_like, sheet_name=0, engine=engine)

    # ищем строку заголовков
    header_idx = None
    for i, row in raw.iterrows():
        vals = [str(v) for v in row.tolist()]
        if any("Артикул" in v for v in vals) and (any("№" in v for v in vals) or any("Товары" in v for v in vals)):
            header_idx = i
            break
    if header_idx is None:
        raise HTTPException(400, "Не удалось найти строку заголовков (ожидаем «Артикул/Товары/№»).")

    header = raw.iloc[header_idx]
    cols = {str(v).strip(): c for c, v in header.items() if pd.notna(v)}

    col_article = cols.get("Артикул") or cols.get("Код") or cols.get("Article")  # «артикул» — наш будущий code
    col_name    = cols.get("Товары (работы, услуги)") or cols.get("Наименование")
    col_qty     = cols.get("Кол.") or cols.get("Кол-во") or cols.get("Колич.") or cols.get("Количество")
    col_unit    = cols.get("Ед.") or cols.get("Ед")
    col_price   = cols.get("Цена") or cols.get("Price")
    col_curr    = cols.get("Валюта") or cols.get("Currency")
    col_weight  = cols.get("Вес") or cols.get("Масса") or cols.get("Weight")

    df = raw.iloc[header_idx + 1:].copy()

    # отбрасываем "ИТОГО" и пустые хвосты
    stop = None
    for i, row in df.iterrows():
        name_v = row[col_name] if col_name in df.columns else None
        if isinstance(name_v, str) and ("ИТОГО" in name_v.upper() or "ПРЕДОПЛАТА" in name_v.upper()):
            stop = i
            break
        if (col_article in df.columns) and pd.isna(row.get(col_article)):
            # пустая строка по обоим полям — конец
            if pd.isna(name_v):
                stop = i
                break
    if stop is not None:
        df = df.loc[: stop - 1]

    out = pd.DataFrame({
        "article": df[col_article] if col_article in df.columns else None,
        "name":    df[col_name] if col_name in df.columns else None,
        "qty":     df[col_qty] if col_qty in df.columns else 1,
        "unit":    df[col_unit] if col_unit in df.columns else None,
        "price":   df[col_price] if col_price in df.columns else None,
        "currency":df[col_curr] if col_curr in df.columns else None,
        "weight":  df[col_weight] if col_weight in df.columns else None,
    })

    for c in ("article", "name", "currency"):
        out[c] = out[c].astype(str).str.strip().replace({"nan": None, "": None})

    out["qty"]    = pd.to_numeric(out["qty"], errors="coerce").fillna(0)
    out["price"]  = pd.to_numeric(out["price"], errors="coerce")
    out["weight"] = pd.to_numeric(out["weight"], errors="coerce")

    out = out[(out["qty"] > 0) & (out["article"].notna())].reset_index(drop=True)
    return out

# -------------------- MS helper find/create --------------------

async def find_single_meta(client: httpx.AsyncClient, entity: str, filter_expr: str) -> Optional[Dict[str, Any]]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/{entity}", params={"filter": filter_expr, "limit": 1})
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def search_single_meta(client: httpx.AsyncClient, entity: str, search: str) -> Optional[Dict[str, Any]]:
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/{entity}", params={"search": search, "limit": 1})
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

def meta_from_id(entity: str, _id: str) -> Dict[str, Any]:
    return {"meta": {"href": f"{MS_API}/entity/{entity}/{_id}", "type": entity, "mediaType": "application/json"}}

async def resolve_uom_meta(client: httpx.AsyncClient, unit_hint: Optional[str]) -> Dict[str, Any]:
    candidates = []
    if unit_hint:
        unit_hint = str(unit_hint).strip()
        candidates += [f"name={unit_hint}", f"code={unit_hint}"]
    candidates += ["name=шт", "code=796"]
    for f in candidates:
        r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"filter": f, "limit": 1})
        rows = r.json().get("rows", [])
        if rows:
            return {"meta": rows[0]["meta"]}
    # fallback
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/uom", params={"limit": 1})
    rows = r.json().get("rows", [])
    if rows:
        return {"meta": rows[0]["meta"]}
    raise HTTPException(400, "Не удалось определить единицу измерения.")

async def find_product_by_code(client: httpx.AsyncClient, code: str) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    r = await _request_with_backoff(client, "GET", f"{MS_API}/entity/product", params={"filter": f"code={code}", "limit": 1})
    rows = r.json().get("rows", [])
    return {"meta": rows[0]["meta"]} if rows else None

async def create_product_with_code(
    client: httpx.AsyncClient, *, code: str, name: str, unit_hint: Optional[str]
) -> Dict[str, Any]:
    uom_meta = await resolve_uom_meta(client, unit_hint)
    payload = {
        "name": name or code or "Товар",
        "uom": uom_meta,
        "code": str(code),
    }
    r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/product", json_body=payload)
    if r.status_code in (401, 403):
        raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (товар)")
    data = r.json()
    return {"meta": data["meta"]}

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

    # organization
    if organization_id:
        refs["organization"] = meta_from_id("organization", organization_id)
    elif organization_name:
        refs["organization"] = await find_single_meta(client, "organization", f"name={organization_name}") \
                               or await search_single_meta(client, "organization", organization_name)
    if not refs.get("organization"):
        raise HTTPException(400, "Не найдена организация (organization).")

    # store
    if store_id:
        refs["store"] = meta_from_id("store", store_id)
    elif store_name:
        refs["store"] = await find_single_meta(client, "store", f"name={store_name}") \
                        or await search_single_meta(client, "store", store_name)
    if not refs.get("store"):
        raise HTTPException(400, "Не найден склад (store).")

    # agent
    if agent_id:
        refs["agent"] = meta_from_id("counterparty", agent_id)
    elif agent_name:
        agent = await find_single_meta(client, "counterparty", f"name={agent_name}") \
                 or await search_single_meta(client, "counterparty", agent_name)
        if not agent and auto_create_agent:
            r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/counterparty", json_body={"name": agent_name})
            if r.status_code in (401, 403):
                raise HTTPException(r.status_code, detail="Нет доступа к API МойСклад (контрагент)")
            agent = {"meta": r.json()["meta"]}
            created_agent = True
        if not agent:
            raise HTTPException(400, "Не найден контрагент (agent). Укажите имя/ID или разрешите авто-создание.")
        refs["agent"] = agent
    else:
        raise HTTPException(400, "Не указан поставщик (agent_name или agent_id).")

    return refs, created_agent

# -------------------- PO scan (заказы поставщику) --------------------

async def scan_purchase_orders_codes(
    client: httpx.AsyncClient,
    *,
    agent_meta: Dict[str, Any],
    days: int = 90,
    limit: int = 1000,
) -> Set[str]:
    """Вернёт МНОЖЕСТВО кодов товаров (product.code), встреченных в ЗП выбранного agent за N дней."""
    codes: Set[str] = set()

    # фильтр: по контрагенту + по дате "updated" за последние days
    params = {
        "expand": "positions.assortment",
        "limit": min(100, limit),
        "order": "updated,desc",
        "filter": f"agent={agent_meta['meta']['href']};updated>={pd.Timestamp.utcnow() - pd.Timedelta(days=days)}",
    }

    next_href = f"{MS_API}/entity/purchaseorder"
    pages = 0
    while next_href and pages < 200:  # страховка
        r = await _request_with_backoff(client, "GET", next_href, params=params if pages == 0 else None)
        data = r.json()
        rows = data.get("rows", []) or []
        for po in rows:
            pos = (po.get("positions") or {}).get("rows") or []
            for p in pos:
                ass = p.get("assortment") or {}
                if ass.get("meta", {}).get("type") == "product":
                    code = ass.get("code")
                    if isinstance(code, str) and code:
                        codes.add(_norm(code))
        next_href = (data.get("meta") or {}).get("nextHref")
        pages += 1

    return codes

# -------------------- pricing --------------------

def calc_kgs_for_row(
    *,
    price_value: Optional[float],
    weight: Optional[float],
    currency_hint: Optional[str],
    ui_currency: str,
    coef: float,
    usd_rate: Optional[float],
    shipping_per_kg_usd: Optional[float],
) -> Optional[float]:
    """Возвращает цену в KGS по правилам из ТЗ."""
    if price_value is None or (isinstance(price_value, float) and np.isnan(price_value)):
        return None

    file_curr = (_norm(currency_hint) or None)
    use_usd = (file_curr == "usd") or (file_curr == "$") or (ui_currency == "usd" and file_curr is None)

    if use_usd:
        rate = usd_rate or 0.0
        ship = shipping_per_kg_usd or 0.0
        kg = (weight or 0.0)
        # ((цена_usd * coef) + (вес * ship)) * rate
        return ((float(price_value) * coef) + (kg * ship)) * rate
    else:
        # сомы: price * coef
        return float(price_value) * coef

# -------------------- API models --------------------

class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]
    po_scan_total: int = 0
    po_scan_hits: int = 0

# -------------------- endpoints --------------------

async def _load_weights_from_form(request: Request) -> Dict[str, float]:
    """
    Вытаскиваем поле 'weights' из multipart/form-data, если фронт его отправил.
    Формат: [{"article": "...", "weight": 1.23}, ...]
    """
    try:
        form = await request.form()
    except Exception:
        return {}
    weights: Dict[str, float] = {}
    if "weights" in form:
        try:
            arr = json.loads(form["weights"])
            for it in arr or []:
                a = _norm(it.get("article") or it.get("code_key") or "")
                w = _safe_float(it.get("weight"), 0.0)
                if a:
                    weights[a] = w
        except Exception:
            pass
    return weights

@app.post("/import-invoice-preview/")
async def import_invoice_preview(
    request: Request,
    file: UploadFile = File(...),
    organization_name: Optional[str] = None,
    store_name: Optional[str] = None,
    agent_name: Optional[str] = None,
    organization_id: Optional[str] = None,
    store_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    auto_create_products: bool = True,
    auto_create_agent: bool = True,

    # pricing from UI
    price_currency: str = "usd",
    coef: float = 1.6,
    usd_rate: Optional[float] = None,
    shipping_per_kg_usd: Optional[float] = 15.0,

    # PO scan window
    po_days: int = 90,
):
    # читаем файл
    content = await file.read()
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Разрешены только .xlsx/.xls")
    df = parse_invoice(io.BytesIO(content), file.filename)
    if df.empty:
        raise HTTPException(400, "Не обнаружены строки с товарами.")

    # веса с фронта (в приоритете над «Вес» из файла)
    weights_map = await _load_weights_from_form(request)

    # готовим список кодов (артикулов)
    articles = [str(a) for a in df["article"].tolist()]
    norm_articles = [_norm(a) for a in articles]

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        # refs (и контрагент нужен нам для скана ЗП)
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name,
            store_name=store_name,
            agent_name=agent_name,
            organization_id=organization_id,
            store_id=store_id,
            agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )

        # подгружаем коды из ЗП для этого поставщика
        try:
            po_codes = await scan_purchase_orders_codes(client, agent_meta=refs["agent"], days=po_days)
        except HTTPException:
            po_codes = set()
        except Exception:
            po_codes = set()

        # ищем существующие продукты по code
        will_use_existing, will_create = [], []
        found_map: Dict[str, Dict[str, Any]] = {}

        for code_raw, name, unit_hint in zip(articles, df["name"].tolist(), df["unit"].tolist()):
            code = str(code_raw)
            meta = await find_product_by_code(client, code=code)
            if meta:
                pid = meta["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"code": code, "name": name, "product_id": pid})
                found_map[_norm(code)] = meta
            else:
                will_create.append({"code": code, "name": name, "unit_hint": unit_hint})

        # расчёт цен KGS и сверка по ЗП
        rows_out = []
        po_hits = 0
        for i, row in df.iterrows():
            art = str(row["article"])
            nm  = row.get("name")
            nart= _norm(art)
            w_from_file = row.get("weight")
            w = weights_map.get(nart, None if pd.isna(w_from_file) else _safe_float(w_from_file, 0.0))

            kgs = calc_kgs_for_row(
                price_value=(None if pd.isna(row.get("price")) else float(row.get("price"))),
                weight=w,
                currency_hint=row.get("currency"),
                ui_currency=price_currency,
                coef=float(coef),
                usd_rate=(None if usd_rate is None else float(usd_rate)),
                shipping_per_kg_usd=(None if shipping_per_kg_usd is None else float(shipping_per_kg_usd)),
            )
            in_po = (nart in po_codes)
            if in_po:
                po_hits += 1

            rows_out.append({
                "article": art,
                "name": nm,
                "price_kgs": None if kgs is None else int(round(kgs)),
                "po_hit": in_po,
            })

    return {
        "rows_total": len(df),
        "will_use_existing_count": len(will_use_existing),
        "will_create_count": len(will_create),
        "will_use_existing": will_use_existing[:100],
        "will_create": will_create[:100],
        "po_scan_total": len(df),
        "po_scan_hits": po_hits,
        "rows": rows_out[:1000],
        "po_scan_note": f"Сверка ЗП: найдено совпадений {po_hits} из {len(df)}.",
    }

@app.post("/import-invoice-to-supply/", response_model=SupplyCreateResponse)
async def import_invoice_to_supply(
    request: Request,
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

    # pricing
    price_currency: str = "usd",
    coef: float = 1.6,
    usd_rate: Optional[float] = None,
    shipping_per_kg_usd: Optional[float] = 15.0,

    po_days: int = 90,
):
    content = await file.read()
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Разрешены только .xlsx/.xls")
    df = parse_invoice(io.BytesIO(content), file.filename)
    if df.empty:
        raise HTTPException(400, "Не обнаружены строки с товарами.")

    weights_map = await _load_weights_from_form(request)

    created_products: List[str] = []
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    not_found: List[str] = []
    positions: List[Dict[str, Any]] = []

    po_hits = 0

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        refs, created_agent = await resolve_refs(
            client,
            organization_name=organization_name,
            store_name=store_name,
            agent_name=agent_name,
            organization_id=organization_id,
            store_id=store_id,
            agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )

        # ЗП — соберём коды для сверки
        try:
            po_codes = await scan_purchase_orders_codes(client, agent_meta=refs["agent"], days=po_days)
        except Exception:
            po_codes = set()

        for _, row in df.iterrows():
            code = str(row["article"])
            name_row = row.get("name") or code
            unit_hint = row.get("unit")
            qty = float(row["qty"])

            # вес
            w_file = row.get("weight")
            nkey = _norm(code)
            weight = weights_map.get(nkey, None if pd.isna(w_file) else _safe_float(w_file, 0.0))

            # ищем/создаём продукт по code
            meta = await find_product_by_code(client, code=code)
            created_new = False
            if not meta and auto_create_products:
                meta = await create_product_with_code(client, code=code, name=name_row, unit_hint=unit_hint)
                created_new = True
            if not meta:
                not_found.append(code)
                continue

            if created_new:
                created_products.append(code)
                will_create.append({"code": code, "name": name_row})
            else:
                pid = meta["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"code": code, "name": name_row, "product_id": pid})

            # цена
            kgs = calc_kgs_for_row(
                price_value=(None if pd.isna(row.get("price")) else float(row.get("price"))),
                weight=weight,
                currency_hint=row.get("currency"),
                ui_currency=price_currency,
                coef=float(coef),
                usd_rate=(None if usd_rate is None else float(usd_rate)),
                shipping_per_kg_usd=(None if shipping_per_kg_usd is None else float(shipping_per_kg_usd)),
            )

            pos: Dict[str, Any] = {"assortment": meta, "quantity": qty}
            if kgs is not None:
                pos["price"] = int(round(float(kgs) * 100))  # копейки

            positions.append(pos)

            if nkey in po_codes:
                po_hits += 1

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
                po_scan_total=len(df),
                po_scan_hits=po_hits,
            )

        payload: Dict[str, Any] = {
            "applicable": True,
            "vatEnabled": bool(vat_enabled),
            "vatIncluded": bool(vat_included),
            **refs,
            "positions": positions,
        }
        if name and str(name).strip():
            payload["name"] = str(name).strip()
        if moment and str(moment).strip():
            payload["moment"] = str(moment).strip()

        r = await _request_with_backoff(client, "POST", f"{MS_API}/entity/supply", json_body=payload)
        if r.status_code in (401, 403):
            raise HTTPException(r.status_code, "Нет доступа к API МойСклад (supply)")
        supply = r.json()

    return SupplyCreateResponse(
        created_positions=len(positions),
        not_found_items=not_found,
        created_products=created_products,
        created_agent=created_agent,
        will_create=will_create,
        will_use_existing=will_use_existing,
        supply_meta=supply["meta"],
        po_scan_total=len(df),
        po_scan_hits=po_hits,
    )