from __future__ import annotations
# requirements: fastapi, uvicorn, httpx, pandas, openpyxl, xlrd, numpy, pydantic, python-multipart
import os
import base64
import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

MS_API: str = os.environ.get("MS_API", "https://api.moysklad.ru/api/remap/1.2")
MS_LOGIN: str = os.environ.get("MS_LOGIN", "")
MS_PASSWORD: str = os.environ.get("MS_PASSWORD", "")
MANUFACTURER_ATTR_NAME = "Производитель"

app = FastAPI()

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

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Set MS_LOGIN and MS_PASSWORD environment variables.")

def ms_headers() -> Dict[str, str]:
    token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }

def _norm_name(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip().casefold()

def _is_unique_name_error(resp: httpx.Response) -> bool:
    try:
        data = resp.json()
        return any(e.get("code") == 3006 for e in data.get("errors", []))
    except Exception:
        return False

def meta_from_id(entity: str, _id: str) -> Dict[str, Any]:
    return {"meta": {"href": f"{MS_API}/entity/{entity}/{_id}", "type": entity, "mediaType": "application/json"}}

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
    code = str(code).strip()
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
    target = _norm_name(MANUFACTURER_ATTR_NAME)

    attrs = await _fetch_product_attrs(client)
    for a in attrs:
        if _norm_name(a.get("name")) == target and str(a.get("type", "")).casefold() in ("string", "text"):
            return a

    if not auto_create:
        raise HTTPException(400, detail=f"В товарах нет поля '{MANUFACTURER_ATTR_NAME}' (string).")

    r = await client.post(f"{MS_API}/entity/product/metadata/attributes",
                          json={"name": MANUFACTURER_ATTR_NAME, "type": "string"})

    if r.status_code in (409, 412) and _is_unique_name_error(r):
        attrs = await _fetch_product_attrs(client)
        for a in attrs:
            if _norm_name(a.get("name")) == target:
                if str(a.get("type", "")).casefold() not in ("string", "text"):
                    raise HTTPException(400, detail=f"Поле '{MANUFACTURER_ATTR_NAME}' существует, но его тип '{a.get('type')}'. Нужен 'string'.")
                return a
        raise HTTPException(400, detail=f"Поле '{MANUFACTURЕР_ATTR_NAME}' уже существует, но API не вернуло его метаданные.")

    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        raise HTTPException(r.status_code, r.text)
    return r.json()

async def resolve_uom_meta(client: httpx.AsyncClient, unit_hint: Optional[str]) -> Dict[str, Any]:
    candidates = []
    if unit_hint:
        unit_hint = str(unit_hint).strip()
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
        "code": str(article) if article else None,
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
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        raise HTTPException(r.status_code, r.text)
    return {"meta": r.json()["meta"]}

async def resolve_product_by_article_manufacturer_or_create(
    client: httpx.AsyncClient,
    *,
    article: str,
    name: Optional[str],
    manufacturer: Optional[str],
    unit_hint: Optional[str],
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
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        raise HTTPException(r.status_code, r.text)
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

def parse_invoice_like_excel(file, *, engine: Optional[str] = None) -> "pd.DataFrame":
    import pandas as pd
    from io import BytesIO, StringIO

    data = file.read() 
    try:
        raw = pd.read_excel(BytesIO(data), sheet_name=0, engine=engine)
        return _normalize_invoice_df(raw)
    except Exception as e_xls:
        alt = "openpyxl" if engine == "xlrd" else "xlrd"
        try:
            raw = pd.read_excel(BytesIO(data), sheet_name=0, engine=alt)
            return _normalize_invoice_df(raw)
        except Exception:
            pass

        for enc in ("utf-8", "cp1251", "windows-1251", "latin-1"):
            try:
                html_text = data.decode(enc)
                break
            except UnicodeDecodeError:
                html_text = None
        if html_text is None:
            html_text = data.decode("utf-8", errors="ignore")

        try:
            tables = pd.read_html(StringIO(html_text))  # нужен lxml/bs4
            if not tables:
                raise ValueError("HTML не содержит таблиц")
            # берём самую «большую» таблицу
            raw = max(tables, key=lambda df: (df.shape[0] * df.shape[1]))
            return _normalize_invoice_df(raw)
        except Exception as e_html:
            raise HTTPException(
                400,
                detail=f"Не удалось прочитать файл Excel: исходный .xls повреждён или является HTML/Excel-XML. "
                       f"Попробуйте сохранить файл как .xlsx."
            ) from e_html

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
    })

    for col in ("manufacturer", "article", "name"):
        parsed[col] = parsed[col].astype(str).str.strip().replace({"nan": None, "": None})
    parsed["qty"] = pd.to_numeric(parsed["qty"], errors="coerce").fillna(0)
    parsed["price"] = pd.to_numeric(parsed["price"], errors="coerce")
    parsed = parsed[(parsed["qty"] > 0) & (parsed["article"].notna())]
    return parsed.reset_index(drop=True)


class SupplyCreateResponse(BaseModel):
    created_positions: int
    not_found_items: List[str]
    created_products: List[str] = []
    created_agent: bool = False
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    supply_meta: Dict[str, Any]

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
):
    # выбор движка по расширению
    ext = Path(file.filename).suffix.lower()
    if ext == ".xlsx":
        engine = "openpyxl"
    elif ext == ".xls":
        engine = "xlrd"
    else:
        raise HTTPException(400, "Разрешены только .xlsx/.xls")

    parsed = parse_invoice_like_excel(file.file, engine=engine)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    async with httpx.AsyncClient(timeout=60.0, headers=ms_headers()) as client:
        _, _ = await resolve_refs(
            client,
            organization_name=organization_name,
            store_name=store_name,
            agent_name=agent_name,
            organization_id=organization_id,
            store_id=store_id,
            agent_id=agent_id,
            auto_create_agent=auto_create_agent,
        )
        _ = await get_or_create_manufacturer_attr_meta(client, auto_create=True)

        will_create, will_use_existing = [], []
        for rec in parsed.to_dict(orient="records"):
            article = str(rec["article"]).strip()
            manufacturer = rec.get("manufacturer")
            name = rec.get("name") or article
            found = await find_product_by_code(client, code=article)
            if found:
                product_id = found["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"article": article, "manufacturer": manufacturer, "name": name, "product_id": product_id})
            else:
                will_create.append({"article": article, "manufacturer": manufacturer, "name": name})

    return {
        "rows_total": len(parsed),
        "will_create_count": len(will_create),
        "will_use_existing_count": len(will_use_existing),
        "will_create": will_create[:50],
        "will_use_existing": will_use_existing[:50],
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
):
    # выбор движка по расширению
    ext = Path(file.filename).suffix.lower()
    if ext == ".xlsx":
        engine = "openpyxl"
    elif ext == ".xls":
        engine = "xlrd"
    else:
        raise HTTPException(400, "Разрешены только .xlsx/.xls")

    parsed = parse_invoice_like_excel(file.file, engine=engine)
    if parsed.empty:
        raise HTTPException(400, detail="Не обнаружены строки с товарами.")

    import numpy as np  # lazy import

    created_products: List[str] = []
    will_create: List[Dict[str, Any]] = []
    will_use_existing: List[Dict[str, Any]] = []
    not_found: List[str] = []
    positions: List[Dict[str, Any]] = []

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
        _ = await get_or_create_manufacturer_attr_meta(client, auto_create=True)

        for rec in parsed.to_dict(orient="records"):
            article = str(rec["article"]).strip()
            name_row = rec.get("name") or article
            manufacturer = rec.get("manufacturer")
            unit_hint = rec.get("unit")
            qty = float(rec["qty"])
            price = rec.get("price")

            meta, created_new = await resolve_product_by_article_manufacturer_or_create(
                client,
                article=article,
                name=name_row,
                manufacturer=manufacturer,
                unit_hint=unit_hint,
                auto_create=auto_create_products,
            )
            if not meta:
                not_found.append(article)
                continue

            if created_new:
                created_products.append(article)
                will_create.append({"article": article, "manufacturer": manufacturer, "name": name_row})
            else:
                product_id = meta["meta"]["href"].rstrip("/").split("/")[-1]
                will_use_existing.append({"article": article, "manufacturer": manufacturer, "name": name_row, "product_id": product_id})

            pos = {"assortment": meta, "quantity": qty}
            if price is not None and not (isinstance(price, float) and np.isnan(price)):
                try:
                    pos["price"] = int(round(float(price) * 100))
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
        if name is not None and str(name).strip():
            payload["name"] = str(name).strip()
        if moment is not None and str(moment).strip():
            payload["moment"] = str(moment).strip()

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
    )
