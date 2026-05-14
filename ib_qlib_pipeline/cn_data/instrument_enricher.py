from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from .symbols import qlib_symbol_to_baostock, raw_code_to_qlib_symbol
from ..webapi.instrument_store import bulk_upsert_instrument_aliases, get_instrument_by_symbol, upsert_instrument


def baostock_to_qlib_symbol(code: str) -> str:
    return raw_code_to_qlib_symbol(code)


def qlib_to_yahoo_symbol(symbol: str) -> str:
    if symbol.startswith("SH"):
        return f"{symbol[2:]}.SS"
    if symbol.startswith("SZ"):
        return f"{symbol[2:]}.SZ"
    if symbol.startswith("BJ"):
        return f"{symbol[2:]}.BJ"
    return symbol


def infer_exchange(symbol: str) -> str | None:
    if len(symbol) >= 2 and symbol[:2] in {"SH", "SZ", "BJ"}:
        return symbol[:2]
    return None


def normalize_listing_status(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    if raw in {"1", "listed", "list"}:
        return "listed"
    if raw in {"0", "delisted", "delist"}:
        return "delisted"
    return "unknown"


def _rows_from_query(rs: Any) -> list[dict[str, str]]:
    error_code = str(getattr(rs, "error_code", ""))
    if error_code != "0":
        raise RuntimeError(f"BaoStock query failed: {getattr(rs, 'error_msg', '')}")
    rows: list[dict[str, str]] = []
    fields = list(getattr(rs, "fields", []))
    while rs.error_code == "0" and rs.next():
        row_data = rs.get_row_data()
        rows.append({field: row_data[idx] if idx < len(row_data) else "" for idx, field in enumerate(fields)})
    return rows


def fetch_baostock_stock_basic(*, bs_module: Any) -> list[dict[str, str]]:
    return _rows_from_query(bs_module.query_stock_basic())


def fetch_baostock_stock_industry(*, bs_module: Any) -> dict[str, dict[str, str]]:
    try:
        rows = _rows_from_query(bs_module.query_stock_industry())
    except Exception:
        return {}
    return {str(row.get("code", "")).strip().lower(): row for row in rows if row.get("code")}


def read_symbol_filter(path: Path) -> set[str]:
    selected: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        first = line.split(",")[0].strip()
        if not first:
            continue
        selected.add(raw_code_to_qlib_symbol(first))
    return selected


def enrich_cn_instruments_from_baostock(
    *,
    db_path: Path,
    bs_module: Any,
    symbols_path: Path | None = None,
    dry_run: bool = False,
    max_symbols: int | None = None,
) -> dict[str, int]:
    basic_rows = fetch_baostock_stock_basic(bs_module=bs_module)
    industry_map = fetch_baostock_stock_industry(bs_module=bs_module)
    selected = read_symbol_filter(symbols_path) if symbols_path is not None else None
    inserted_instruments = 0
    updated_instruments = 0
    inserted_aliases = 0
    existing_aliases = 0
    missing_industry = 0
    failed = 0
    processed = 0
    source_now = dt.datetime.now(dt.timezone.utc).isoformat()

    for row in basic_rows:
        try:
            canonical_symbol = baostock_to_qlib_symbol(str(row.get("code", "")).strip())
        except Exception:
            failed += 1
            continue
        if selected is not None and canonical_symbol not in selected:
            continue
        processed += 1
        if max_symbols is not None and processed > int(max_symbols):
            break

        exchange = infer_exchange(canonical_symbol)
        industry_row = industry_map.get(str(row.get("code", "")).strip().lower(), {})
        industry = str(industry_row.get("industry", "")).strip() or None
        sector = str(industry_row.get("industryClassification", "")).strip() or None
        if industry is None:
            missing_industry += 1

        payload = {
            "canonical_symbol": canonical_symbol,
            "display_symbol": canonical_symbol,
            "asset_type": "stock",
            "country": "CN",
            "exchange": exchange,
            "currency": "CNY",
            "name_zh": str(row.get("code_name") or industry_row.get("code_name") or "").strip() or None,
            "ipo_date": str(row.get("ipoDate", "")).strip() or None,
            "delist_date": str(row.get("outDate", "")).strip() or None,
            "listing_status": normalize_listing_status(row.get("status")),
            "sector": sector,
            "industry": industry,
            "source": "baostock",
            "source_updated_at": str(industry_row.get("updateDate", "")).strip() or source_now,
        }
        aliases = [
            ("qlib", canonical_symbol),
            ("baostock", qlib_symbol_to_baostock(canonical_symbol)),
            ("yahoo", qlib_to_yahoo_symbol(canonical_symbol)),
        ]
        existing = get_instrument_by_symbol(db_path, canonical_symbol) if dry_run else None
        if dry_run:
            if existing is None:
                inserted_instruments += 1
            else:
                updated_instruments += 1
            continue
        _, created = upsert_instrument(db_path, payload)
        alias_inserted, alias_existing = bulk_upsert_instrument_aliases(
            db_path,
            canonical_symbol=canonical_symbol,
            aliases=aliases,
        )
        inserted_aliases += alias_inserted
        existing_aliases += alias_existing
        if created:
            inserted_instruments += 1
        else:
            updated_instruments += 1

    return {
        "inserted_instruments": inserted_instruments,
        "updated_instruments": updated_instruments,
        "inserted_aliases": inserted_aliases,
        "existing_aliases": existing_aliases,
        "missing_industry": missing_industry,
        "failed": failed,
        "processed": processed,
    }
