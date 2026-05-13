from __future__ import annotations

import html
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from ib_insync import IB, Contract, Stock

from ib_qlib_pipeline.qlib_runtime import load_qlib_runtime_config
from ib_qlib_pipeline.runner.common import log
from ib_qlib_pipeline.ranking import TOP_N


def _build_stock_contract(symbol: str) -> Contract:
    return Stock(symbol, "SMART", "USD")


def _fetch_pe_ratio(ib: IB, contract: Contract) -> Optional[str]:
    return None


def _fmt_num(value: object, digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except TypeError:
        pass
    try:
        return f"{float(value):,.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_int(value: object) -> str:
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except TypeError:
        pass
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return str(value)


def _has_value(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in ("", "N/A")
    try:
        return not pd.isna(value)
    except TypeError:
        return True


def render_dl_rows(rows: list[tuple[str, object]]) -> str:
    parts: list[str] = []
    for label, value in rows:
        if not _has_value(value):
            continue
        parts.append(f"<dt>{html.escape(label)}</dt><dd>{html.escape(str(value))}</dd>")
    return "".join(parts)


def build_price_stats(project_root: Path, symbol: str, config_path: Path | None = None) -> dict[str, object]:
    price_path = load_qlib_runtime_config(project_root, config_path=config_path).data_dir / "processed" / "qlib_csv" / f"{symbol}.csv"
    if not price_path.exists():
        return {}
    df = pd.read_csv(price_path, usecols=["date", "open", "high", "low", "close", "volume"])
    if df.empty:
        return {}

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    latest = df.iloc[-1]
    month_1 = df[df["date"] >= (df["date"].max() - pd.Timedelta(days=30))]
    year_1 = df[df["date"] >= (df["date"].max() - pd.Timedelta(days=365))]

    return {
        "latest_date": latest["date"].date().isoformat(),
        "latest_open": latest["open"],
        "latest_high": latest["high"],
        "latest_low": latest["low"],
        "latest_close": latest["close"],
        "all_time_low": df["low"].min(),
        "all_time_high": df["high"].max(),
        "day_30_low": month_1["low"].min() if not month_1.empty else None,
        "day_30_high": month_1["high"].max() if not month_1.empty else None,
        "week_52_low": year_1["low"].min() if not year_1.empty else None,
        "week_52_high": year_1["high"].max() if not year_1.empty else None,
        "avg_volume_30d": month_1["volume"].mean() if not month_1.empty else None,
    }


def load_ib_config(project_root: Path, config_path: Path | None = None) -> tuple[str, int]:
    cfg = yaml.safe_load((config_path or (project_root / "config.yaml")).read_text(encoding="utf-8"))
    return str(cfg["ib"]["host"]), int(cfg["ib"]["port"])


def company_meta_cache_path(project_root: Path, symbol: str, config_path: Path | None = None) -> Path:
    return load_qlib_runtime_config(project_root, config_path=config_path).data_dir / "raw" / "company_meta" / f"{symbol}.json"


def load_company_meta_cache(project_root: Path, symbol: str, config_path: Path | None = None) -> dict[str, str]:
    path = company_meta_cache_path(project_root, symbol, config_path=config_path)
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return {}


def fetch_topn_company_data(
    project_root: Path,
    topn: pd.DataFrame,
    client_id: int,
    console_lines: list[str],
    config_path: Path | None = None,
) -> list[dict]:
    host, port = load_ib_config(project_root, config_path=config_path)
    ib = IB()
    rows: list[dict] = []
    try:
        ib.connect(host, port, clientId=client_id + 1000)
        if not ib.isConnected():
            raise RuntimeError("IB connection was not established")
        log(f"[info] enriching top{TOP_N} html data from IB: host={host} port={port}", console_lines)

        for row in topn.itertuples(index=False):
            symbol = str(row.symbol)
            card = {
                "rank": int(row.rank),
                "symbol": symbol,
                "score": float(row.score),
                "percentile": float(row.percentile),
                "close": None if pd.isna(row.close) else float(row.close),
                "metadata": {},
                "price_stats": build_price_stats(project_root, symbol, config_path=config_path),
            }
            cached = load_company_meta_cache(project_root, symbol, config_path=config_path)
            if cached:
                card["metadata"] = {
                    "company_name": cached.get("longName", "") or symbol,
                    "industry": cached.get("industry", "") or "N/A",
                    "sector": cached.get("sector", "") or "N/A",
                    "category": cached.get("category", "") or "N/A",
                    "market_name": cached.get("marketName", "") or "N/A",
                    "exchange": cached.get("exchange", "") or "N/A",
                    "currency": cached.get("currency", "") or "N/A",
                    "description": cached.get("description", "") or "N/A",
                    "pe": cached.get("pe", "") or "N/A",
                }
            try:
                contract = _build_stock_contract(symbol)
                details = ib.reqContractDetails(contract)
                if details:
                    detail = details[0]
                    contract = detail.contract
                    live_metadata = {
                        "company_name": getattr(detail, "longName", "") or symbol,
                        "industry": getattr(detail, "industry", "") or "N/A",
                        "sector": getattr(detail, "sector", "") or "N/A",
                        "category": getattr(detail, "category", "") or "N/A",
                        "market_name": getattr(detail, "marketName", "") or "N/A",
                        "exchange": getattr(contract, "exchange", "") or "N/A",
                        "currency": getattr(contract, "currency", "") or "N/A",
                        "description": getattr(detail, "description", "") or "N/A",
                        "pe": _fetch_pe_ratio(ib, contract) or card["metadata"].get("pe", "N/A"),
                    }
                    card["metadata"] = {**card["metadata"], **live_metadata}
                elif not card["metadata"]:
                    card["metadata"] = _fallback_metadata(symbol)
            except Exception as exc:  # noqa: BLE001
                log(f"[warn] failed to enrich {symbol} for html: {exc}", console_lines)
                if not card["metadata"]:
                    card["metadata"] = _fallback_metadata(symbol)
            rows.append(card)
    except Exception as exc:  # noqa: BLE001
        log(f"[warn] html enrichment skipped: {exc}", console_lines)
        for row in topn.itertuples(index=False):
            cached = load_company_meta_cache(project_root, str(row.symbol), config_path=config_path)
            rows.append(
                {
                    "rank": int(row.rank),
                    "symbol": str(row.symbol),
                    "score": float(row.score),
                    "percentile": float(row.percentile),
                    "close": None if pd.isna(row.close) else float(row.close),
                    "metadata": {
                        "company_name": cached.get("longName", "") or str(row.symbol),
                        "industry": cached.get("industry", "") or "N/A",
                        "sector": cached.get("sector", "") or "N/A",
                        "category": cached.get("category", "") or "N/A",
                        "market_name": cached.get("marketName", "") or "N/A",
                        "exchange": cached.get("exchange", "") or "N/A",
                        "currency": cached.get("currency", "") or "N/A",
                        "description": cached.get("description", "") or "N/A",
                        "pe": cached.get("pe", "") or "N/A",
                    },
                    "price_stats": build_price_stats(project_root, str(row.symbol), config_path=config_path),
                }
            )
    finally:
        if ib.isConnected():
            ib.disconnect()
    return rows


def cached_topn_details(
    project_root: Path,
    ranking_df: pd.DataFrame,
    client_id: int,
    console_lines: list[str],
    config_path: Path | None = None,
) -> list[dict]:
    rows: list[dict] = []
    log(f"[info] building cached html data for top{TOP_N} without live IB calls", console_lines)
    for row in ranking_df.head(TOP_N).itertuples(index=False):
        symbol = str(row.symbol)
        cached = load_company_meta_cache(project_root, symbol, config_path=config_path)
        rows.append(
            {
                "rank": int(row.rank),
                "symbol": symbol,
                "score": float(row.score),
                "percentile": float(row.percentile),
                "close": None if pd.isna(row.close) else float(row.close),
                "metadata": {
                    "company_name": cached.get("longName", "") or symbol,
                    "industry": cached.get("industry", "") or "N/A",
                    "sector": cached.get("sector", "") or "N/A",
                    "category": cached.get("category", "") or "N/A",
                    "market_name": cached.get("marketName", "") or "N/A",
                    "exchange": cached.get("exchange", "") or "N/A",
                    "currency": cached.get("currency", "") or "N/A",
                    "description": cached.get("description", "") or "N/A",
                    "pe": cached.get("pe", "") or "N/A",
                },
                "price_stats": build_price_stats(project_root, symbol, config_path=config_path),
            }
        )
    return rows


def _fallback_metadata(symbol: str) -> dict[str, str]:
    return {
        "company_name": symbol,
        "industry": "N/A",
        "sector": "N/A",
        "category": "N/A",
        "market_name": "N/A",
        "exchange": "N/A",
        "currency": "N/A",
        "description": "N/A",
        "pe": "N/A",
    }
