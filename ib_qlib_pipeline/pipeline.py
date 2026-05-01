from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import yaml
from ib_insync import Contract, IB, Stock, util


@dataclass
class IBConfig:
    host: str
    port: int
    client_id: int
    account: str
    trading_mode: str


@dataclass
class DataConfig:
    symbols_file: str
    start_date: str
    end_date: Optional[str]
    bar_size: str
    what_to_show: str
    use_rth: bool
    request_pause_seconds: float
    with_news: bool
    include_news_body: bool
    news_start_date: Optional[str] = None
    max_news_results: int = 100
    skip_existing_prices: bool = True


@dataclass
class OutputConfig:
    root_dir: str
    raw_prices_dir: str
    raw_news_dir: str
    qlib_csv_dir: str
    qlib_bin_dir: str


@dataclass
class QlibConfig:
    enabled: bool
    qlib_repo_path: str
    python_bin: str


@dataclass
class AppConfig:
    ib: IBConfig
    data: DataConfig
    output: OutputConfig
    qlib: QlibConfig


class PipelineError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch stock data from IB, clean it, and convert to Qlib-compatible format"
    )
    p.add_argument("--config", required=True, help="Path to YAML config")
    p.add_argument("--symbols", default=None, help="Comma-separated symbols; overrides symbols_file")
    p.add_argument("--start-date", default=None, help="YYYY-MM-DD; overrides config")
    p.add_argument("--end-date", default=None, help="YYYY-MM-DD; overrides config")
    p.add_argument("--bar-size", default=None, help='IB bar size, e.g. "1 day", "1 hour"')
    p.add_argument("--client-id", type=int, default=None, help="Override IB client_id from config")
    p.add_argument("--with-news", action="store_true", help="Enable news fetch")
    p.add_argument("--no-news", action="store_true", help="Disable news fetch")
    p.add_argument("--dump-bin", action="store_true", help="Run qlib dump_bin.py after CSV build")
    p.add_argument("--no-dump-bin", action="store_true", help="Skip qlib dump_bin.py")
    return p.parse_args()


def load_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return AppConfig(
        ib=IBConfig(**raw["ib"]),
        data=DataConfig(**raw["data"]),
        output=OutputConfig(**raw["output"]),
        qlib=QlibConfig(**raw["qlib"]),
    )


def _ensure_dirs(paths: Iterable[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def _log(message: str, messages: list[str]) -> None:
    print(message)
    messages.append(message)


def _contract_metadata(details) -> dict[str, str]:
    if not details:
        return {}
    detail = details[0]
    contract = getattr(detail, "contract", None)
    return {
        "symbol": getattr(contract, "symbol", "") if contract is not None else "",
        "longName": getattr(detail, "longName", "") or "",
        "industry": getattr(detail, "industry", "") or "",
        "sector": getattr(detail, "sector", "") or "",
        "category": getattr(detail, "category", "") or "",
        "marketName": getattr(detail, "marketName", "") or "",
        "exchange": getattr(contract, "exchange", "") if contract is not None else "",
        "currency": getattr(contract, "currency", "") if contract is not None else "",
        "description": getattr(detail, "description", "") or "",
    }


def _fetch_pe_ratio(ib: IB, contract: Contract) -> Optional[str]:
    # Many IB accounts do not have fundamentals entitlement enabled. Requesting
    # ReportRatios then floods the console with non-fatal 10358 errors. Since PE
    # is only optional display metadata in this project, skip the request entirely.
    return None


def _metadata_cache_path(metadata_dir: Path, qlib_symbol: str) -> Path:
    return metadata_dir / f"{qlib_symbol}.json"


def _write_metadata_cache(metadata_dir: Path, qlib_symbol: str, metadata: dict[str, str]) -> None:
    if not metadata:
        return
    metadata_dir.mkdir(parents=True, exist_ok=True)
    payload = {k: v for k, v in metadata.items() if v not in (None, "")}
    _metadata_cache_path(metadata_dir, qlib_symbol).write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _load_metadata_cache(metadata_dir: Path, qlib_symbol: str) -> dict[str, str]:
    path = _metadata_cache_path(metadata_dir, qlib_symbol)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _parse_date(d: Optional[str]) -> Optional[dt.date]:
    if not d:
        return None
    return dt.datetime.strptime(d, "%Y-%m-%d").date()


def _load_symbols(symbols_file: Path) -> list[tuple[str, str]]:
    symbols: list[tuple[str, str]] = []
    for line in symbols_file.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        # Supports either:
        # 1) QLIB_SYMBOL
        # 2) QLIB_SYMBOL,IB_SYMBOL
        parts = [x.strip() for x in s.split(",")]
        if len(parts) == 1:
            qsym = parts[0].upper()
            isym = qsym
        else:
            qsym = parts[0].upper()
            isym = parts[1].upper()
        symbols.append((qsym, isym))
    if not symbols:
        raise PipelineError(f"No symbols loaded from {symbols_file}")
    return symbols


def _duration_from_dates(start: dt.date, end: dt.date) -> str:
    days = (end - start).days
    if days <= 0:
        raise PipelineError("end_date must be later than start_date")
    years = max(1, int(days / 365) + 1)
    if years <= 30:
        return f"{years} Y"
    return f"{days} D"


def _ib_connect(cfg: IBConfig) -> IB:
    ib = IB()
    ib.connect(cfg.host, cfg.port, clientId=cfg.client_id)
    if not ib.isConnected():
        raise PipelineError("Failed to connect to IB")
    return ib


def _build_stock_contract(symbol: str) -> Contract:
    return Stock(symbol, "SMART", "USD")


def _bars_to_df(symbol: str, bars) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "factor"])
    df = util.df(bars)
    if df.empty:
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "factor"])

    if "date" not in df.columns:
        raise PipelineError(f"Unexpected IB bars format for {symbol}: missing date column")

    df = df.rename(columns=str.lower)
    df["symbol"] = symbol

    # Normalize date to daily string for Qlib loader.
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
    df["factor"] = 1.0

    keep_cols = ["date", "symbol", "open", "high", "low", "close", "volume", "factor"]
    for col in keep_cols:
        if col not in df.columns:
            if col == "volume":
                df[col] = 0
            else:
                raise PipelineError(f"Missing required column {col} for symbol {symbol}")

    out = df[keep_cols].copy()
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0)
    for col in ["open", "high", "low", "close", "factor"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["open", "high", "low", "close", "factor"])
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last")
    out = out.sort_values(["date", "symbol"]).reset_index(drop=True)
    return out


def _fetch_prices(
    ib: IB,
    ib_symbol: str,
    qlib_symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    bar_size: str,
    what_to_show: str,
    use_rth: bool,
) -> tuple[pd.DataFrame, dict[str, str]]:
    contract = _build_stock_contract(ib_symbol)
    details = ib.reqContractDetails(contract)
    if not details:
        raise PipelineError(f"Contract details not found for symbol={ib_symbol}")
    contract = details[0].contract

    end_date_time = end_date.strftime("%Y%m%d 23:59:59")
    # IB limitation: ADJUSTED_LAST does not support endDateTime.
    if what_to_show.upper() == "ADJUSTED_LAST":
        end_date_time = ""

    bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_date_time,
        durationStr=_duration_from_dates(start_date, end_date),
        barSizeSetting=bar_size,
        whatToShow=what_to_show,
        useRTH=1 if use_rth else 0,
        formatDate=1,
        keepUpToDate=False,
    )

    metadata = _contract_metadata(details)
    metadata["pe"] = _fetch_pe_ratio(ib, contract) or "N/A"

    df = _bars_to_df(qlib_symbol, bars)
    if df.empty:
        return df, metadata

    mask = (pd.to_datetime(df["date"]).dt.date >= start_date) & (
        pd.to_datetime(df["date"]).dt.date <= end_date
    )
    return df.loc[mask].reset_index(drop=True), metadata


def _fetch_news(
    ib: IB,
    ib_symbol: str,
    qlib_symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    include_body: bool,
    max_news_results: int,
) -> pd.DataFrame:
    contract = _build_stock_contract(ib_symbol)
    details = ib.reqContractDetails(contract)
    if not details:
        return pd.DataFrame()

    con_id = details[0].contract.conId
    providers = ib.reqNewsProviders()
    if not providers:
        return pd.DataFrame()
    provider_codes = "+".join(sorted({p.code for p in providers if getattr(p, "code", None)}))
    if not provider_codes:
        return pd.DataFrame()

    headlines = ib.reqHistoricalNews(
        con_id,
        provider_codes,
        start_date.strftime("%Y%m%d 00:00:00"),
        end_date.strftime("%Y%m%d 23:59:59"),
        totalResults=max_news_results,
    )
    if not headlines:
        return pd.DataFrame()

    rows: list[dict] = []
    for h in headlines:
        row = {
            "symbol": qlib_symbol,
            "ib_symbol": ib_symbol,
            "qlib_symbol": qlib_symbol,
            "time": h.time,
            "providerCode": h.providerCode,
            "articleId": h.articleId,
            "headline": h.headline,
        }
        if include_body:
            try:
                article = ib.reqNewsArticle(h.providerCode, h.articleId)
                row["articleType"] = article.articleType
                row["articleText"] = article.articleText
            except Exception as exc:  # noqa: BLE001
                row["articleType"] = None
                row["articleText"] = f"<failed_to_fetch: {exc}>"
        rows.append(row)

    return pd.DataFrame(rows)


def _write_symbol_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _read_existing_prices(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "factor"])
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "factor"])
    required = ["date", "symbol", "open", "high", "low", "close", "volume", "factor"]
    for col in required:
        if col not in df.columns:
            return pd.DataFrame(columns=required)
    out = df[required].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["date"])
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last")
    return out.sort_values(["date", "symbol"]).reset_index(drop=True)


def _merge_prices(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return fetched
    if fetched.empty:
        return existing
    out = pd.concat([existing, fetched], ignore_index=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last")
    out = out.sort_values(["date", "symbol"]).reset_index(drop=True)
    return out


def _run_dump_bin(cfg: AppConfig, qlib_csv_dir: Path, qlib_bin_dir: Path) -> None:
    qlib_repo = Path(cfg.qlib.qlib_repo_path)
    python_bin = Path(cfg.qlib.python_bin)
    dump_script = qlib_repo / "scripts" / "dump_bin.py"

    if not qlib_repo.exists() or not dump_script.exists():
        raise PipelineError(f"qlib repo or dump script not found: {dump_script}")
    if not python_bin.exists():
        raise PipelineError(f"qlib python not found: {python_bin}")

    cmd = [
        str(python_bin),
        str(dump_script),
        "dump_all",
        "--data_path",
        str(qlib_csv_dir),
        "--qlib_dir",
        str(qlib_bin_dir),
        "--freq",
        "day",
        "--date_field_name",
        "date",
        "--symbol_field_name",
        "symbol",
        "--include_fields",
        "open,close,high,low,volume,factor",
        "--file_suffix",
        ".csv",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise PipelineError(f"dump_bin failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")


def _effective_symbols(args: argparse.Namespace, cfg: AppConfig, project_root: Path) -> list[tuple[str, str]]:
    if args.symbols:
        out: list[tuple[str, str]] = []
        for s in [x.strip() for x in args.symbols.split(",") if x.strip()]:
            out.append((s.upper(), s.upper()))
        return out
    symbols_file = (project_root / cfg.data.symbols_file).resolve()
    return _load_symbols(symbols_file)


def _override_config(args: argparse.Namespace, cfg: AppConfig) -> None:
    if args.start_date:
        cfg.data.start_date = args.start_date
    if args.end_date:
        cfg.data.end_date = args.end_date
    if args.bar_size:
        cfg.data.bar_size = args.bar_size
    if args.client_id is not None:
        cfg.ib.client_id = args.client_id

    if args.with_news:
        cfg.data.with_news = True
    if args.no_news:
        cfg.data.with_news = False

    if args.dump_bin:
        cfg.qlib.enabled = True
    if args.no_dump_bin:
        cfg.qlib.enabled = False


def run() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parent.parent
    cfg_path = Path(args.config).resolve()
    cfg = load_config(cfg_path)
    _override_config(args, cfg)

    symbols = _effective_symbols(args, cfg, project_root)

    start_date = _parse_date(cfg.data.start_date)
    end_date = _parse_date(cfg.data.end_date) or dt.date.today()
    if start_date is None:
        raise PipelineError("start_date is required")

    root_dir = (project_root / cfg.output.root_dir).resolve()
    raw_prices_dir = (project_root / cfg.output.raw_prices_dir).resolve()
    raw_news_dir = (project_root / cfg.output.raw_news_dir).resolve()
    qlib_csv_dir = (project_root / cfg.output.qlib_csv_dir).resolve()
    qlib_bin_dir = (project_root / cfg.output.qlib_bin_dir).resolve()
    metadata_dir = root_dir / "raw" / "company_meta"

    _ensure_dirs([root_dir, raw_prices_dir, raw_news_dir, qlib_csv_dir, qlib_bin_dir, metadata_dir])

    report_messages: list[str] = []

    _log("[info] connecting to IB...", report_messages)
    ib: Optional[IB] = _ib_connect(cfg.ib)
    summary = {
        "symbols_count": len(symbols),
        "symbols_preview": [f"{q}/{i}" for q, i in symbols[:10]],
        "start_date": str(start_date),
        "end_date": str(end_date),
        "bar_size": cfg.data.bar_size,
        "what_to_show": cfg.data.what_to_show,
        "with_news": cfg.data.with_news,
        "dump_bin": cfg.qlib.enabled,
    }
    _log("[info] run summary:", report_messages)
    _log(json.dumps(summary, indent=2, ensure_ascii=False), report_messages)

    try:
        news_start_date = _parse_date(cfg.data.news_start_date) if cfg.data.news_start_date else start_date
        for qlib_symbol, ib_symbol in symbols:
            raw_price_path = raw_prices_dir / f"{qlib_symbol}.csv"
            qlib_price_path = qlib_csv_dir / f"{qlib_symbol}.csv"
            metadata_path = _metadata_cache_path(metadata_dir, qlib_symbol)
            existing = _read_existing_prices(qlib_price_path) if cfg.data.skip_existing_prices else pd.DataFrame()
            fetch_start = start_date
            metadata = _load_metadata_cache(metadata_dir, qlib_symbol)
            if not existing.empty:
                latest_existing = pd.to_datetime(existing["date"]).dt.date.max()
                if latest_existing >= end_date:
                    _log(f"[info] up-to-date prices for {qlib_symbol}; latest={latest_existing}", report_messages)
                else:
                    # Keep one-day overlap to handle data revisions and ensure continuity.
                    fetch_start = max(start_date, latest_existing - dt.timedelta(days=1))
                    _log(
                        f"[info] incremental fetch for {qlib_symbol}: "
                        f"{fetch_start} -> {end_date} (latest={latest_existing})",
                        report_messages,
                    )

            merged = existing
            if existing.empty or fetch_start <= end_date:
                if existing.empty:
                    _log(f"[info] fetching prices for {qlib_symbol} (ib={ib_symbol})...", report_messages)
                try:
                    prices, metadata = _fetch_prices(
                        ib=ib,
                        ib_symbol=ib_symbol,
                        qlib_symbol=qlib_symbol,
                        start_date=fetch_start,
                        end_date=end_date,
                        bar_size=cfg.data.bar_size,
                        what_to_show=cfg.data.what_to_show,
                        use_rth=cfg.data.use_rth,
                    )
                except Exception as exc:  # noqa: BLE001
                    _log(f"[error] price fetch failed for {qlib_symbol} (ib={ib_symbol}): {exc}", report_messages)
                    continue

                merged = _merge_prices(existing, prices)
                if merged.empty:
                    _log(f"[warn] no price data for {qlib_symbol} (ib={ib_symbol})", report_messages)
                else:
                    _write_symbol_csv(merged, raw_price_path)
                    _write_symbol_csv(merged, qlib_price_path)
                    _write_metadata_cache(metadata_dir, qlib_symbol, metadata)
                    _log(f"[info] wrote prices: {raw_price_path}", report_messages)
                    _log(f"[info] wrote qlib csv: {qlib_price_path}", report_messages)
                    if metadata_path.exists():
                        _log(f"[info] wrote company meta: {metadata_path}", report_messages)
            elif not metadata_path.exists():
                try:
                    _, metadata = _fetch_prices(
                        ib=ib,
                        ib_symbol=ib_symbol,
                        qlib_symbol=qlib_symbol,
                        start_date=end_date - dt.timedelta(days=5),
                        end_date=end_date,
                        bar_size=cfg.data.bar_size,
                        what_to_show=cfg.data.what_to_show,
                        use_rth=cfg.data.use_rth,
                    )
                    _write_metadata_cache(metadata_dir, qlib_symbol, metadata)
                    _log(f"[info] wrote company meta: {metadata_path}", report_messages)
                except Exception as exc:  # noqa: BLE001
                    _log(f"[warn] company meta fetch failed for {qlib_symbol}: {exc}", report_messages)

            if cfg.data.with_news:
                _log(f"[info] fetching news for {qlib_symbol} (ib={ib_symbol})...", report_messages)
                try:
                    news = _fetch_news(
                        ib=ib,
                        ib_symbol=ib_symbol,
                        qlib_symbol=qlib_symbol,
                        start_date=news_start_date,
                        end_date=end_date,
                        include_body=cfg.data.include_news_body,
                        max_news_results=cfg.data.max_news_results,
                    )
                    news_path = raw_news_dir / f"{qlib_symbol}.csv"
                    if news.empty:
                        _log(f"[warn] no news data for {qlib_symbol} (or no subscription)", report_messages)
                    else:
                        _write_symbol_csv(news, news_path)
                        _log(f"[info] wrote news: {news_path}", report_messages)
                except Exception as exc:  # noqa: BLE001
                    _log(f"[warn] news fetch failed for {qlib_symbol}: {exc}", report_messages)

            time.sleep(max(0.0, cfg.data.request_pause_seconds))

        if cfg.qlib.enabled:
            _log("[info] running qlib dump_bin...", report_messages)
            _run_dump_bin(cfg, qlib_csv_dir, qlib_bin_dir)
            _log(f"[info] qlib bin generated: {qlib_bin_dir}", report_messages)

    finally:
        if ib is not None and ib.isConnected():
            ib.disconnect()

    _log("[info] pipeline finished", report_messages)
    return 0


def main() -> None:
    try:
        rc = run()
    except PipelineError as exc:
        print(f"[fatal] {exc}")
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("[fatal] interrupted")
        raise SystemExit(130)
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
