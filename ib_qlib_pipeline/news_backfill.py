from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path

import pandas as pd
from ib_insync import IB, Stock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill IB historical news for symbols with pagination and dedup.")
    p.add_argument("--symbols-file", required=True, help="symbols file; supports qlib,ib format per line")
    p.add_argument("--out-dir", required=True, help="output directory for per-symbol news csv")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=7497)
    p.add_argument("--client-id", type=int, default=201)
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", default=None, help="YYYY-MM-DD; default today")
    p.add_argument("--results-per-request", type=int, default=50)
    p.add_argument("--max-pages-per-symbol", type=int, default=200)
    p.add_argument("--pause-seconds", type=float, default=0.1)
    return p.parse_args()


def _load_symbols(path: Path) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        parts = [x.strip() for x in s.split(",")]
        qsym = parts[0].upper()
        isym = (parts[1] if len(parts) > 1 else parts[0]).upper()
        out.append((qsym, isym))
    return out


def _to_ib_time(ts: dt.datetime) -> str:
    return ts.strftime("%Y%m%d %H:%M:%S")


def _parse_time(s) -> dt.datetime | None:
    if s is None:
        return None
    if isinstance(s, dt.datetime):
        return s.replace(tzinfo=None)
    if isinstance(s, dt.date):
        return dt.datetime.combine(s, dt.time(0, 0, 0))
    if not isinstance(s, str):
        return None
    if not s:
        return None
    # Example from IB here is "2026-03-05 13:27:39"
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ["symbol", "providerCode", "articleId", "time", "headline"]:
        if col not in df.columns:
            df[col] = ""
    key_primary = ["symbol", "providerCode", "articleId"]
    df = df.drop_duplicates(subset=key_primary, keep="first")
    df["headline_hash"] = df["headline"].astype(str).str.strip().str.lower()
    key_fallback = ["symbol", "time", "headline_hash"]
    df = df.drop_duplicates(subset=key_fallback, keep="first")
    df = df.drop(columns=["headline_hash"])
    return df.sort_values(["time", "symbol"], ascending=[False, True]).reset_index(drop=True)


def _read_existing(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(
            columns=["symbol", "ib_symbol", "qlib_symbol", "time", "providerCode", "articleId", "headline"]
        )
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame(
            columns=["symbol", "ib_symbol", "qlib_symbol", "time", "providerCode", "articleId", "headline"]
        )


def run() -> int:
    args = parse_args()
    symbols = _load_symbols(Path(args.symbols_file))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    start_dt = dt.datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else dt.date.today()
    end_dt = dt.datetime.combine(end_date, dt.time(23, 59, 59))

    ib = IB()
    ib.connect(args.host, args.port, clientId=args.client_id, timeout=10)
    if not ib.isConnected():
        raise SystemExit("failed to connect IB")

    try:
        providers = ib.reqNewsProviders()
        provider_codes = "+".join(sorted({p.code for p in providers if getattr(p, "code", None)}))
        if not provider_codes:
            raise SystemExit("no news providers available")

        print(
            f"[info] symbols={len(symbols)} start={start_dt.date()} end={end_dt.date()} providers={provider_codes}"
        )
        ok_symbols = 0
        for i, (qsym, isym) in enumerate(symbols, start=1):
            print(f"[info] ({i}/{len(symbols)}) news backfill {qsym} (ib={isym})")
            out_path = out_dir / f"{qsym}.csv"
            existing = _read_existing(out_path)
            rows: list[dict] = []

            try:
                details = ib.reqContractDetails(Stock(isym, "SMART", "USD"))
                if not details:
                    print(f"[warn] contract not found: {qsym}/{isym}")
                    continue
                con_id = details[0].contract.conId
            except Exception as exc:  # noqa: BLE001
                print(f"[warn] contract lookup failed for {qsym}: {exc}")
                continue

            page = 0
            cursor_end = end_dt
            while page < args.max_pages_per_symbol:
                page += 1
                try:
                    headlines = ib.reqHistoricalNews(
                        con_id,
                        provider_codes,
                        _to_ib_time(start_dt),
                        _to_ib_time(cursor_end),
                        totalResults=args.results_per_request,
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"[warn] reqHistoricalNews failed {qsym} page={page}: {exc}")
                    break

                if not headlines:
                    break

                parsed_times: list[dt.datetime] = []
                for h in headlines:
                    t = _parse_time(getattr(h, "time", ""))
                    if t is None:
                        continue
                    parsed_times.append(t)
                    rows.append(
                        {
                            "symbol": qsym,
                            "ib_symbol": isym,
                            "qlib_symbol": qsym,
                            "time": t.strftime("%Y-%m-%d %H:%M:%S"),
                            "providerCode": getattr(h, "providerCode", ""),
                            "articleId": getattr(h, "articleId", ""),
                            "headline": getattr(h, "headline", ""),
                        }
                    )

                if not parsed_times:
                    break

                oldest = min(parsed_times)
                if oldest <= start_dt:
                    break

                if len(headlines) < args.results_per_request:
                    break

                cursor_end = oldest - dt.timedelta(seconds=1)
                time.sleep(max(0.0, args.pause_seconds))

            merged = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
            merged = _dedup(merged)
            merged.to_csv(out_path, index=False)
            print(f"[info] wrote {qsym}: rows={len(merged)}")
            ok_symbols += 1
            time.sleep(max(0.0, args.pause_seconds))

        print(f"[info] done symbols_with_output={ok_symbols}/{len(symbols)}")
        return 0
    finally:
        ib.disconnect()


if __name__ == "__main__":
    raise SystemExit(run())
