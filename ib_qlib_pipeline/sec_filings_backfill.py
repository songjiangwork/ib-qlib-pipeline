from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik10}.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill SEC filings (10-K/10-Q) by symbols list.")
    p.add_argument("--symbols-file", required=True, help="symbols file; supports qlib,ib format per line")
    p.add_argument("--out-dir", required=True, help="output directory for per-symbol SEC filings csv")
    p.add_argument(
        "--user-agent",
        required=True,
        help="SEC-required User-Agent, e.g. 'song-research/1.0 your_email@example.com'",
    )
    p.add_argument("--forms", default="10-K,10-Q", help="comma-separated forms, e.g. 10-K,10-Q,8-K")
    p.add_argument("--start-date", default=None, help="YYYY-MM-DD; optional filter")
    p.add_argument("--pause-seconds", type=float, default=0.2, help="pause between SEC requests")
    return p.parse_args()


def _load_symbols(path: Path) -> list[str]:
    symbols: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        parts = [x.strip() for x in s.split(",")]
        symbols.append(parts[0].upper())
    return symbols


def _sec_get_json(url: str, user_agent: str) -> Any:
    req = Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    with urlopen(req, timeout=20) as resp:  # nosec B310
        return json.loads(resp.read().decode("utf-8"))


def _build_ticker_to_cik(ticker_map_json: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    if isinstance(ticker_map_json, dict):
        for _, item in ticker_map_json.items():
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker", "")).upper().strip()
            cik = item.get("cik_str")
            if not ticker or cik is None:
                continue
            try:
                out[ticker] = int(cik)
            except Exception:
                continue
    return out


def _normalize_symbol_for_sec(symbol: str) -> str:
    # SEC ticker files commonly use '-' for dual-class symbols (e.g. BRK-B, BF-B).
    return symbol.upper().replace(".", "-")


def _date_ok(d: str, start_date: dt.date | None) -> bool:
    if not start_date:
        return True
    try:
        return dt.datetime.strptime(d, "%Y-%m-%d").date() >= start_date
    except Exception:
        return False


def _extract_recent_filings(sub_json: Any, symbol: str, cik: int, forms: set[str], start_date: dt.date | None) -> list[dict]:
    filings = (((sub_json or {}).get("filings") or {}).get("recent") or {})
    form_l = filings.get("form") or []
    filing_date_l = filings.get("filingDate") or []
    report_date_l = filings.get("reportDate") or []
    acc_l = filings.get("accessionNumber") or []
    accept_l = filings.get("acceptanceDateTime") or []
    doc_l = filings.get("primaryDocument") or []
    desc_l = filings.get("primaryDocDescription") or []

    n = min(len(form_l), len(filing_date_l))
    rows: list[dict] = []
    for i in range(n):
        form = str(form_l[i]).strip().upper()
        filing_date = str(filing_date_l[i]).strip()
        if form not in forms or not _date_ok(filing_date, start_date):
            continue
        rows.append(
            {
                "symbol": symbol,
                "cik": cik,
                "form": form,
                "filingDate": filing_date,
                "reportDate": (report_date_l[i] if i < len(report_date_l) else "") or "",
                "acceptanceDateTime": (accept_l[i] if i < len(accept_l) else "") or "",
                "accessionNumber": (acc_l[i] if i < len(acc_l) else "") or "",
                "primaryDocument": (doc_l[i] if i < len(doc_l) else "") or "",
                "primaryDocDescription": (desc_l[i] if i < len(desc_l) else "") or "",
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def run() -> int:
    args = parse_args()
    symbols = _load_symbols(Path(args.symbols_file))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    forms = {x.strip().upper() for x in args.forms.split(",") if x.strip()}
    start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else None

    print(f"[info] loading SEC ticker map from {TICKER_MAP_URL}")
    ticker_map = _build_ticker_to_cik(_sec_get_json(TICKER_MAP_URL, args.user_agent))
    print(f"[info] ticker map size={len(ticker_map)}")

    all_rows: list[dict] = []
    missing_cik: list[str] = []
    failures: list[dict] = []

    for i, sym in enumerate(symbols, start=1):
        sec_sym = _normalize_symbol_for_sec(sym)
        cik = ticker_map.get(sec_sym)
        if cik is None:
            missing_cik.append(sym)
            print(f"[warn] ({i}/{len(symbols)}) no CIK mapping for {sym}")
            continue

        cik10 = f"{cik:010d}"
        url = SUBMISSIONS_URL_TMPL.format(cik10=cik10)
        print(f"[info] ({i}/{len(symbols)}) {sym} cik={cik10}")
        try:
            sub_json = _sec_get_json(url, args.user_agent)
            rows = _extract_recent_filings(sub_json, sym, cik, forms, start_date)
            symbol_path = out_dir / f"{sym}.csv"
            _write_csv(
                symbol_path,
                rows,
                [
                    "symbol",
                    "cik",
                    "form",
                    "filingDate",
                    "reportDate",
                    "acceptanceDateTime",
                    "accessionNumber",
                    "primaryDocument",
                    "primaryDocDescription",
                ],
            )
            print(f"[info] wrote {symbol_path} rows={len(rows)}")
            all_rows.extend(rows)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            failures.append({"symbol": sym, "cik": cik10, "error": str(exc)})
            print(f"[warn] SEC fetch failed for {sym} cik={cik10}: {exc}")
        except Exception as exc:  # noqa: BLE001
            failures.append({"symbol": sym, "cik": cik10, "error": str(exc)})
            print(f"[warn] SEC parse failed for {sym} cik={cik10}: {exc}")

        time.sleep(max(0.0, args.pause_seconds))

    all_rows = sorted(all_rows, key=lambda x: (x["filingDate"], x["symbol"], x["form"]), reverse=True)
    summary_path = out_dir / "_all_filings.csv"
    _write_csv(
        summary_path,
        all_rows,
        [
            "symbol",
            "cik",
            "form",
            "filingDate",
            "reportDate",
            "acceptanceDateTime",
            "accessionNumber",
            "primaryDocument",
            "primaryDocDescription",
        ],
    )

    meta = {
        "symbols_total": len(symbols),
        "rows_total": len(all_rows),
        "forms": sorted(forms),
        "start_date": str(start_date) if start_date else None,
        "missing_cik_count": len(missing_cik),
        "missing_cik_symbols": missing_cik[:50],
        "failures_count": len(failures),
        "failures": failures[:50],
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    meta_path = out_dir / "_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[info] wrote summary: {summary_path}")
    print(f"[info] wrote meta: {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
