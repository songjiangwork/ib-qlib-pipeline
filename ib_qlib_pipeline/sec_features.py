from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aggregate SEC filings into daily symbol features and merge into Qlib CSV.")
    p.add_argument("--symbols-file", required=True, help="Path to symbols file (supports qlib,ib format).")
    p.add_argument("--filings-dir", required=True, help="Directory of per-symbol SEC filings csv.")
    p.add_argument("--price-dir", required=True, help="Directory of per-symbol qlib price csv.")
    p.add_argument("--out-dir", required=True, help="Output directory for merged csv.")
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


def _build_sec_features(filings_df: pd.DataFrame, price_dates: pd.Series) -> pd.DataFrame:
    base = pd.DataFrame({"date": price_dates.astype(str)})
    if filings_df.empty:
        base["sec_is_10k_day"] = 0.0
        base["sec_is_10q_day"] = 0.0
        base["sec_days_since_filing"] = -1.0
        return base

    df = filings_df.copy()
    df["form"] = df["form"].astype(str).str.upper().str.strip()
    df["filingDate"] = pd.to_datetime(df["filingDate"], errors="coerce")
    df = df.dropna(subset=["filingDate"])
    if df.empty:
        base["sec_is_10k_day"] = 0.0
        base["sec_is_10q_day"] = 0.0
        base["sec_days_since_filing"] = -1.0
        return base

    df["date"] = df["filingDate"].dt.strftime("%Y-%m-%d")
    day_flags = (
        df.groupby("date", as_index=False)
        .agg(
            sec_is_10k_day=("form", lambda x: float((x == "10-K").any())),
            sec_is_10q_day=("form", lambda x: float((x == "10-Q").any())),
        )
        .sort_values("date")
    )
    merged = base.merge(day_flags, on="date", how="left")
    merged["sec_is_10k_day"] = merged["sec_is_10k_day"].fillna(0.0).astype(float)
    merged["sec_is_10q_day"] = merged["sec_is_10q_day"].fillna(0.0).astype(float)

    # Forward-looking neutral encoding for no-history period is -1.
    dts = pd.to_datetime(merged["date"], errors="coerce")
    filing_days = sorted(pd.to_datetime(day_flags["date"], errors="coerce").dropna().tolist())
    since_vals: list[float] = []
    j = 0
    last = None
    for cur in dts:
        while j < len(filing_days) and filing_days[j] <= cur:
            last = filing_days[j]
            j += 1
        if last is None or pd.isna(cur):
            since_vals.append(-1.0)
        else:
            since_vals.append(float((cur - last).days))
    merged["sec_days_since_filing"] = since_vals
    return merged


def run() -> int:
    args = parse_args()
    symbols = _load_symbols(Path(args.symbols_file))
    filings_dir = Path(args.filings_dir)
    price_dir = Path(args.price_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    done = 0
    for symbol in symbols:
        price_path = price_dir / f"{symbol}.csv"
        if not price_path.exists():
            continue
        price_df = pd.read_csv(price_path)
        if price_df.empty or "date" not in price_df.columns:
            continue

        filings_path = filings_dir / f"{symbol}.csv"
        filings_df = pd.read_csv(filings_path) if filings_path.exists() else pd.DataFrame()
        feat_df = _build_sec_features(filings_df, price_df["date"])
        merged = price_df.merge(feat_df, on="date", how="left")
        merged["sec_is_10k_day"] = merged["sec_is_10k_day"].fillna(0.0).astype(float)
        merged["sec_is_10q_day"] = merged["sec_is_10q_day"].fillna(0.0).astype(float)
        merged["sec_days_since_filing"] = merged["sec_days_since_filing"].fillna(-1.0).astype(float)

        out_path = out_dir / f"{symbol}.csv"
        merged.to_csv(out_path, index=False)
        done += 1

    print(f"merged_symbols={done}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
