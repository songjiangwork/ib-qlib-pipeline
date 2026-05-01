from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


NEG_WORDS = {
    "miss",
    "down",
    "cut",
    "lawsuit",
    "probe",
    "fraud",
    "drop",
    "fall",
    "decline",
    "weak",
    "risk",
    "warning",
    "loss",
    "bearish",
}

POS_WORDS = {
    "beat",
    "up",
    "raise",
    "growth",
    "surge",
    "gain",
    "strong",
    "record",
    "bullish",
    "upgrade",
    "profit",
    "improve",
    "expand",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aggregate raw news into daily symbol features and merge into Qlib CSV.")
    p.add_argument("--symbols-file", required=True, help="Path to symbols file (supports qlib,ib format).")
    p.add_argument("--news-dir", required=True, help="Directory of per-symbol raw news csv.")
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


def _headline_sentiment(headline: str) -> float:
    text = str(headline).lower()
    pos = sum(1 for w in POS_WORDS if w in text)
    neg = sum(1 for w in NEG_WORDS if w in text)
    return float(pos - neg)


def _build_news_feature(news_df: pd.DataFrame) -> pd.DataFrame:
    if news_df.empty:
        return pd.DataFrame(columns=["date", "symbol", "news_count", "news_sentiment", "news_negative_ratio"])

    news_df = news_df.copy()
    news_df["time"] = pd.to_datetime(news_df["time"], errors="coerce")
    news_df = news_df.dropna(subset=["time"])
    news_df["date"] = news_df["time"].dt.strftime("%Y-%m-%d")
    news_df["sent_score"] = news_df["headline"].map(_headline_sentiment)
    news_df["is_negative"] = (news_df["sent_score"] < 0).astype(float)

    agg = (
        news_df.groupby(["date", "symbol"], as_index=False)
        .agg(
            news_count=("headline", "count"),
            news_sentiment=("sent_score", "mean"),
            news_negative_ratio=("is_negative", "mean"),
        )
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )
    return agg


def run() -> int:
    args = parse_args()
    symbols = _load_symbols(Path(args.symbols_file))
    news_dir = Path(args.news_dir)
    price_dir = Path(args.price_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    done = 0
    for symbol in symbols:
        price_path = price_dir / f"{symbol}.csv"
        if not price_path.exists():
            continue

        price_df = pd.read_csv(price_path)
        if price_df.empty:
            continue

        news_path = news_dir / f"{symbol}.csv"
        if news_path.exists():
            news_df = pd.read_csv(news_path)
            feat_df = _build_news_feature(news_df)
        else:
            feat_df = pd.DataFrame(columns=["date", "symbol", "news_count", "news_sentiment", "news_negative_ratio"])

        merged = price_df.merge(feat_df, on=["date", "symbol"], how="left")
        merged["news_count"] = merged["news_count"].fillna(0).astype(float)
        merged["news_sentiment"] = merged["news_sentiment"].fillna(0.0).astype(float)
        merged["news_negative_ratio"] = merged["news_negative_ratio"].fillna(0.0).astype(float)

        out_path = out_dir / f"{symbol}.csv"
        merged.to_csv(out_path, index=False)
        done += 1

    print(f"merged_symbols={done}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
