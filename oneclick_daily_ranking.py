#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import html
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from ib_insync import IB, Contract, Stock

TOP_N = 20


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="One-click: incremental data update + qlib run + ranking export")
    p.add_argument("--client-id", type=int, default=151, help="IB client id (default: 151)")
    p.add_argument("--lookback-days", type=int, default=7, help="Incremental fetch window in days (default: 7)")
    p.add_argument(
        "--workflow-base",
        default="examples/workflow_us_lgb_2020_port.yaml",
        help="Base workflow yaml path",
    )
    return p.parse_args()


def log(message: str, console_lines: list[str]) -> None:
    print(message)
    console_lines.append(message)


def run_cmd(cmd: list[str], cwd: Path, console_lines: list[str], env: dict[str, str] | None = None) -> None:
    rendered = " ".join(cmd)
    log(f"[run] {rendered}", console_lines)
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\n")
        print(line)
        console_lines.append(line)
    rc = proc.wait()
    if rc != 0:
        raise SystemExit(rc)


def find_latest_pred(project_root: Path) -> tuple[Path, str, str]:
    preds = []
    for p in (project_root / "mlruns").glob("*/*/artifacts/pred.pkl"):
        preds.append((p.stat().st_mtime, p))
    if not preds:
        raise RuntimeError("No pred.pkl found under mlruns/")
    _, pred_path = max(preds, key=lambda x: x[0])
    parts = pred_path.parts
    exp_id = parts[-4]
    rec_id = parts[-3]
    return pred_path, exp_id, rec_id


def next_rank_file(out_dir: Path, run_date: dt.date) -> Path:
    base = out_dir / f"sp500_ranking_{run_date.isoformat()}.csv"
    if not base.exists():
        return base
    i = 1
    while True:
        candidate = out_dir / f"sp500_ranking_{run_date.isoformat()}-{i:02d}.csv"
        if not candidate.exists():
            return candidate
        i += 1


def read_available_trading_days(project_root: Path) -> list[dt.date]:
    cal_path = project_root / "data" / "qlib" / "us_data_custom" / "calendars" / "day.txt"
    if not cal_path.exists():
        raise SystemExit(f"Missing qlib calendar: {cal_path}")
    days = [
        dt.date.fromisoformat(line.strip())
        for line in cal_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if len(days) < 2:
        raise SystemExit(f"Qlib calendar has insufficient dates: {cal_path}")
    return days


def load_ranking_dataframe(project_root: Path, pred_path: Path, exp_id: str, rec_id: str) -> pd.DataFrame:
    df = pd.read_pickle(pred_path)
    d = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()

    date_col = "datetime" if "datetime" in d.columns else "date"
    inst_col = "instrument" if "instrument" in d.columns else "symbol"
    score_col = "score" if "score" in d.columns else d.select_dtypes("number").columns[-1]

    d[date_col] = pd.to_datetime(d[date_col])
    signal_date = d[date_col].max().date()
    cur = d[d[date_col].dt.date == signal_date][[inst_col, score_col]].copy()
    cur.columns = ["symbol", "score"]
    cur = cur.sort_values("score", ascending=False).reset_index(drop=True)
    cur["rank"] = cur.index + 1
    cur["percentile"] = cur["score"].rank(pct=True, ascending=True) * 100

    price_dir = project_root / "data" / "processed" / "qlib_csv"
    close_vals = []
    for sym in cur["symbol"]:
        fp = price_dir / f"{sym}.csv"
        p = pd.read_csv(fp, usecols=["date", "close"])
        p["date"] = pd.to_datetime(p["date"]).dt.date
        hit = p.loc[p["date"] == signal_date, "close"]
        close_vals.append(float(hit.iloc[-1]) if len(hit) else float("nan"))
    cur["close"] = close_vals

    run_date = dt.date.today()
    cur["run_date"] = pd.to_datetime(run_date)
    cur["signal_date"] = pd.to_datetime(signal_date)
    cur["experiment_id"] = exp_id
    cur["recorder_id"] = rec_id
    return cur[
        ["run_date", "signal_date", "rank", "symbol", "score", "percentile", "close", "experiment_id", "recorder_id"]
    ]


def export_ranking_csv(project_root: Path, ranking_df: pd.DataFrame, console_lines: list[str]) -> Path:
    out_dir = project_root / "reports" / "rankings"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_date = pd.to_datetime(ranking_df["run_date"].iloc[0]).date()
    out_file = next_rank_file(out_dir, run_date)
    ranking_df.to_csv(out_file, index=False)

    signal_date = pd.to_datetime(ranking_df["signal_date"].iloc[0]).date()
    log(f"[ok] ranking exported: {out_file}", console_lines)
    log(
        f"[ok] signal_date={signal_date} rows={len(ranking_df)} missing_close={int(ranking_df['close'].isna().sum())}",
        console_lines,
    )
    log(f"[top{TOP_N}]", console_lines)
    topn_text = ranking_df[["rank", "symbol", "score", "close"]].head(TOP_N).to_string(index=False)
    print(topn_text)
    console_lines.extend(topn_text.splitlines())
    return out_file


def _build_stock_contract(symbol: str) -> Contract:
    return Stock(symbol, "SMART", "USD")


def _fetch_pe_ratio(ib: IB, contract: Contract) -> Optional[str]:
    # IB fundamentals are optional for this report. On accounts without the
    # required entitlement, reqFundamentalData emits noisy 10358 errors even
    # though the ranking pipeline itself is otherwise healthy, so skip it.
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


def _render_dl_rows(rows: list[tuple[str, object]]) -> str:
    parts: list[str] = []
    for label, value in rows:
        if not _has_value(value):
            continue
        parts.append(f"<dt>{html.escape(label)}</dt><dd>{html.escape(str(value))}</dd>")
    return "".join(parts)


def _build_price_stats(project_root: Path, symbol: str) -> dict[str, object]:
    price_path = project_root / "data" / "processed" / "qlib_csv" / f"{symbol}.csv"
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


def _load_ib_config(project_root: Path) -> tuple[str, int]:
    cfg = yaml.safe_load((project_root / "config.yaml").read_text(encoding="utf-8"))
    return str(cfg["ib"]["host"]), int(cfg["ib"]["port"])


def _company_meta_cache_path(project_root: Path, symbol: str) -> Path:
    return project_root / "data" / "raw" / "company_meta" / f"{symbol}.json"


def _load_company_meta_cache(project_root: Path, symbol: str) -> dict[str, str]:
    path = _company_meta_cache_path(project_root, symbol)
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return {}


def fetch_topn_company_data(project_root: Path, topn: pd.DataFrame, client_id: int, console_lines: list[str]) -> list[dict]:
    host, port = _load_ib_config(project_root)
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
                "price_stats": _build_price_stats(project_root, symbol),
            }
            cached = _load_company_meta_cache(project_root, symbol)
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
                    card["metadata"] = {
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
            except Exception as exc:  # noqa: BLE001
                log(f"[warn] failed to enrich {symbol} for html: {exc}", console_lines)
                if not card["metadata"]:
                    card["metadata"] = {
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
            rows.append(card)
    except Exception as exc:  # noqa: BLE001
        log(f"[warn] html enrichment skipped: {exc}", console_lines)
        for row in topn.itertuples(index=False):
            cached = _load_company_meta_cache(project_root, str(row.symbol))
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
                    "price_stats": _build_price_stats(project_root, str(row.symbol)),
                }
            )
    finally:
        if ib.isConnected():
            ib.disconnect()
    return rows


def build_html_report(
    ranking_df: pd.DataFrame,
    topn_details: list[dict],
    html_path: Path,
    console_lines: list[str],
) -> None:
    run_date = pd.to_datetime(ranking_df["run_date"].iloc[0]).date().isoformat()
    signal_date = pd.to_datetime(ranking_df["signal_date"].iloc[0]).date().isoformat()
    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary_cards = []
    topn = ranking_df.head(TOP_N)
    for row in topn.itertuples(index=False):
        summary_cards.append(
            f"""
            <tr>
              <td>{int(row.rank)}</td>
              <td>{html.escape(str(row.symbol))}</td>
              <td>{_fmt_num(row.score, 6)}</td>
              <td>{_fmt_num(row.percentile, 2, "%")}</td>
              <td>{_fmt_num(row.close, 2)}</td>
            </tr>
            """
        )

    detail_cards = []
    for item in topn_details:
        meta = item["metadata"]
        stats = item["price_stats"]
        title = html.escape(meta.get("company_name") or item["symbol"])
        company_rows = _render_dl_rows(
            [
                ("行业", meta.get("industry")),
                ("类别", meta.get("category")),
                ("交易所", meta.get("exchange")),
            ]
        )
        if not company_rows:
            company_rows = "<dt>说明</dt><dd>当前没有可用的公司补充信息。</dd>"
        price_rows = _render_dl_rows(
            [
                ("最新交易日", stats.get("latest_date")),
                (
                    "最新开高低收",
                    (
                        f"{_fmt_num(stats.get('latest_open'))} / {_fmt_num(stats.get('latest_high'))} / "
                        f"{_fmt_num(stats.get('latest_low'))} / {_fmt_num(stats.get('latest_close'))}"
                    )
                    if any(
                        _has_value(stats.get(k))
                        for k in ("latest_open", "latest_high", "latest_low", "latest_close")
                    )
                    else None
                ),
                (
                    "30天区间",
                    f"{_fmt_num(stats.get('day_30_low'))} - {_fmt_num(stats.get('day_30_high'))}"
                    if _has_value(stats.get("day_30_low")) or _has_value(stats.get("day_30_high"))
                    else None
                ),
                (
                    "52周区间",
                    f"{_fmt_num(stats.get('week_52_low'))} - {_fmt_num(stats.get('week_52_high'))}"
                    if _has_value(stats.get("week_52_low")) or _has_value(stats.get("week_52_high"))
                    else None
                ),
                (
                    "历史区间",
                    f"{_fmt_num(stats.get('all_time_low'))} - {_fmt_num(stats.get('all_time_high'))}"
                    if _has_value(stats.get("all_time_low")) or _has_value(stats.get("all_time_high"))
                    else None
                ),
                ("30天均量", _fmt_int(stats.get("avg_volume_30d")) if _has_value(stats.get("avg_volume_30d")) else None),
                ("Percentile", _fmt_num(item["percentile"], 2, "%")),
            ]
        )
        detail_cards.append(
            f"""
            <details class="stock-card" {'open' if item['rank'] == 1 else ''}>
              <summary>
                <div class="summary-left">
                  <span class="rank-badge">#{item['rank']}</span>
                  <div>
                    <div class="ticker-line">{html.escape(item['symbol'])}</div>
                    <div class="company-line">{title}</div>
                  </div>
                </div>
                <div class="summary-right">
                  <span>Score {_fmt_num(item['score'], 6)}</span>
                  <span>Close {_fmt_num(item['close'], 2)}</span>
                </div>
              </summary>
              <div class="card-grid">
                <div class="card-panel">
                  <h3>Company</h3>
                  <dl>{company_rows}</dl>
                </div>
                <div class="card-panel">
                  <h3>Price Snapshot</h3>
                  <dl>{price_rows}</dl>
                </div>
              </div>
            </details>
            """
        )

    console_block = html.escape("\n".join(console_lines))
    page = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Top {TOP_N} Ranking Report {run_date}</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --ink: #1f2933;
      --muted: #5f6c7b;
      --panel: rgba(255, 250, 243, 0.92);
      --line: rgba(31, 41, 51, 0.12);
      --accent: #0f766e;
      --accent-soft: #d9f3ed;
      --gold: #d7a12b;
      --shadow: 0 20px 60px rgba(31, 41, 51, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font: 16px/1.55 Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top left, rgba(215, 161, 43, 0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(15, 118, 110, 0.18), transparent 24%),
        linear-gradient(180deg, #f7f3ec 0%, var(--bg) 100%);
    }}
    .page {{
      width: min(1180px, calc(100% - 32px));
      margin: 28px auto 56px;
    }}
    .hero {{
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 28px;
      background: linear-gradient(135deg, rgba(255,255,255,0.85), rgba(255,248,238,0.94));
      box-shadow: var(--shadow);
    }}
    .eyebrow {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 14px 0 8px;
      font-size: clamp(34px, 5vw, 58px);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }}
    .subhead {{
      margin: 0;
      color: var(--muted);
      max-width: 720px;
    }}
    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-top: 22px;
    }}
    .meta-card {{
      padding: 16px 18px;
      border-radius: 18px;
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--line);
    }}
    .meta-card span {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .meta-card strong {{
      display: block;
      margin-top: 6px;
      font-size: 26px;
      line-height: 1;
    }}
    .section {{
      margin-top: 24px;
      padding: 24px;
      border-radius: 24px;
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .section h2 {{
      margin: 0 0 16px;
      font-size: 24px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .stock-list {{
      display: grid;
      gap: 14px;
    }}
    .stock-card {{
      overflow: hidden;
      border-radius: 22px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.82);
    }}
    .stock-card summary {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 20px;
      cursor: pointer;
      list-style: none;
    }}
    .stock-card summary::-webkit-details-marker {{ display: none; }}
    .stock-card[open] summary {{
      background: linear-gradient(90deg, rgba(15,118,110,0.08), rgba(215,161,43,0.10));
    }}
    .summary-left {{
      display: flex;
      align-items: center;
      gap: 14px;
      min-width: 0;
    }}
    .rank-badge {{
      min-width: 54px;
      height: 54px;
      display: grid;
      place-items: center;
      border-radius: 16px;
      color: white;
      background: linear-gradient(135deg, var(--accent), #155e75);
      font-weight: 700;
      font-size: 20px;
    }}
    .ticker-line {{
      font-size: 22px;
      font-weight: 700;
      letter-spacing: 0.02em;
    }}
    .company-line {{
      color: var(--muted);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 540px;
    }}
    .summary-right {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      justify-content: flex-end;
      color: var(--muted);
      font-size: 14px;
    }}
    .card-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      padding: 0 20px 20px;
    }}
    .card-panel {{
      padding: 16px 18px;
      border-radius: 18px;
      background: rgba(244, 239, 230, 0.72);
      border: 1px solid rgba(31, 41, 51, 0.08);
    }}
    .card-panel h3 {{
      margin: 0 0 12px;
      font-size: 16px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--accent);
    }}
    dl {{
      margin: 0;
      display: grid;
      grid-template-columns: minmax(96px, 130px) 1fr;
      gap: 8px 12px;
    }}
    dt {{
      color: var(--muted);
    }}
    dd {{
      margin: 0;
    }}
    pre {{
      margin: 0;
      padding: 18px;
      overflow-x: auto;
      border-radius: 18px;
      background: #1d2732;
      color: #eef2f7;
      font: 13px/1.5 "SFMono-Regular", Consolas, "Liberation Mono", monospace;
    }}
    @media (max-width: 720px) {{
      .stock-card summary {{
        flex-direction: column;
        align-items: flex-start;
      }}
      .summary-right {{
        justify-content: flex-start;
      }}
      dl {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <span class="eyebrow">Daily Ranking Report</span>
      <h1>Top {TOP_N} picks for {signal_date}</h1>
      <p class="subhead">只保留本次运行最终推荐的前 {TOP_N} 只股票。每张卡片都可以展开，查看行业、类别、交易所以及价格区间、30 天和 52 周范围、成交量等补充信息。</p>
      <div class="meta-grid">
        <article class="meta-card"><span>Run Date</span><strong>{run_date}</strong></article>
        <article class="meta-card"><span>Signal Date</span><strong>{signal_date}</strong></article>
        <article class="meta-card"><span>Generated At</span><strong>{generated_at}</strong></article>
        <article class="meta-card"><span>Total Ranked</span><strong>{len(ranking_df)}</strong></article>
        <article class="meta-card"><span>Top {TOP_N} Avg Score</span><strong>{_fmt_num(topn['score'].mean(), 4)}</strong></article>
      </div>
    </section>

    <section class="section">
      <h2>Top {TOP_N} Snapshot</h2>
      <table>
        <thead>
          <tr>
            <th>Rank</th>
            <th>Symbol</th>
            <th>Score</th>
            <th>Percentile</th>
            <th>Close</th>
          </tr>
        </thead>
        <tbody>
          {''.join(summary_cards)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Expandable Cards</h2>
      <div class="stock-list">
        {''.join(detail_cards)}
      </div>
    </section>

    <section class="section">
      <h2>Console Transcript</h2>
      <pre>{console_block}</pre>
    </section>
  </main>
</body>
</html>
"""
    html_path.write_text(page, encoding="utf-8")


def main() -> None:
    args = parse_args()
    console_lines: list[str] = []
    project_root = Path(__file__).resolve().parent
    qlib_qrun = Path("/home/song/projects/qlib/.venv/bin/qrun")
    if not qlib_qrun.exists():
        raise SystemExit("Missing qrun: /home/song/projects/qlib/.venv/bin/qrun")

    today = dt.date.today()
    start_date = today - dt.timedelta(days=max(1, args.lookback_days))

    run_cmd(
        [
            str(project_root / ".venv" / "bin" / "python"),
            "run.py",
            "--config",
            "config.yaml",
            "--client-id",
            str(args.client_id),
            "--start-date",
            start_date.isoformat(),
            "--bar-size",
            "1 day",
            "--no-news",
            "--dump-bin",
        ],
        cwd=project_root,
        console_lines=console_lines,
    )

    trading_days = read_available_trading_days(project_root)
    latest_trade_date = trading_days[-1]
    backtest_end = trading_days[-2]
    if backtest_end < dt.date(2025, 1, 2):
        backtest_end = dt.date(2025, 1, 2)

    base_path = project_root / args.workflow_base
    if not base_path.exists():
        raise SystemExit(f"Base workflow not found: {base_path}")
    wf = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    wf["data_handler_config"]["end_time"] = latest_trade_date.isoformat()
    wf["task"]["dataset"]["kwargs"]["segments"]["test"][1] = latest_trade_date.isoformat()
    wf["port_analysis_config"]["backtest"]["end_time"] = backtest_end.isoformat()

    tmp_dir = project_root / "reports" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    runtime_wf = tmp_dir / f"workflow_runtime_{today.isoformat()}.yaml"
    runtime_wf.write_text(yaml.safe_dump(wf, sort_keys=False, allow_unicode=False), encoding="utf-8")
    log(f"[ok] runtime workflow: {runtime_wf}", console_lines)
    log(f"[ok] latest_trade_date={latest_trade_date} backtest_end={backtest_end}", console_lines)

    qrun_env = os.environ.copy()
    qrun_env["GIT_DIR"] = str(qlib_qrun.parent.parent.parent / ".git")
    qrun_env["GIT_WORK_TREE"] = str(qlib_qrun.parent.parent.parent)
    run_cmd([str(qlib_qrun), str(runtime_wf)], cwd=project_root, console_lines=console_lines, env=qrun_env)

    pred_path, exp_id, rec_id = find_latest_pred(project_root)
    log(f"[ok] pred={pred_path}", console_lines)
    log(f"[ok] experiment_id={exp_id} recorder_id={rec_id}", console_lines)

    ranking_df = load_ranking_dataframe(project_root, pred_path, exp_id, rec_id)
    csv_path = export_ranking_csv(project_root, ranking_df, console_lines)

    topn_details = fetch_topn_company_data(project_root, ranking_df.head(TOP_N), args.client_id, console_lines)
    html_stamp = dt.datetime.now().strftime("%H%M%S")
    run_date_str = pd.to_datetime(ranking_df["run_date"].iloc[0]).date().isoformat()
    html_path = csv_path.with_name(f"sp500_ranking_{run_date_str}_{html_stamp}.html")
    build_html_report(ranking_df, topn_details, html_path, console_lines)
    log(f"[ok] html report exported: {html_path}", console_lines)


if __name__ == "__main__":
    main()
