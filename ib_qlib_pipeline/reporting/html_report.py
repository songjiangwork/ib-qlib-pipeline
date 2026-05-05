from __future__ import annotations

import datetime as dt
import html
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.reporting.company_enricher import render_dl_rows, _fmt_num, _has_value
from ib_qlib_pipeline.ranking import TOP_N


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
        company_rows = render_dl_rows(
            [
                ("行业", meta.get("industry")),
                ("类别", meta.get("category")),
                ("交易所", meta.get("exchange")),
            ]
        )
        if not company_rows:
            company_rows = "<dt>说明</dt><dd>当前没有可用的公司补充信息。</dd>"
        price_rows = render_dl_rows(
            [
                ("最新交易日", stats.get("latest_date")),
                (
                    "最新开高低收",
                    (
                        f"{_fmt_num(stats.get('latest_open'))} / {_fmt_num(stats.get('latest_high'))} / "
                        f"{_fmt_num(stats.get('latest_low'))} / {_fmt_num(stats.get('latest_close'))}"
                    )
                    if any(_has_value(stats.get(k)) for k in ("latest_open", "latest_high", "latest_low", "latest_close"))
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
                ("30天均量", _fmt_num(stats.get("avg_volume_30d"), 0) if _has_value(stats.get("avg_volume_30d")) else None),
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
    .page {{ width: min(1180px, calc(100% - 32px)); margin: 28px auto 56px; }}
    .hero {{ padding: 28px; border: 1px solid var(--line); border-radius: 28px; background: linear-gradient(135deg, rgba(255,255,255,0.85), rgba(255,248,238,0.94)); box-shadow: var(--shadow); }}
    .eyebrow {{ display: inline-block; padding: 6px 10px; border-radius: 999px; background: var(--accent-soft); color: var(--accent); font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; }}
    h1 {{ margin: 14px 0 8px; font-size: clamp(34px, 5vw, 58px); line-height: 0.95; letter-spacing: -0.04em; }}
    .subhead {{ margin: 0; color: var(--muted); max-width: 720px; }}
    .meta-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 22px; }}
    .meta-card {{ padding: 16px 18px; border-radius: 18px; background: rgba(255,255,255,0.72); border: 1px solid var(--line); }}
    .meta-card span {{ display: block; color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }}
    .meta-card strong {{ display: block; margin-top: 6px; font-size: 26px; line-height: 1; }}
    .section {{ margin-top: 24px; padding: 24px; border-radius: 24px; background: var(--panel); border: 1px solid var(--line); box-shadow: var(--shadow); backdrop-filter: blur(10px); }}
    .section h2 {{ margin: 0 0 16px; font-size: 24px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 12px 10px; border-bottom: 1px solid var(--line); text-align: left; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }}
    .stock-list {{ display: grid; gap: 14px; }}
    .stock-card {{ overflow: hidden; border-radius: 22px; border: 1px solid var(--line); background: rgba(255,255,255,0.82); }}
    .stock-card summary {{ display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 18px 20px; cursor: pointer; list-style: none; }}
    .stock-card summary::-webkit-details-marker {{ display: none; }}
    .stock-card[open] summary {{ background: linear-gradient(90deg, rgba(15,118,110,0.08), rgba(215,161,43,0.10)); }}
    .summary-left {{ display: flex; align-items: center; gap: 14px; min-width: 0; }}
    .rank-badge {{ min-width: 54px; height: 54px; display: grid; place-items: center; border-radius: 16px; color: white; background: linear-gradient(135deg, var(--accent), #155e75); font-weight: 700; font-size: 20px; }}
    .ticker-line {{ font-size: 22px; font-weight: 700; letter-spacing: 0.02em; }}
    .company-line {{ color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 540px; }}
    .summary-right {{ display: flex; gap: 14px; flex-wrap: wrap; justify-content: flex-end; color: var(--muted); font-size: 14px; }}
    .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; padding: 0 20px 20px; }}
    .card-panel {{ padding: 16px 18px; border-radius: 18px; background: rgba(244, 239, 230, 0.72); border: 1px solid rgba(31, 41, 51, 0.08); }}
    .card-panel h3 {{ margin: 0 0 12px; font-size: 16px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--accent); }}
    dl {{ margin: 0; display: grid; grid-template-columns: minmax(96px, 130px) 1fr; gap: 8px 12px; }}
    dt {{ color: var(--muted); }}
    dd {{ margin: 0; }}
    pre {{ margin: 0; padding: 18px; overflow-x: auto; border-radius: 18px; background: #1d2732; color: #eef2f7; font: 13px/1.5 "SFMono-Regular", Consolas, "Liberation Mono", monospace; }}
    @media (max-width: 720px) {{
      .stock-card summary {{ flex-direction: column; align-items: flex-start; }}
      .summary-right {{ justify-content: flex-start; }}
      dl {{ grid-template-columns: 1fr; }}
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
        <thead><tr><th>Rank</th><th>Symbol</th><th>Score</th><th>Percentile</th><th>Close</th></tr></thead>
        <tbody>{''.join(summary_cards)}</tbody>
      </table>
    </section>

    <section class="section">
      <h2>Expandable Cards</h2>
      <div class="stock-list">{''.join(detail_cards)}</div>
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
