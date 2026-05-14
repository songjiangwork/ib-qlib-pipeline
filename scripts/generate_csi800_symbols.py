#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from ib_qlib_pipeline.cn_data.symbols import (
    CSI300_URL_CANDIDATES,
    CSI500_URL_CANDIDATES,
    download_excel,
    load_baostock_constituent_codes,
    load_constituent_codes,
    merge_qlib_symbol_lists,
    qlib_symbols_from_raw_codes,
    write_baostock_map,
    write_symbol_lines,
)


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate current CSI800 symbol files for A-share experiments.")
    parser.add_argument("--project-root", default=".", help="Project root path")
    parser.add_argument("--csi300-source", default=None, help="Optional local CSI300 xls/csv/txt path")
    parser.add_argument("--csi500-source", default=None, help="Optional local CSI500 xls/csv/txt path")
    parser.add_argument("--download-dir", default="symbols/cn/downloads", help="Directory for downloaded source files")
    parser.add_argument("--date", default=None, help="Optional query date YYYY-MM-DD for BaoStock constituent APIs")
    return parser.parse_args()


def _resolve_source(project_root: Path, explicit_path: str | None, download_path: Path, urls: list[str], label: str) -> Path:
    if explicit_path:
        path = (project_root / explicit_path).resolve() if not Path(explicit_path).is_absolute() else Path(explicit_path)
        if not path.exists():
            raise SystemExit(f"{label} source not found: {path}")
        return path
    try:
        return download_excel(urls, download_path)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            f"Failed to download {label} constituent file.\n"
            f"Please provide a local file with --{label.lower()}-source.\n"
            f"Error: {exc}"
        ) from exc


def _load_from_baostock(index_name: str, *, date: str | None) -> list[str]:
    raw_codes = load_baostock_constituent_codes(index_name, date=date)
    return qlib_symbols_from_raw_codes(raw_codes)


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    symbols_dir = project_root / "symbols" / "cn"
    download_dir = (project_root / args.download_dir).resolve()
    download_dir.mkdir(parents=True, exist_ok=True)

    csi300_symbols: list[str]
    csi500_symbols: list[str]
    if args.csi300_source:
        csi300_source = _resolve_source(
            project_root,
            args.csi300_source,
            download_dir / "000300cons.xls",
            CSI300_URL_CANDIDATES,
            "CSI300",
        )
        csi300_symbols = qlib_symbols_from_raw_codes(load_constituent_codes(csi300_source))
    else:
        csi300_symbols = _load_from_baostock("hs300", date=args.date)

    if args.csi500_source:
        csi500_source = _resolve_source(
            project_root,
            args.csi500_source,
            download_dir / "000905cons.xls",
            CSI500_URL_CANDIDATES,
            "CSI500",
        )
        csi500_symbols = qlib_symbols_from_raw_codes(load_constituent_codes(csi500_source))
    else:
        csi500_symbols = _load_from_baostock("zz500", date=args.date)

    csi800_symbols = merge_qlib_symbol_lists(csi300_symbols, csi500_symbols)

    write_symbol_lines(symbols_dir / "csi300_qlib.txt", csi300_symbols)
    write_symbol_lines(symbols_dir / "csi500_qlib.txt", csi500_symbols)
    write_symbol_lines(symbols_dir / "csi800_qlib.txt", csi800_symbols)
    write_baostock_map(symbols_dir / "csi800_baostock_map.txt", csi800_symbols)

    status(f"[ok] wrote {symbols_dir / 'csi300_qlib.txt'} rows={len(csi300_symbols)}")
    status(f"[ok] wrote {symbols_dir / 'csi500_qlib.txt'} rows={len(csi500_symbols)}")
    status(f"[ok] wrote {symbols_dir / 'csi800_qlib.txt'} rows={len(csi800_symbols)}")
    if len(csi300_symbols) != 300:
        status(f"[warn] CSI300 count is {len(csi300_symbols)}, expected around 300")
    if len(csi500_symbols) != 500:
        status(f"[warn] CSI500 count is {len(csi500_symbols)}, expected around 500")
    if len(csi800_symbols) < 750 or len(csi800_symbols) > 850:
        status(f"[warn] CSI800 deduped count is {len(csi800_symbols)}, expected around 800")
    status("[warn] Universe is current CSI800, not point-in-time historical CSI800")


if __name__ == "__main__":
    main()
