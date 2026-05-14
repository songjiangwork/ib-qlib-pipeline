from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.request import urlopen

import pandas as pd


CSI300_URL_CANDIDATES = [
    "https://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls",
]

CSI500_URL_CANDIDATES = [
    "https://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls",
    "https://www.csindex.com.cn/uploads/file/autofile/cons/000500cons.xls",
]


@dataclass(frozen=True)
class ConstituentRecord:
    qlib_symbol: str
    raw_code: str

    @property
    def baostock_code(self) -> str:
        return qlib_symbol_to_baostock(self.qlib_symbol)


def raw_code_to_qlib_symbol(code: str) -> str:
    text = str(code).strip()
    upper = text.upper()
    if len(upper) == 8 and upper[:2] in {"SH", "SZ", "BJ"} and upper[2:].isdigit():
        return upper
    lower = text.lower()
    if lower.startswith(("sh.", "sz.", "bj.")):
        market = lower[:2].upper()
        ticker = lower[3:].zfill(6)
        return f"{market}{ticker}"
    normalized = text.split(".")[0].zfill(6)
    if normalized.startswith("6"):
        return f"SH{normalized}"
    if normalized.startswith(("0", "3")):
        return f"SZ{normalized}"
    if normalized.startswith(("4", "8")):
        return f"BJ{normalized}"
    raise ValueError(f"Unknown exchange for code: {code}")


def qlib_symbol_to_baostock(symbol: str) -> str:
    text = str(symbol).strip().upper()
    market = text[:2].lower()
    ticker = text[2:]
    if market not in {"sh", "sz", "bj"}:
        raise ValueError(f"Unsupported Qlib symbol: {symbol}")
    return f"{market}.{ticker}"


def dedupe_symbols(symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for symbol in symbols:
        normalized = str(symbol).strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def merge_qlib_symbol_lists(*symbol_lists: Iterable[str]) -> list[str]:
    merged: list[str] = []
    for rows in symbol_lists:
        merged.extend([str(item).strip().upper() for item in rows if str(item).strip()])
    return dedupe_symbols(merged)


def download_excel(url_candidates: list[str], out_path: Path, timeout: float = 20.0) -> Path:
    errors: list[str] = []
    out_path.parent.mkdir(parents=True, exist_ok=True)
    for url in url_candidates:
        try:
            with urlopen(url, timeout=timeout) as response:
                content = response.read()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
            continue
        out_path.write_bytes(content)
        return out_path
    raise RuntimeError("Failed to download constituent file.\n" + "\n".join(errors))


def load_constituent_codes(source_path: Path) -> list[str]:
    if source_path.suffix.lower() in {".txt", ".csv"}:
        return _load_codes_from_text_like(source_path)
    return _load_codes_from_excel(source_path)


def _load_codes_from_text_like(source_path: Path) -> list[str]:
    rows: list[str] = []
    for raw in source_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        rows.append(line.split(",")[0].strip())
    return rows


def _load_codes_from_excel(source_path: Path) -> list[str]:
    frame = pd.read_excel(source_path)
    code_column = _find_code_column(frame)
    if code_column is None:
        raise RuntimeError(f"Could not find stock-code column in {source_path}")
    series = frame[code_column].dropna().astype(str).tolist()
    return [value.strip() for value in series if str(value).strip()]


def _find_code_column(frame: pd.DataFrame) -> str | None:
    candidates = [
        "成分券代码",
        "证券代码",
        "股票代码",
        "代码",
        "code",
        "Code",
    ]
    for candidate in candidates:
        if candidate in frame.columns:
            return str(candidate)
    normalized = {str(col).strip().lower(): col for col in frame.columns}
    for candidate in ["成分券代码", "证券代码", "股票代码", "代码", "code"]:
        key = candidate.strip().lower()
        if key in normalized:
            return str(normalized[key])
    return None


def qlib_symbols_from_raw_codes(raw_codes: Iterable[str]) -> list[str]:
    return dedupe_symbols(raw_code_to_qlib_symbol(code) for code in raw_codes)


def write_symbol_lines(path: Path, symbols: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def write_baostock_map(path: Path, qlib_symbols: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{symbol},{qlib_symbol_to_baostock(symbol)}" for symbol in qlib_symbols]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
