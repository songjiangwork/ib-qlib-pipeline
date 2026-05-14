from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from ib_qlib_pipeline.cn_data.baostock_downloader import (
    BAOSTOCK_FIELDS,
    download_baostock_daily,
    parse_symbol_map,
)
from ib_qlib_pipeline.cn_data.qlib_exporter import export_baostock_raw_to_qlib_csv
from ib_qlib_pipeline.cn_data.symbols import (
    merge_qlib_symbol_lists,
    qlib_symbol_to_baostock,
    qlib_symbols_from_raw_codes,
    raw_code_to_qlib_symbol,
)


class _FakeQueryResult:
    def __init__(self, rows: list[list[str]], fields: list[str], error_code: str = "0", error_msg: str = "") -> None:
        self._rows = rows
        self.fields = fields
        self.error_code = error_code
        self.error_msg = error_msg
        self._index = -1

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._index]


class _FakeBaoStock:
    def __init__(self, responses: dict[str, _FakeQueryResult]) -> None:
        self.responses = responses

    def login(self):
        return SimpleNamespace(error_code="0", error_msg="success")

    def logout(self):
        return None

    def query_history_k_data_plus(self, code: str, fields: str, start_date: str, end_date: str, frequency: str, adjustflag: str):
        _ = (fields, start_date, end_date, frequency, adjustflag)
        return self.responses[code]


class CnDataTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "symbols" / "cn").mkdir(parents=True, exist_ok=True)
        (self.root / "data" / "cn" / "raw" / "baostock" / "daily").mkdir(parents=True, exist_ok=True)
        self.symbol_map = self.root / "symbols" / "cn" / "csi800_baostock_map.txt"
        self.metadata = self.root / "symbols" / "cn" / "csi800_metadata.csv"
        self.failed = self.root / "symbols" / "cn" / "failed_symbols.txt"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_symbol_conversions_and_merge(self) -> None:
        self.assertEqual("SH600519", raw_code_to_qlib_symbol("600519"))
        self.assertEqual("SZ000001", raw_code_to_qlib_symbol("1"))
        self.assertEqual("SH600519", raw_code_to_qlib_symbol("SH600519"))
        self.assertEqual("SZ000001", raw_code_to_qlib_symbol("sz.000001"))
        self.assertEqual("sh.600519", qlib_symbol_to_baostock("SH600519"))
        merged = merge_qlib_symbol_lists(["SH600519", "SZ000001"], ["SZ000001", "SZ300750"])
        self.assertEqual(["SH600519", "SZ000001", "SZ300750"], merged)
        self.assertEqual(["SH600519", "SZ000001"], qlib_symbols_from_raw_codes(["600519", "000001"]))

    def test_download_baostock_daily_writes_metadata_and_raw_csv(self) -> None:
        self.symbol_map.write_text("SH600000,sh.600000\nSZ000001,sz.000001\n", encoding="utf-8")
        ok_rows = [
            ["2016-01-04", "sh.600000", "10", "11", "9", "10.5", "10", "100", "1000", "2", "1", "1", "1", "0"],
            ["2016-01-05", "sh.600000", "10.5", "11", "10", "10.8", "10.5", "120", "1200", "2", "1", "1", "1", "0"],
        ]
        fake = _FakeBaoStock(
            {
                "sh.600000": _FakeQueryResult(ok_rows, BAOSTOCK_FIELDS),
                "sz.000001": _FakeQueryResult([], BAOSTOCK_FIELDS, error_code="1", error_msg="failed"),
            }
        )
        rows = download_baostock_daily(
            symbol_map_path=self.symbol_map,
            raw_dir=self.root / "data" / "cn" / "raw" / "baostock" / "daily",
            metadata_path=self.metadata,
            failed_file=self.failed,
            start_date="2016-01-01",
            end_date="2016-01-31",
            sleep_seconds=0.0,
            bs_module=fake,
        )
        self.assertEqual(2, len(rows))
        self.assertTrue((self.root / "data" / "cn" / "raw" / "baostock" / "daily" / "SH600000.csv").exists())
        metadata_text = self.metadata.read_text(encoding="utf-8")
        self.assertIn("SH600000", metadata_text)
        self.assertIn("SZ000001", self.failed.read_text(encoding="utf-8"))

    def test_export_baostock_raw_to_qlib_csv_filters_suspended_and_zero_volume(self) -> None:
        raw_dir = self.root / "data" / "cn" / "raw" / "baostock" / "daily"
        out_dir = self.root / "data" / "cn" / "processed" / "qlib_csv"
        raw = pd.DataFrame(
            [
                {
                    "date": "2016-01-04",
                    "code": "sh.600000",
                    "open": "10",
                    "high": "11",
                    "low": "9",
                    "close": "10.5",
                    "preclose": "10",
                    "volume": "100",
                    "amount": "1000",
                    "adjustflag": "2",
                    "turn": "1",
                    "tradestatus": "1",
                    "pctChg": "1",
                    "isST": "0",
                },
                {
                    "date": "2016-01-05",
                    "code": "sh.600000",
                    "open": "10.5",
                    "high": "11",
                    "low": "10",
                    "close": "10.8",
                    "preclose": "10.5",
                    "volume": "0",
                    "amount": "0",
                    "adjustflag": "2",
                    "turn": "0",
                    "tradestatus": "1",
                    "pctChg": "1",
                    "isST": "0",
                },
                {
                    "date": "2016-01-06",
                    "code": "sh.600000",
                    "open": "10.8",
                    "high": "11.2",
                    "low": "10.7",
                    "close": "11.0",
                    "preclose": "10.8",
                    "volume": "150",
                    "amount": "1300",
                    "adjustflag": "2",
                    "turn": "1",
                    "tradestatus": "0",
                    "pctChg": "1",
                    "isST": "0",
                },
            ]
        )
        raw.to_csv(raw_dir / "SH600000.csv", index=False)
        rows = export_baostock_raw_to_qlib_csv(
            raw_dir=raw_dir,
            out_dir=out_dir,
            metadata_path=self.metadata,
            failed_file=self.failed,
            start_date="2016-01-01",
            drop_suspended=True,
            drop_zero_volume=True,
        )
        self.assertEqual("ok", rows[0]["status"])
        exported = pd.read_csv(out_dir / "SH600000.csv")
        self.assertEqual(["date", "symbol", "open", "high", "low", "close", "volume", "factor"], list(exported.columns))
        self.assertEqual(1, len(exported))
        self.assertEqual("2016-01-04", exported.iloc[0]["date"])
        self.assertEqual(1, int(rows[0]["dropped_suspended_rows"]))
        self.assertEqual(1, int(rows[0]["dropped_zero_volume_rows"]))


if __name__ == "__main__":
    unittest.main()
