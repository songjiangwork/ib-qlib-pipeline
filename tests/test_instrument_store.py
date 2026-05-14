from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ib_qlib_pipeline.cn_data.instrument_enricher import enrich_cn_instruments_from_baostock
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.instrument_store import (
    get_instrument_by_symbol,
    list_instrument_aliases,
    list_instruments,
    upsert_instrument,
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
    def query_stock_basic(self):
        return _FakeQueryResult(
            [
                ["sh.600519", "贵州茅台", "2001-08-27", "", "1", "1"],
                ["sz.300750", "宁德时代", "2018-06-11", "", "1", "1"],
            ],
            ["code", "code_name", "ipoDate", "outDate", "type", "status"],
        )

    def query_stock_industry(self):
        return _FakeQueryResult(
            [
                ["2026-05-14", "sh.600519", "贵州茅台", "白酒", "消费"],
                ["2026-05-14", "sz.300750", "宁德时代", "电池", "新能源"],
            ],
            ["updateDate", "code", "code_name", "industry", "industryClassification"],
        )


class InstrumentStoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "ranking_service.db"
        init_db(self.db_path)
        self.symbols_path = Path(self._tmp.name) / "csi800_qlib.txt"
        self.symbols_path.write_text("SH600519\nSZ300750\n", encoding="utf-8")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_upsert_instrument_preserves_non_null_fields(self) -> None:
        row, created = upsert_instrument(
            self.db_path,
            {
                "canonical_symbol": "SH600519",
                "display_symbol": "SH600519",
                "asset_type": "stock",
                "country": "CN",
                "exchange": "SH",
                "currency": "CNY",
                "name_zh": "贵州茅台",
                "source": "baostock",
            },
        )
        self.assertTrue(created)
        updated, created_again = upsert_instrument(
            self.db_path,
            {
                "canonical_symbol": "SH600519",
                "display_symbol": "SH600519",
                "asset_type": "stock",
                "country": "CN",
                "exchange": "SH",
                "currency": "CNY",
                "name_zh": None,
                "industry": "白酒",
                "source": "baostock",
            },
        )
        self.assertFalse(created_again)
        self.assertEqual("贵州茅台", updated["name_zh"])
        self.assertEqual("白酒", updated["industry"])
        self.assertEqual(row["id"], updated["id"])

    def test_enrich_cn_instruments_baostock_is_idempotent(self) -> None:
        fake = _FakeBaoStock()
        summary1 = enrich_cn_instruments_from_baostock(
            db_path=self.db_path,
            bs_module=fake,
            symbols_path=self.symbols_path,
        )
        summary2 = enrich_cn_instruments_from_baostock(
            db_path=self.db_path,
            bs_module=fake,
            symbols_path=self.symbols_path,
        )
        self.assertEqual(2, summary1["inserted_instruments"])
        self.assertEqual(0, summary1["updated_instruments"])
        self.assertEqual(6, summary1["inserted_aliases"])
        self.assertEqual(0, summary1["failed"])
        self.assertEqual(2, summary2["updated_instruments"])
        self.assertEqual(0, summary2["inserted_instruments"])

        items = list_instruments(self.db_path, country="CN")
        self.assertEqual(2, len(items))
        maotai = get_instrument_by_symbol(self.db_path, "SH600519")
        self.assertEqual("贵州茅台", maotai["name_zh"])
        self.assertEqual("白酒", maotai["industry"])
        self.assertEqual("消费", maotai["sector"])
        aliases = list_instrument_aliases(self.db_path, "SH600519")
        alias_map = {row["alias_type"]: row["alias_value"] for row in aliases}
        self.assertEqual("SH600519", alias_map["qlib"])
        self.assertEqual("sh.600519", alias_map["baostock"])
        self.assertEqual("600519.SS", alias_map["yahoo"])

    def test_enrich_cn_instruments_baostock_dry_run(self) -> None:
        summary = enrich_cn_instruments_from_baostock(
            db_path=self.db_path,
            bs_module=_FakeBaoStock(),
            symbols_path=self.symbols_path,
            dry_run=True,
        )
        self.assertEqual(2, summary["inserted_instruments"])
        self.assertEqual(0, len(list_instruments(self.db_path)))


if __name__ == "__main__":
    unittest.main()
