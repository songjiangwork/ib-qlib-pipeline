# CN A-Share Onboarding

这条线目前是“骨架已就位，等待真实日线数据”状态。

## 已准备好的部分

- `config_cn.yaml`
- 默认 universe: `cn_a_share`
- symbol 占位文件: `symbols/cn/cn_a_share.txt`
- 训练 runtime 已不再硬编码 `us_data_custom`
- BaoStock / CSI800 脚本：
  - `scripts/generate_csi800_symbols.py`
  - `scripts/download_cn_csi800_daily_baostock.py`
  - `scripts/export_cn_daily_to_qlib_csv.py`

## 你需要提供的内容

1. A 股 symbol 列表
   - 推荐格式：
     - `600519.SH`
     - `000001.SZ`
     - `300750.SZ`

2. 日线 CSV
   - 放到：
     - `data/cn/raw/baostock/daily/<SYMBOL>.csv`
   - 列格式需要与当前 raw price CSV 保持一致：
     - `date,symbol,open,high,low,close,volume,factor`

## 典型接入流程

1. 生成或手工提供 CSI300 / CSI500 / CSI800 symbols

```bash
python scripts/generate_csi800_symbols.py
```

2. 下载 BaoStock 原始日线

```bash
python scripts/download_cn_csi800_daily_baostock.py \
  --symbols symbols/cn/csi800_baostock_map.txt \
  --start-date 2016-01-01
```

3. 导出 Qlib CSV

```bash
python scripts/export_cn_daily_to_qlib_csv.py \
  --raw-dir data/cn/raw/baostock/daily \
  --out-dir data/cn/processed/qlib_csv \
  --start-date 2016-01-01 \
  --drop-suspended \
  --drop-zero-volume
```

4. 生成 qlib bin：

```bash
python /home/song/projects/qlib/scripts/dump_bin.py dump_all \
  --csv_path data/cn/processed/qlib_csv \
  --qlib_dir data/qlib/cn_csi800 \
  --freq day \
  --date_field_name date \
  --symbol_field_name symbol \
  --include_fields open,close,high,low,volume,factor
```

5. 再为 A 股新建 workflow / model family
6. 然后跑：
   - `backfill_rankings_bulk.py`
   - `simulate_portfolio.py`

## 目录约定

当前 A 股按 `data/<market>/...` 方案组织：

- `data/cn/raw/baostock/daily`
- `data/cn/processed/qlib_csv`
- `data/qlib/cn_csi800`

美股历史目录目前仍保留原状，后续如需统一到 `data/us/...`，再单独迁移。

## 当前限制

- `run.py` 仍然是按 IB / 美股 `Stock(..., "SMART", "USD")` 的抓取路径设计的
- A 股如果来自其他数据渠道，建议先走“外部 CSV -> materialize -> qlib”这条线
- 真正的 A 股数据采集脚本后面再单独加
