# CN A-Share Onboarding

这条线目前是“骨架已就位，等待真实日线数据”状态。

## 已准备好的部分

- `config_cn.yaml`
- 默认 universe: `cn_a_share`
- symbol 占位文件: `symbols/cn_a_share.txt`
- 训练 runtime 已不再硬编码 `us_data_custom`

## 你需要提供的内容

1. A 股 symbol 列表
   - 推荐格式：
     - `600519.SH`
     - `000001.SZ`
     - `300750.SZ`

2. 日线 CSV
   - 放到：
     - `data_cn/raw/prices/<SYMBOL>.csv`
   - 列格式需要与当前 raw price CSV 保持一致：
     - `date,symbol,open,high,low,close,volume,factor`

## 典型接入流程

1. 填写 `symbols/cn_a_share.txt`
2. 将外部 A 股日线整理成 raw CSV，写入 `data_cn/raw/prices`
3. 物化 qlib CSV / qlib bin：

```bash
python -m ib_qlib_pipeline.materialize_universe_data \
  --config config_cn.yaml \
  --dump-bin
```

4. 再为 A 股新建 workflow / model family
5. 然后跑：
   - `backfill_rankings_bulk.py`
   - `simulate_portfolio.py`

## 当前限制

- `run.py` 仍然是按 IB / 美股 `Stock(..., "SMART", "USD")` 的抓取路径设计的
- A 股如果来自其他数据渠道，建议先走“外部 CSV -> materialize -> qlib”这条线
- 真正的 A 股数据采集脚本后面再单独加
