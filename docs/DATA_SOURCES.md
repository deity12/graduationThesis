# 数据来源与限制

## 研究时间边界

- 数据准备起点：`2015-09-01`
- 正式评估区间：`2016-01-01` 到 `2023-12-31`
- `2015-09-01` 到 `2015-12-31` 仅用于 warm-up 初始化，不进入任何 `train/val/test` 样本

## 阶段1数据来源

### 1. 固定研究股票池

- 来源：Wikipedia `List of S&P 500 companies`
- 获取方式：带 `User-Agent` 的 `requests` 请求，再用 `pandas.read_html` 解析
- 产物：`data/raw/universe/sp500_constituents.csv`
- 元数据：`data/raw/universe/sp500_constituents.metadata.json`

限制：

- 当前采用固定 snapshot，不做历史成分股动态切换
- 抓取结果会在下载时冻结为项目使用的固定 universe 版本

### 2. 股票价格与市场指数

- 来源：Yahoo Finance，经 `yfinance` 下载
- 股票范围：固定 universe 中全部 ticker
- 市场指数：`^GSPC`
- 时间范围：`2015-09-01` 到 `2023-12-31`
- 关键字段：`adj_close`

产物：

- `data/raw/prices/stocks/*.csv`
- `data/raw/prices/stocks/stocks.metadata.json`
- `data/raw/prices/stocks/failed_tickers.json`
- `data/raw/prices/market/sp500_index.csv`
- `data/raw/prices/market/sp500_index.metadata.json`

限制：

- Yahoo Finance 是公开数据源，可能出现个别 ticker 缺失、停牌、代码变更或历史回填
- 脚本会记录失败列表并支持断点续跑，但不会自动修复数据源本身的问题

### 3. Alias Seed

- 来源：由固定 universe 中的 `company_name` 和 `ticker` 派生
- 产物：`data/raw/mapping/company_alias_seed.csv`
- 元数据：`data/raw/mapping/company_alias_seed.metadata.json`

限制：

- 当前 alias 仅为初版 seed，主要用于后续阶段的映射冷启动
- 不包含人工审核和高级别名扩展
