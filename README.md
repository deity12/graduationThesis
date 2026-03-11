# Graduation Thesis Repo

本仓库当前已完成“阶段0：repo骨架”和“阶段1：外部数据准备”的最小工程实现。当前范围覆盖工程结构、路径配置、研究边界、walk-forward 评估协议，以及原始 universe/price/mapping 数据准备流程；仍不实现 LLM 抽取、图构建、训练或实验。

## 固定研究边界

- 固定实验评估区间：`2016-01-01` 到 `2023-12-31`
- 固定 warm-up 起点：`2015-09-01`
- 明确 warm-up 数据只用于窗口初始化，不进入任何 `train/val/test` 样本
- 固定 4 个 walk-forward split：
  - `S1`: 2016-2018 train, 2019 val, 2020 test
  - `S2`: 2017-2019 train, 2020 val, 2021 test
  - `S3`: 2018-2020 train, 2021 val, 2022 test
  - `S4`: 2019-2021 train, 2022 val, 2023 test

## 目录骨架

```text
configs/
  data/
  experiment/
  llm/
  model/
data/
  raw/
  interim/
  processed/
docs/
FNSPID/
logs/
outputs/
scripts/
src/
  analysis/
  common/
  data/
  llm/
  model/
tests/
  smoke/
```

## 关键配置入口

- `configs/paths.yaml`: 项目路径总入口
- `configs/data/walk_forward_2016_2023.yaml`: 固定评估与 warm-up 协议
- `configs/data/universe.yaml`: 阶段1数据源、固定 universe 与下载参数
- `configs/llm/runtime_qwen25_32b_awq.yaml`: 阶段4预留的 LLM runtime 占位配置
- `configs/llm/spillover_schema_v2_vllm_compatible.json`: vLLM-compatible schema 占位版
- `configs/model/temporal_gru.yaml`: 时序模型占位配置
- `configs/experiment/*.yaml`: 后续阶段实验入口占位配置

所有路径优先从 YAML 读取，不在脚本中硬编码项目路径。

## 阶段1数据源

- `SP500 fixed universe`: 使用 Wikipedia 的 S&P 500 constituents 页面抓取，并冻结为本项目的固定研究股票池
- `股票价格与市场指数`: 使用 `yfinance` 从 Yahoo Finance 获取 `2015-09-01` 到 `2023-12-31` 的日频 OHLCV 与 `adj_close`
- `alias seed`: 基于固定 universe 的 `company_name` 和 `ticker` 自动生成初版别名字典

对应脚本：

- `python -m src.data.download_universe`
- `python -m src.data.download_prices`
- `python -m src.mapping.alias_table`

## 阶段1限制

- 当前采用“固定研究股票池”模式，不处理历史成分股动态切换
- 市场指数优先使用 `^GSPC`
- Yahoo Finance 为公开数据源，可能存在回填、停牌或个别 ticker 缺失问题；脚本会记录失败列表，但不会中断整批下载
- alias 仅为初版 seed，后续阶段仍需补充人工校验和扩展

## 现有数据说明

- `FNSPID/` 已存在，视为原始外部输入的一部分
- 阶段1不会重新下载或改写该目录内容
- 后续阶段新增数据产物统一落到 `data/` 下

## 最小环境

```bash
python -m pip install -r requirements.txt
```

## Smoke Test

```bash
python -m pytest tests/smoke -q
```

Smoke test 会验证：

- 关键配置文件可被读取
- 目录结构完整
- walk-forward 配置可解析且满足固定时间边界
- 抽样股票价格数据可读取
- 市场指数数据存在
- universe 文件字段完整
- alias seed 文件生成成功

## 后续阶段入口

- 阶段1：外部数据准备，使用 `configs/paths.yaml`、`configs/data/walk_forward_2016_2023.yaml` 与 `configs/data/universe.yaml`
- 阶段4：LLM 可行性验证，使用 `configs/llm/` 下占位配置
- 阶段7 及以后：训练与实验，使用 `configs/model/` 和 `configs/experiment/` 下占位入口
