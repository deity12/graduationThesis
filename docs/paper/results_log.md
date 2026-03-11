# Results Log

## 阶段2.5c：双源新闻标准化与 Source Mapping

### 产物

- `data/interim/news_normalized.parquet`
- `data/processed/news_source_mapped.parquet`

### 核心统计

| 指标 | 数值 |
| --- | ---: |
| 双源合并后总条数 | 2,782,726 |
| mapped 条数 | 1,546,599 |
| 总体映射率 | 55.58% |
| `is_warmup=True` | 392,380 |
| `in_evaluation_window=True` | 2,390,276 |
| `ticker_raw` 非空比例 | 71.70% |
| `body` 非空比例 | 44.75% |
| 去重总数 | 4,928,390 |
| 2020-06-12 ~ 2023-12-31 缺口补齐条数 | 577,087 |

### `source_file` 贡献与映射结果

| source_file | 贡献条数 | mapped 条数 | mapped 比例 |
| --- | ---: | ---: | ---: |
| `All_external.csv` | 1,779,296 | 543,215 | 30.53% |
| `nasdaq_exteral_data.csv` | 1,003,430 | 1,003,384 | 99.995% |

### 年度分布（2015-2024）

| 年份 | 条数 |
| --- | ---: |
| 2015 | 392,380 |
| 2016 | 591,992 |
| 2017 | 305,948 |
| 2018 | 374,822 |
| 2019 | 398,117 |
| 2020 | 193,513 |
| 2021 | 123,702 |
| 2022 | 174,502 |
| 2023 | 227,680 |
| 2024 | 70 |

### 2020-2023 补齐说明

- `All_external.csv` 单独使用时，主语料最晚只到 `2020-06-11`。
- 双源合并后，`2020-06-12 ~ 2023-12-31` 新增覆盖 `577,087` 条。
- 这一步消除了阶段2进入阶段3前的时间覆盖阻塞项。

### 验证结果

- `pytest tests/smoke -q`：`11 passed`
- 双源输入可读、`news_source_mapped.parquet` 可生成、`in_evaluation_window` 标记保留、ticker 映射比例断言均通过。

### 阶段判断

- 阶段2.5c已完成，双源新闻标准化与 source mapping 已闭环。
- 当前主语料已足以支撑 `2016-01-01 ~ 2023-12-31` 的论文主线。
- 阶段2可以正式封板，可进入阶段3准备。

## 阶段3：特征、标签与 sample_index 底座

### 产物

- 主产物：`data/processed/feature_panel_v1.parquet`
- 主产物：`data/processed/forward_returns.parquet`
- 主产物：`data/processed/sample_index.parquet`
- 依赖补生成：`data/processed/prices_daily.parquet`

> 说明：`prices_daily.parquet` 是本轮为满足阶段3前置依赖补落盘的价格侧正式产物，不属于阶段3方法扩展。

### 核心统计

| 产物 | 指标 | 数值 |
| --- | --- | ---: |
| `feature_panel_v1.parquet` | 总行数 | 995,508 |
| `feature_panel_v1.parquet` | ticker 数 | 499（见下方研究宇宙口径） |
| `feature_panel_v1.parquet` | `feature_complete` 行数 | 985,389 |
| `feature_panel_v1.parquet` | warm-up 行数 | 39,858 |
| `forward_returns.parquet` | 总行数 | 995,007 |
| `sample_index.parquet` | 总行数 | 2,394,061 |

### 日期范围与标签口径

| 产物 | 日期范围 / 约束 | 结果 |
| --- | --- | --- |
| `feature_panel_v1.parquet` | 日期范围 | `2015-09-01 ~ 2023-12-29` |
| `forward_returns.parquet` | 日期范围 | `2015-09-01 ~ 2023-12-28` |
| `forward_returns.parquet` | 标签口径 | next-1-trading-day 对数收益 |
| `forward_returns.parquet` | `label_start_date > as_of_date` | 全量为真 |
| `sample_index.parquet` | 最早样本日 | `2016-01-04` |
| `sample_index.parquet` | warm-up 样本 | 无 |
| `sample_index.parquet` | `cutoff_date == as_of_date` | 全量为真 |

### split 分布

| Split | 样本量 |
| --- | ---: |
| S1 | 599,842 |
| S2 | 599,501 |
| S3 | 598,397 |
| S4 | 596,321 |

### 新闻元数据接入

| 指标 | 数值 |
| --- | ---: |
| `news_source_mapped.parquet` 聚合出的 `(date, ticker)` 唯一对 | 419,039 |
| `sample_index` 中带映射新闻的样本行数 | 866,838 |
| `mapped_news_count_1d` 总和 | 3,268,890 |

当前 sample-level metadata 只接入：

- `mapped_news_count_1d`
- `has_mapped_news`

未提前实现：

- LLM 抽取
- 图构建
- 训练
- baseline

### 测试与泄露防护验证

- `python -m pytest tests/smoke/test_stage3_features_labels_sample_index.py -q`
- `python -m pytest tests/leakage -q`
- `python -m pytest tests/smoke/test_stage3_features_labels_sample_index.py tests/leakage -q`
- 最终结果：`4 passed`

以下未来数据 / 越界注入测试已按预期触发失败校验，从而证明 `leakage_guard` 生效：

- `test_feature_frame_rejects_future_window_end`
- `test_forward_returns_reject_same_day_label_start`
- `test_sample_index_rejects_warmup_as_of_date`

### 阶段判断

- 阶段3已完成，统一 `leakage_guard`、特征表、标签表和无图版 `sample_index` 骨架均已落盘。
- 阶段2已封板，阶段3可闭环验收。
- 文档状态可同步更新为“可进入阶段4”，但阶段4内容尚未开始填写。

### 研究宇宙与数据口径（固定）

- **最终研究宇宙固定为 SP500；FNSPID 仅作为新闻来源而非股票宇宙定义来源。** 研究宇宙理想口径为 point-in-time SP500 成分；当前实现为静态近似，后续需审计 survivorship bias 及是否升级逐日成分。
- 论文口径：本文以 SP500 成分股构成研究股票宇宙；FNSPID 双源新闻语料作为外部新闻来源。新闻仅在能够映射到研究宇宙内股票时进入主线建模。价格、特征、标签、样本索引已在同一 SP500 宇宙内构建；后续滚动图构建也将严格基于该宇宙完成。
- 上游标准化新闻表可保留双源全量新闻用于审计与复现；主线建模仅使用映射到研究宇宙内股票的新闻记录。
- 当前 feature panel 为 499 只 ticker：**目标研究宇宙定义为 SP500**；有效可用宇宙 499 只。工程上 503−4=499（四只：GEV、Q、SNDK、SOLV）。**已核实**：非 Wikipedia 解析错误、非 symbol 清洗错误；根因为成分表未按 date_added 过滤，含 2024/2025 才加入或已退市行（GEV/SOLV 2024 年上市，Q 为 2025-11 计划加入，SNDK 2016 年收购退市、表中为 2025-11 行）。详见 `docs/project/universe_499_verification.md`。

## 阶段4：本地实现准备完成，待服务器正式验证

### 当前状态

- 已补齐 `vLLM client`、`schema compatibility precheck`、`structured decode`、`2016 抽样`、`runner`、`report` 与 `smoke tests`。
- 本地 `mock` 流程已跑通，正式 2016 样本已按固定 seed 完成抽样。
- 当前不得宣称阶段4正式完成；真实 `1 条 smoke + 100 条 formal` 仍待服务器执行。

### 本地产物

- `configs/llm/stage4_feasibility_2016.yaml`
- `outputs/stage4/stage4_official_sample_2016.parquet`
- `outputs/stage4/stage4_local_mock_results.parquet`
- `outputs/stage4/stage4_feasibility_report.json`
- `outputs/stage4/stage4_feasibility_report.md`

### 本地检查结果

| 指标 | 数值 |
| --- | ---: |
| warm-up 候选新闻数 | 106,525 |
| 2016 正式候选新闻数 | 197,164 |
| 正式样本抽样数 | 100 |
| 本地 mock 实际处理数 | 5 |

### 口径与边界

- 阶段4正式样本池来自 `data/processed/news_source_mapped.parquet`，且仅保留**已映射到 SP500 研究宇宙内股票**的新闻记录。
- `2015-09-01 ~ 2015-12-31` 仅作为 warm-up 可读区间，不进入正式 100 条样本分母。
- “100 条已抽样”不等于“100 条已完成真实抽取”；当前仅对其中 5 条进行了本地 mock 流程验证。

### 阶段判断

- **可以进入服务器上的阶段4正式验证准备/执行。**
- **不可以宣称阶段4正式完成。**
- **不可以进入阶段5。**
