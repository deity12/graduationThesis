# Figure Plan

## 阶段2.5c：双源新闻标准化与 Source Mapping

### 图 2.5c-1 年度覆盖分布图

- 目标：展示双源合并后的新闻年度覆盖是否连续，并确认主语料已延伸到 `2023-12-31` 之后。
- 数据来源：`data/interim/news_normalized.parquet`
- 建议图型：年度柱状图，横轴为 `2015-2024`，纵轴为新闻条数。
- 备注：可由 Codex 或后续绘图脚本生成。
- 状态：`[待实验数据生成]`

### 图 2.5c-2 2020-2023 补齐图

- 目标：突出 `2020-06-12 ~ 2023-12-31` 的新增覆盖，说明双源合并解决了单源尾部缺口。
- 数据来源：`data/interim/news_normalized.parquet`
- 建议图型：区间累计柱状图或分年补齐条数图。
- 备注：重点标注补齐总量 `577,087`。
- 状态：`[待实验数据生成]`

### 图 2.5c-3 双源 vs 单源映射对比图

- 目标：对比阶段2.5a单源映射率与阶段2.5c双源映射率，展示 source mapping 完整度提升。
- 数据来源：`data/processed/news_source_mapped.parquet` 与阶段2.5a历史统计。
- 建议图型：分组柱状图。
- 备注：建议同时标注总体映射率、`All_external.csv` 映射率、`nasdaq_exteral_data.csv` 映射率。
- 状态：`[待实验数据生成]`

### 图 2.5c-4 ticker / body 覆盖比例图

- 目标：展示双源主语料中 `ticker_raw` 和 `body` 的可用性，为阶段3特征构造做准备。
- 数据来源：`data/interim/news_normalized.parquet`
- 建议图型：双指标条形图或 100% 堆叠图。
- 备注：当前统计值分别为 `ticker_raw=71.70%`、`body=44.75%`。
- 状态：`[待实验数据生成]`

## 阶段3：特征、标签与 sample_index 底座

### 图 A feature panel 日期覆盖与完整率图

- 目标：展示 `feature_panel_v1.parquet` 在 `2015-09-01 ~ 2023-12-29` 区间的日期覆盖，以及 `feature_complete` 随时间的可用率。
- 数据来源：`data/processed/feature_panel_v1.parquet`
- 建议图型：日期覆盖折线 + 完整率副轴图，或双面板时间序列图。
- 备注：重点区分 warm-up 段与正式评估段，但不要提前写入图上结论。
- 状态：`[待实验数据生成]`

### 图 B sample_index 四个 split 样本量分布图

- 目标：展示 `sample_index.parquet` 中 `S1 ~ S4` 的样本量分布，作为 walk-forward 切分的样本规模审计图。
- 数据来源：`data/processed/sample_index.parquet`
- 建议图型：split 级柱状图，或 split 内 train / val / test 分组柱状图。
- 备注：当前只做图表占位，不提前写 split 优劣解释。
- 状态：`[待实验数据生成]`

### 图 C has_mapped_news 样本占比图

- 目标：展示 `sample_index` 中 `has_mapped_news=True` 的样本占比，说明新闻映射元数据已以 sample-level metadata 接入。
- 数据来源：`data/processed/sample_index.parquet`
- 建议图型：总体占比条形图或按 split 分组的堆叠柱状图。
- 备注：只展示占比与口径，不延伸到图构建效果解释。
- 状态：`[待实验数据生成]`

### 图 D mapped_news_count_1d 分布图

- 目标：展示 `mapped_news_count_1d` 的样本分布，作为后续新闻密度与图窗口设计的描述性输入。
- 数据来源：`data/processed/sample_index.parquet`
- 建议图型：直方图、对数坐标直方图或箱线图。
- 备注：保留分布图占位，不虚构偏度、长尾或集中度结论。
- 状态：`[待实验数据生成]`
