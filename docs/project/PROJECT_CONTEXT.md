可以，这样做是对的。
下面我直接给你一份**可作为项目统一背景与执行基线的完整文档**，你可以把这整条消息保存进项目里，例如放成：

* `docs/project/PROJECT_CONTEXT.md`
  或
* `docs/project/PROJECT_STATE.md`

从现在开始，后续规则、阶段状态、数据口径、论文叙事的修改，都以这份文档为准继续维护。

---

````markdown
# PROJECT_CONTEXT.md

## 0. 文档定位

这份文档是本项目当前的**统一背景说明、工程执行基线、数据口径说明、阶段状态记录与后续协作约束**。

用途：

1. 作为后续与 Codex / Claude / GPT 协作时的统一上下文
2. 作为项目内长期维护的“单一事实来源”
3. 用来覆盖和替代此前已经被证伪或已过时的旧设定
4. 后续若项目设定发生变化，应优先修改本文件，而不是让旧提示词继续漂移

**重要说明：**
本文件中关于新闻主语料的口径，已经从早期版本的“仅使用 `All_external.csv`”修正为“双源统一标准化去重合并”。凡与本文件冲突的旧说明，一律以本文件为准。

---

## 1. 项目目标

本项目面向**股票横截面排序（cross-sectional stock ranking）**任务，目标是完成一篇可投稿、可复现、满足硕士论文要求的金融机器学习论文。

论文主线方法为：

- 使用 **LLM 从金融新闻中抽取事件溢出关系**
- 构建 **滚动语义图（rolling semantic graph）**
- 同时构建 **滚动价格相关图（rolling price correlation graph）**
- 采用 **时序 backbone + 双图编码 + adaptive fusion + RankIC loss** 进行排序预测

当前论文主叙事不是“全面大幅超过所有强 baseline”，而是：

> rolling event graph 在远期测试区间比 static graph 更稳定，也比不显式建模滚动事件关系的强 baseline 更稳健。

---

## 2. 论文主线的固定方法设定

以下内容属于**不可随意改动的主线设定**，后续工程实现必须严格遵守：

### 2.1 语义边定义

1. 语义边不是文本相似度边
2. 语义边定义为 **LLM 抽取的事件溢出有向边**
3. 抽取形式是：
   - `source`
   - `targets`
   - `event_type`

### 2.2 LLM 的职责边界

LLM **只做结构抽取**，不做收益预测，不做情感打分，不做边权学习，不输出自评分作为主线信号。

明确禁止将以下字段纳入主线抽取输出：

- `polarity`
- `strength`
- `confidence`

### 2.3 图构建原则

1. 语义图必须是 **rolling**
2. 价格图必须是 **rolling correlation graph**
3. 边权优先采用：
   - binary edge
   - time-decayed count
4. 不允许将 LLM 自评置信度作为边权

### 2.4 Backbone 与训练目标

1. 主线 temporal backbone 固定为 **2-layer GRU**
2. 排序训练目标统一采用 **RankIC loss**

---

## 3. 实验协议与时间边界

### 3.1 总评估区间

固定为：

- `2016-01-01 ~ 2023-12-31`

### 3.2 warm-up 起点

固定为：

- `2015-09-01`

### 3.3 warm-up 的用途

warm-up 数据只用于初始化以下滚动窗口：

- temporal lookback（60 交易日）
- price rolling graph（60 交易日）
- semantic rolling graph（63 日）

### 3.4 warm-up 约束

warm-up 数据：

- 可以进入中间标准化主表
- 可以用于图/窗口初始化
- **绝不能进入任何 train / val / test 样本**

### 3.5 walk-forward 协议

固定采用 4 个完整 split：

| Split | Train                   | Val                     | Test                    |
|------|-------------------------|-------------------------|-------------------------|
| S1   | 2016-01-01 ~ 2018-12-31 | 2019-01-01 ~ 2019-12-31 | 2020-01-01 ~ 2020-12-31 |
| S2   | 2017-01-01 ~ 2019-12-31 | 2020-01-01 ~ 2020-12-31 | 2021-01-01 ~ 2021-12-31 |
| S3   | 2018-01-01 ~ 2020-12-31 | 2021-01-01 ~ 2021-12-31 | 2022-01-01 ~ 2022-12-31 |
| S4   | 2019-01-01 ~ 2021-12-31 | 2022-01-01 ~ 2022-12-31 | 2023-01-01 ~ 2023-12-31 |

---

## 4. 防时间泄露原则（必须执行）

这是本项目最重要的工程约束之一。

### 4.1 强制要求

所有以下类型的函数，都必须显式接受 `cutoff_date` 参数：

- 图构建函数
- 样本构建函数
- 特征切片函数
- 标签生成函数
- 任何依赖历史窗口的聚合函数

### 4.2 断言要求

所有依赖时间的数据处理逻辑，必须有显式断言，确保：

- 不读取 `cutoff_date` 之后的数据
- 不把 warm-up 数据混入 train / val / test 样本
- 不出现未来函数、窗口越界或标签泄露

### 4.3 若发现冲突

如果已有实现与上述原则冲突，必须优先修正，并在输出中明确说明：

- 原来哪里有风险
- 现在怎么修正
- 是否已测试验证

---

## 5. LLM 抽取方案（当前定稿）

### 5.1 抽取模型

- 模型：`Qwen/Qwen2.5-32B-Instruct-AWQ`
- 推理引擎：`vLLM`

### 5.2 schema 兼容性策略

由于 vLLM 对 `nullable / anyOf / oneOf` 的兼容性有限，当前抽取 schema 方案固定为：

- 所有字段统一使用 `string`
- 空值统一写为 `""`
- **禁止 JSON null**
- 在 `structured_decode.py` 中再执行：
  - `"" -> None`

### 5.3 顶层 schema 字段

顶层只允许：

- `source`
- `targets`
- `event_type`

不允许额外恢复情感、自评强度或置信度字段作为主线输出。

---

## 5.5 研究宇宙与数据口径（固定）

**最终研究宇宙固定为 SP500；FNSPID 仅作为新闻来源而非股票宇宙定义来源。**

研究宇宙在理想口径上为 **point-in-time SP500 成分变动**；当前实现采用**静态近似**（固定时点成分名单），后续需审计是否引入 survivorship bias 及是否升级为逐日成分。

工程与论文口径统一表述：

> **本文以 SP500 成分股构成研究股票宇宙；FNSPID 双源新闻语料作为外部新闻来源。新闻仅在能够映射到研究宇宙内股票时进入主线建模。价格、特征、标签、样本索引已在同一 SP500 宇宙内构建；后续滚动图构建也将严格基于该宇宙完成。**

据此：

- **目标研究宇宙** = SP500；**有效可用宇宙** = 当前 499 只（静态成分表 503 只，4 只无有效价格：GEV、Q、SNDK、SOLV）。已核实：非 Wikipedia 解析/非 symbol 清洗错误；根因为成分表未按 date_added 过滤，含 2024/2025 才加入或已退市行。详见 `docs/project/universe_499_verification.md`；建议下游按 `date_added <= evaluation_end` 过滤。
- **边界说明**：上述过滤可修复当前已识别的未来成分股误入，但**尚不能完全替代真正的 point-in-time S&P 500 membership 宇宙**（Wikipedia 为现行快照，非逐日历史成分库）。论文中应将 universe definition / survivorship-bias 风险明确列为限制项。
- 上游：`news_normalized.parquet` 可保留双源全量标准化新闻，便于审计与复现。
- 主线：自 `news_source_mapped.parquet` 起，仅 `mapped == True` 且对应 ticker 属于 SP500 研究宇宙的新闻进入下游；价格、特征、标签、sample_index 与滚动图均仅针对 SP500 宇宙（当前为静态近似）。
- 避免将 FNSPID 全部 mapped 股票误作研究宇宙，防止价格侧、新闻侧、图侧 universe 不一致。

---

## 6. FNSPID 原始数据结构与正式数据口径

### 6.1 FNSPID 原始位置

原始数据目录：

```text
data/raw/fnspid/
````

当前已确认目录下存在：

1. `All_external.csv`
2. `nasdaq_exteral_data.csv`
3. `full_history/`

### 6.2 三者角色

* `All_external.csv`：新闻
* `nasdaq_exteral_data.csv`：新闻
* `full_history/`：价格历史，不是新闻主源

---

## 7. 新闻主语料口径的正式修正

### 7.1 已被证伪的旧口径

此前曾暂定：

* 正式新闻主语料只使用 `All_external.csv`
* `nasdaq_exteral_data.csv` 只做辅助审计，不并入主表

这个设定**现已废止**。

### 7.2 修正原因

后续已核查确认：

* `All_external.csv` 最晚只到：`2020-06-11`
* `nasdaq_exteral_data.csv` 最晚到：`2024-01-09`
* 若只使用 `All_external.csv`，则无法支撑既定实验协议：

  * `2016-01-01 ~ 2023-12-31`

进一步核查确认：

* 两表结构兼容
* 两表在 `2020-06-11` 之前高度重叠
* `nasdaq_exteral_data.csv` 可以补齐：

  * `2020-06-12 ~ 2023-12-31`
* `nasdaq_exteral_data.csv` 还表现出更长覆盖与更丰富正文

### 7.3 当前正式口径（以此为准）

**正式新闻主语料 = `All_external.csv` + `nasdaq_exteral_data.csv`**

处理方式为：

1. 两表统一标准化
2. 统一字段映射
3. 统一日期解析
4. 统一 warm-up / evaluation 标记
5. 按统一去重规则去重合并
6. 输出为正式主新闻 parquet

### 7.4 正式主产物路径

正式主新闻产物路径为：

```text
data/interim/news_normalized.parquet
```

同时可保留旧单源版本作为备份，例如：

```text
data/interim/news_normalized_v1_all_only.parquet
```

---

## 8. 新闻标准化与去重规则（当前正式版本）

### 8.1 字段映射

标准字段至少包括：

* `Date -> published_at`
* `Article_title -> title`
* `Stock_symbol -> ticker_raw`
* `Article -> body`

并保留必要原始信息字段，例如：

* `url`
* `publisher_raw`
* `author_raw`
* `summary_lsa`
* `summary_luhn`
* `summary_textrank`
* `summary_lexrank`
* `source_file`
* `source_row_number`

### 8.2 时间字段要求

* `published_at` 必须统一解析为可排序、可过滤的标准时间字段
* 当前采用 UTC 时间戳解析与存储

### 8.3 标记字段

标准化表中必须至少包含：

* `is_warmup`
* `in_evaluation_window`

定义如下：

* `is_warmup = True` 对应 `2015-09-01 ~ 2015-12-31`
* `in_evaluation_window = True` 对应 `2016-01-01 ~ 2023-12-31`

### 8.4 去重主键

统一去重规则：

1. `url` 优先
2. 若 `url` 缺失，则使用后备键：

   * `published_date + ticker_raw + title前50字`

### 8.5 去重保留优先级（必须写死）

当检测到重复新闻时，保留规则固定为：

1. `body` 非空优先于 `body` 为空
2. 若两条都有 `body` 或都无 `body`，优先保留 `All_external.csv` 来源
3. 若仍完全相同，则按稳定排序保留第一条，确保结果可复现

这个优先级必须显式实现，不能让模型自由猜测“更完整”意味着什么。

---

## 9. 数据诊断中已确认的关键事实

### 9.1 单表 `All_external.csv` 的事实

已确认：

* 原始 `All_external.csv` 最晚日期：`2020-06-11 13:12:35+00:00`
* 因此单独使用它时，无法覆盖：

  * `2020-06-12 ~ 2023-12-31`

### 9.2 `nasdaq_exteral_data.csv` 的事实

已确认：

* 最晚日期：`2024-01-09`
* 足以覆盖缺口区间：

  * `2020-06-12 ~ 2023-12-31`

### 9.3 两表关系

已确认：

* 两表在 `2020-06-11` 之前高度重叠
* 字段结构兼容
* 当前 `normalize_news` 逻辑可复用
* 适合采用双源统一标准化 + 统一去重合并

### 9.4 文本稀疏问题

还需持续注意：

* 新闻正文 `body` 非空比例不高
* 大量新闻可能是标题-only 样本

这意味着在阶段4可行性验证时，必须专门检查：

* 仅标题输入时的抽取质量
* JSON 解析成功率
* valid target ratio
* 月均 out-degree
* 标题-only 是否足以支撑主线语义图抽取

---

## 10. 阶段定义（固定，不要混用）

阶段编号固定如下：

* **阶段0：repo骨架**
* **阶段1：外部数据准备**
* **阶段2：数据标准化与基础映射**
* **阶段3：特征、标签与 sample_index 底座**
* **阶段4：可行性验证**
* **阶段5：全量抽取流水线**
* **阶段6：语义图与价格图**
* **阶段7：训练框架**
* **阶段8：Temporal-only 与 MASTER**
* **阶段9：A2~A6 主实验矩阵**
* **阶段10：RSR、经典 baseline 与 backbone robustness**
* **阶段11：论文图表与结果汇总**

任何协作与文档都不得混用或重编号。

---

## 11. 当前阶段状态

### 11.1 已完成内容

#### 阶段0：repo骨架

已完成。

#### 阶段1：外部数据准备

已完成。

#### 阶段2：数据标准化与基础映射

已完成 smoke 级验证，并已核查清楚 FNSPID 真实结构。

已确认的实现包括：

* `configs/paths.yaml` 修正
* `configs/data/stage2_normalization.yaml`
* `src/common/dates.py`
* `src/common/io.py`
* `src/data/ingest_fnspid.py`
* `src/data/normalize_news.py`
* `src/data/normalize_prices.py`
* `src/data/build_universe.py`
* `src/mapping/source_mapper.py`
* `tests/smoke/test_stage2_normalize_and_mapping.py`

### 11.2 阶段2当前准确认定

阶段2的逻辑框架和 smoke 版代码已可用，但当前仍处于：

> **阶段2.5：正式全量执行与数据口径闭环阶段**

更细化的子状态可理解为：

* **阶段2.5a**：`All_external.csv` 单表正式全量标准化落盘
  已完成

* **阶段2.5b**：`nasdaq_exteral_data.csv` 覆盖核查与合并可行性评估
  已完成

* **阶段2.5c**：双源统一标准化、去重合并、正式主语料落盘
  **当前应执行的下一步**

---

## 12. 当前最合理的下一步

### 12.1 现在不要做的事

当前阶段不应直接进入：

* 阶段3
* 阶段4
* 训练
* sample_index
* 语义图建模
* 论文结果分析

因为新闻主语料的正式双源合并还未完成。

### 12.2 当前真正该做的事

应立即执行：

> **阶段2.5c：双源统一标准化、去重合并、正式主语料落盘**

目标：

1. 输入双源：

   * `All_external.csv`
   * `nasdaq_exteral_data.csv`

2. 复用当前标准化逻辑做最小必要修改

3. 输出正式主表：

   * `data/interim/news_normalized.parquet`

4. 保留单源旧版本备份：

   * `data/interim/news_normalized_v1_all_only.parquet`

5. 再次验证：

   * 最晚日期覆盖到 `2023-12-31` 之后
   * `2020~2023` 年度分布合理
   * `is_warmup / in_evaluation_window` 标记正确
   * 去重结果可解释、可复现

### 12.3 只有满足以下条件，才可以进入阶段3

必须确认：

1. 正式主新闻 parquet 已生成
2. 时间覆盖足以支撑 `2016-01-01 ~ 2023-12-31`
3. 双源去重合并规则已固定
4. smoke test 通过
5. 输出统计已检查无明显异常

在此之前，不建议进入阶段3。

---

## 13. 阶段4可行性验证（当前定稿）

阶段4不是 repo 初始化，而是：

> **可行性验证**

### 13.1 阶段4内容

1. 先做 1 条测试新闻的 vLLM guided JSON compatibility smoke test
2. 再从 **2016 年新闻**中随机抽 100 条
3. 做 source mapping
4. 做 Qwen 抽取
5. 做 target resolution
6. 输出报告

### 13.2 阶段4正式样本范围

* `2016-01-01 ~ 2016-12-31`

### 13.3 允许读取的 warm-up 新闻

* `2015-09-01 ~ 2015-12-31`

注意：

* 只用于初始化
* 不进入 100 条样本分母

### 13.4 阶段4验收标准

1. schema compatibility 测试通过
2. 不出现 `anyOf / oneOf` 错误
3. JSON 解析成功率 ≥ 95%
4. source 映射率 ≥ 80%
5. valid target ratio ≥ 25%
6. monthly avg out-degree ≥ 1.0
7. 必须给出“2016 年新闻密度是否足够”的结论

### 13.5 阶段4失败后的三级降级路径

1. **2016 足够**

   * 继续 `2016–2023`、4 split 主线

2. **2016 不足，但扩窗后足够**

   * `semantic_window_days: 63 -> 126`
   * 或从月度更新降为季度更新
   * 若通过，仍维持 `2016–2023`、4 split

3. **2016 扩窗后仍不足，但 2017 足够**

   * 降级到 `2017–2023`、3 split 版本继续推进

只有这三级都失败，才暂停项目主线。

---

## 14. baseline 体系（当前定稿）

### 14.1 内部变体（全部必做）

* A1 Temporal-only
* A2 Temporal + Static SemGraph
* A3 Temporal + Rolling SemGraph
* A4 Temporal + PriceGraph
* A5 Temporal + PriceGraph + Rolling SemGraph（simple fusion）
* A6 Full Model（adaptive fusion）

### 14.2 公开 baseline

* **MASTER：必做**
* **RSR：建议做**
* ALSTM / SFM：可后补

### 14.3 当前阶段关系

* 阶段8做 A1 与 MASTER
* 阶段9做 A2~A6
* A1 结果可在阶段9中复用

---

## 15. MASTER 复现方案（当前定稿）

### 15.1 唯一主线

采用：

> **架构复用 + 数据替换**

### 15.2 具体要求

* 复用 MASTER 架构代码
* 修改输入层维度以适配当前 feature panel
* 接入本项目的：

  * `sample_index`
  * walk-forward
  * validation-only selection
* 训练统一使用 **RankIC loss**

### 15.3 market information 处理

主线先尝试：

* 在 feature panel 中加入：

  * `feat_mkt_ret_1d = SP500 当日收益`

若工程复杂度过高，则可：

* 去掉独立 market branch

但必须在论文中明确说明与原论文实现的差异。

### 15.4 论文中必须写明的一句

> 为确保比较公平性，MASTER 在与本文相同的数据协议、特征集和评估流程下进行复现，与原论文实现存在以下差异：[列出差异]。

---

## 16. 工程实现通用规则（长期有效）

### 16.1 总体原则

1. 先阅读当前 repo 中已有的相关代码、配置、脚本、测试与文档；若 repo 近乎空白，则先检查目录结构与已有数据文件，再开始修改
2. 只做当前阶段要求的内容，不要提前实现后续阶段
3. 优先复用已有实现，遵循“最小必要修改”原则
4. 与现有目录结构、配置风格、日志风格保持一致
5. 不要为了“更优雅”而重写已经可用的模块

### 16.2 数据与配置原则

1. FNSPID 原始数据已经存在，不要重新下载
2. 不要覆盖 raw 数据，不要把处理后的数据写回 raw 目录
3. 如果新增配置，优先写到 `configs/`
4. 不要把关键日期、阈值、路径、窗口长度硬编码在业务逻辑中
5. 处理大文件时优先采用分块读取

### 16.3 测试与验证原则

1. 修改后必须运行本阶段最小 smoke test
2. 若当前缺测试，应补一个最小 smoke test
3. 不要只说“理论可跑”，必须区分：

   * 已实际跑通
   * 仅完成代码但未验证

### 16.4 输出要求

每次任务完成后，输出必须明确说明：

* 改了什么
* 为什么这么改
* 修改了哪些文件
* 如何运行
* 是否跑通
* 产物路径
* 关键统计/结果
* 剩余风险
* 是否可以进入下一阶段

---

## 17. 论文草案需要同步修正的要点

后续在 `paper_draft_v1.md` 或其他论文文档中，必须将此前的旧表述：

> 正式新闻主语料只使用 `All_external.csv`

修正为：

> 新闻数据来自 FNSPID 语料中的两个原始新闻文件：`All_external.csv` 与 `nasdaq_exteral_data.csv`。两者字段结构兼容，但时间覆盖不同：经核查，`All_external.csv` 仅覆盖至 2020-06-11，而 `nasdaq_exteral_data.csv` 延伸至 2024-01-09。为满足本文 2016-01-01 至 2023-12-31 的实验协议，本文对两份原始新闻表进行统一标准化，并基于 URL 优先、后备键补充的规则去重合并，最终形成正式新闻主表。

简化版本也可以写成：

> 正式新闻主语料由 `All_external.csv` 与 `nasdaq_exteral_data.csv` 统一标准化后去重合并得到。

---

## 18. 当前项目的简短状态结论

截至目前：

* 阶段0、阶段1已完成
* 阶段2 smoke 版实现已完成
* 单表 `All_external.csv` 标准化已完成
* 已确认 `All_external.csv` 无法单独支撑 2016–2023
* 已确认 `nasdaq_exteral_data.csv` 可补齐缺口
* 当前必须执行的是：

  * **阶段2.5c：双源统一标准化、去重合并、正式主语料落盘**
* 在这一步完成前：

  * **不要进入阶段3**

---

## 19. 后续维护规则

从现在起，任何以下类型的变更，都应优先更新本文件：

* 数据口径变化
* 阶段状态变化
* 主产物路径变化
* baseline 体系变化
* 论文主叙事变化
* 失败后的降级策略变化
* 与旧提示词冲突的修正规则

如果后续新开聊天窗口，应优先将本文件内容作为项目背景粘贴给协作模型。

---

## 20. 当前一句话行动指南

> 先完成阶段2.5c 的双源正式主语料合并，再决定是否进入阶段3；在此之前，所有后续方法、图构建、sample_index、训练与论文结果分析都不应提前启动。

```

---

你就把这份存进项目里，后面持续改这一个源文件即可。  
另外，项目里所有旧的“只用 `All_external.csv`”描述，建议尽快同步替换。
```
