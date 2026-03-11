# Stage 2: 数据标准化与基础映射

你现在负责“阶段2：数据标准化与基础映射”。

先阅读当前 repo、配置、原始数据目录和已生成脚本，再开始修改。
只做当前阶段，不要实现 LLM 抽取、图构建、训练或实验。

## 已知条件

- FNSPID 原始数据已经存在；
- 外部价格、市场指数、股票池与 alias seed 已在上一阶段准备；
- 数据准备起点是 2015-09-01；
- 评估区间是 2016-01-01 到 2023-12-31。

## 本阶段目标

1. 标准化 FNSPID 新闻表；
2. 标准化价格表；
3. 生成 universe_daily 或等价研究股票池表；
4. 建立最小 source mapping 能力；
5. 为后续阶段输出统一 parquet 产物；
6. 明确 warm-up 数据与评估样本区间的边界。

## 必须创建或补全

- src/data/ingest_fnspid.py
- src/data/normalize_news.py
- src/data/normalize_prices.py
- src/data/build_universe.py
- src/mapping/source_mapper.py
- src/common/io.py
- src/common/dates.py
- data/interim/news_normalized.parquet
- data/processed/prices_daily.parquet
- data/processed/universe_daily.parquet
- data/processed/news_source_mapped.parquet（允许先是初版）

## 关键要求

- 新闻去重要实现；
- 时间戳与日期要统一；
- source mapping 优先用 ticker_raw，再用 alias/company_name；
- 所有产物都保留 version 字段或文件级版本说明；
- warm-up 期间数据允许出现在 normalized/interim/processed 层，但样本层后面必须排除；
- 现在先不要做 target mapping，不要做 LLM 调用。

## 修改后运行 smoke test

- 新闻 parquet 可读取；
- 价格 parquet 可读取；
- universe parquet 可读取；
- 抽样验证 source mapping 对少量样本有效；
- 验证 warm-up 数据存在，但未被误标为评估样本。

### 请按以下固定格式输出结果：

1. 本次任务目标
2. 阅读了哪些现有文件/模块
3. 改了什么
4. 为什么这么改
5. 修改了哪些文件
6. 如何运行
7. 运行了哪些测试
8. 是否跑通
9. 生成了哪些产物
10. 关键统计或关键检查结果
11. 剩余风险
12. 明确说明是否可以进入下一阶段


你现在负责“阶段2.5a：All_external.csv 正式全量标准化落盘”。

这是一个已经做过 smoke 验证的项目，请优先复用现有阶段2代码与配置，尽量最小修改，不要重写整套流程，不要进入阶段3及之后内容。

【已确认的数据口径，不需要再核查】
- 正式新闻主源：只使用 data/raw/fnspid/All_external.csv
- nasdaq_exteral_data.csv：保留但不并入主语料，本阶段不处理
- full_history/：只作价格 fallback，不作新闻源

【本阶段唯一目标】
把 All_external.csv 正式全量标准化并落盘到：
- data/interim/news_normalized.parquet

【处理要求】
1. 使用 pandas 分块读取，适配大文件
2. 基于现有阶段2实现做最小修改，保持项目现有目录结构、配置风格和函数风格一致
3. 字段映射至少包含：
   - Date -> published_at
   - Article_title -> title
   - Stock_symbol -> ticker_raw
   - Article -> body
   - 其余原始字段尽量保留
4. published_at 必须被解析为可排序、可过滤的标准时间字段
5. 去重规则：
   - url 优先
   - 若 url 缺失，则使用 published_date + ticker_raw + title前50字 作为后备去重键
6. 增加明确标记字段：
   - is_warmup：2015-09-01 ~ 2015-12-31
   - in_evaluation_window：2016-01-01 ~ 2023-12-31
7. warm-up 数据可以进入 parquet，但必须明确标记，且后续不能进入任何 train/val/test 样本
8. 不要处理 nasdaq_exteral_data.csv
9. 不要进入阶段3，不要做 sample_index，不要做训练相关内容

【产出后请给出以下结果】
1. 修改了哪些文件
2. 如何运行这一步
3. 是否成功跑通
4. 输出文件路径
5. 关键统计：
   - 原始总行数
   - 去重后行数
   - is_warmup=True 条数
   - in_evaluation_window=True 条数
   - published_at 最早/最晚日期
   - ticker_raw 非空比例
6. 输出 parquet 的字段列表/schema
7. 随机展示几行样例，确认字段内容和时间解析正确
8. 剩余风险与下一步建议

注意：
- 不要擅自扩展任务范围
- 不要把 nasdaq_exteral_data.csv 并入主表
- 不要为了“更完整”而改动论文主线设定
