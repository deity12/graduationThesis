# Stage 6: 语义图与价格图

你现在负责“阶段6：语义图与价格图”。

先阅读当前 repo、抽取产物、价格与特征产物，再开始修改。
只做当前阶段，不要实现训练器、baseline 或 full model。

## 本阶段目标

1. 构建月度 rolling semantic graph；
2. 构建各 split 专属 static semantic graph；
3. 构建月度 rolling price graph；
4. 输出图统计；
5. 将 graph snapshot 选择逻辑接入 sample_index。

## 必须创建或补全

- src/graphs/build_semantic_snapshot.py
- src/graphs/build_semantic_static.py
- src/graphs/build_price_snapshot.py
- src/graphs/graph_store.py
- src/graphs/graph_stats.py
- src/graphs/snapshot_selector.py
- tests/leakage/test_semantic_cutoff.py
- tests/leakage/test_price_cutoff.py
- data/processed/graphs/semantic/monthly_edges.parquet
- data/processed/graphs/price/monthly_edges.parquet

## 语义图主线

- 先做 binary edge；
- 必做 time-decayed edge 作为可配置选项；
- 每个 source 保留 top-K=5 出边；
- 月度快照；
- 窗口默认 63 交易日。

## 价格图主线

- rolling corr；
- window=60；
- abs corr；
- top-K=10；
- 月度快照。

## static graph 规则

- S1 只能用 2016-2018 新闻；
- S2 只能用 2017-2019 新闻；
- S3 只能用 2018-2020 新闻；
- S4 只能用 2019-2021 新闻。

## 硬约束

- 所有图构建函数必须显式接受 cutoff_date；
- graph_date 必须 <= cutoff_date；
- warm-up 数据允许用于 2016 年初始窗口；
- 但不得通过 sample_index 泄露进未来。

## 修改后运行 smoke test

- 生成一个月的 semantic snapshot；
- 生成一个月的 price snapshot；
- 生成一个 split 的 static semantic graph；
- 抽样验证 graph_date 和 cutoff_date 关系正确。

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