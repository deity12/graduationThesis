# Stage 5: 全量抽取流水线

你现在负责“阶段5：全量抽取流水线”。

先阅读当前 repo、可行性验证报告和已有 llm 模块，再开始修改。
只做当前阶段，不要实现图构建、训练或 baseline。

## 本阶段前提

- 阶段4已通过，或已确定采用哪一级降级方案。

## 本阶段目标

1. 实现 2015-09-01 到 2023-12-31 的按月分片抽取；
2. 实现 raw / parsed 双层缓存；
3. 实现断点续跑；
4. 实现 target resolver；
5. 保证 warm-up 新闻只用于初始化，不直接进入评估样本。

## 必须创建或补全

- src/llm/batch_extract.py
- src/llm/cache_manager.py
- src/llm/response_repair.py
- src/mapping/target_resolver.py
- configs/llm/full_extract_2016_2023.yaml
- data/cache/llm_raw/
- data/cache/llm_parsed/
- data/processed/spillover_extractions.parquet
- data/processed/spillover_edges.parquet

## 硬约束

- 抽取准备范围固定为 2015-09-01 到 2023-12-31；
- warm-up 期间数据只用于窗口初始化；
- 所有时间过滤逻辑必须带 cutoff_date；
- schema compatibility precheck 必须在正式调用前执行；
- 不得加入 polarity、strength、confidence。

## 建议执行顺序

1. 先抽 2015-09 到 2018-12；
2. 确认图和训练链路能通；
3. 再补 2019-01 到 2023-12。

## 修改后运行 smoke test

- 能对一个月分片抽取并缓存；
- 能中断后续跑；
- 能生成边表；
- 不出现 structured-output schema 错误。

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
