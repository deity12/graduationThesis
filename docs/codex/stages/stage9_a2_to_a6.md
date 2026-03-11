# Stage 9: A2~A6 主实验矩阵

你现在负责“阶段9：A2~A6 主实验矩阵”。

先阅读当前 repo、训练框架和已有 baseline 实现，再开始修改。
只做当前阶段，不做 RSR、ALSTM/SFM 或 iTransformer robustness。

## 本阶段目标

1. 实现 semantic_encoder；
2. 实现 price_encoder；
3. 实现 simple_fusion；
4. 实现 adaptive_fusion；
5. 跑通 A2~A6 五个内部变体；
6. 输出 split-wise 结果表。

## 重要说明

- A1（Temporal-only）已在阶段8完成；
- 本阶段只需确认 A1 结果可以复用；
- 重点实现 A2~A6。

## 必须创建或补全

- src/models/graph/semantic_encoder.py
- src/models/graph/price_encoder.py
- src/models/fusion/simple_fusion.py
- src/models/fusion/adaptive_fusion.py
- src/models/dual_graph_ranker.py
- configs/experiment/static_sem.yaml
- configs/experiment/rolling_sem.yaml
- configs/experiment/price_only.yaml
- configs/experiment/dual_graph_simple.yaml
- configs/experiment/full_model_adaptive.yaml
- tests/smoke/test_dual_graph_forward.py
- tests/smoke/test_ablation_one_split.py

## 内部变体要求

- **A2** Temporal + Static SemGraph
- **A3** Temporal + Rolling SemGraph
- **A4** Temporal + PriceGraph
- **A5** Temporal + PriceGraph + Rolling SemGraph（simple fusion）
- **A6** Full Model（adaptive fusion）

## 硬约束

- semantic graph 必须是有向 in/out 分开聚合；
- price graph 先做无向一跳加权聚合；
- 所有图加载必须检查 graph_date <= cutoff_date；
- 不要扩展到多层 GAT，不要做 relation-specific channels。

## 修改后运行 smoke test

- 至少跑通一个 split 上的 A2~A6 forward；
- 至少完成一个 split 上的 A2~A6 小规模训练；
- 导出初版 split-wise 指标表；
- 验证 A1 结果可被当前实验矩阵复用。

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