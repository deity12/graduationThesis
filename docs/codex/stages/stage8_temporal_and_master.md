# Stage 8: Temporal-only 与 MASTER

你现在负责“阶段8：Temporal-only 与 MASTER”。

先阅读当前 repo、训练框架和 baseline 配置，再开始修改。
只做当前阶段，不做双图模型、RSR 或 robustness。

## 本阶段目标

1. 实现 2-layer GRU backbone；
2. 跑通 Temporal-only；
3. 用“架构复用 + 数据替换”方案实现 MASTER；
4. 统一使用 RankIC loss；
5. 输出 MASTER 与原论文实现的差异清单草稿。

## 必须创建或补全

- src/models/backbones/gru_backbone.py
- src/models/heads/rank_head.py
- src/models/losses/rankic_loss.py
- src/baselines/temporal_only.py
- src/baselines/master_runner.py
- configs/experiment/temporal_only.yaml
- configs/experiment/master.yaml
- tests/baseline/test_master_one_split.py

## MASTER 复现硬约束

1. 必须采用“架构复用 + 数据替换”；
2. 必须接入本项目 sample_index、feature_panel、walk-forward；
3. 必须使用 validation-only selection；
4. 训练损失统一使用 RankIC loss；
5. 优先尝试在 feature panel 中加入 SP500 当日收益作为简单 market feature；
6. 若实现复杂度过高，可去掉独立 market information 输入，但必须在输出中明确记录这一差异。

## 本阶段目标最小验收

- 跑通 S1 上的 Temporal-only；
- 跑通 S1 上的 MASTER；
- 输出 predictions.parquet；
- 输出 model_selection_log.csv；
- 输出 MASTER 差异清单草稿。

## 修改后运行 smoke test

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
