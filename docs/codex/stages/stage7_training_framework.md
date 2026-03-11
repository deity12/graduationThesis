# Stage 7: 训练框架

你现在负责“阶段7：训练框架”。

先阅读当前 repo 和图产物，再开始修改。
只做当前阶段，不要实现 MASTER、RSR 或双图模型。

## 本阶段目标

1. 将语义图/价格图快照索引接入 sample_index；
2. 完成 dataset/dataloader；
3. 完成 trainer/evaluator/model_selection；
4. 落地 validation-only model selection；
5. 让训练框架可以先跑 Temporal-only。

## 必须创建或补全

- src/training/sample_index.py
- src/training/dataset.py
- src/training/trainer.py
- src/training/evaluator.py
- src/training/model_selection.py
- tests/smoke/test_train_temporal.py

## 硬约束

- sample_index 中必须显式记录 sem_graph_date 与 price_graph_date；
- label_start_date 必须严格大于 as_of_date；
- 所有读取图/特征/标签的路径都必须再次检查 cutoff_date；
- warm-up 数据不得出现在 dataset 的可训练样本里。

## 修改后运行 smoke test

- S1 上能生成可训练 dataset；
- 能完成一个极小的 Temporal-only 训练回合；
- 能输出 predictions.parquet 和 selection log。

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
