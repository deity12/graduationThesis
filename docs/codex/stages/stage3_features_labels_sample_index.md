# Stage 3: 特征、标签与 sample_index 底座

你现在负责“阶段3：特征、标签与 sample_index 底座”。

先阅读当前 repo 与数据产物，再开始修改。
只做当前阶段，不要实现 LLM 抽取、图构建、训练或 baseline。

## 本阶段目标

1. 实现统一的 leakage_guard；
2. 构建 feature panel；
3. 构建 forward return label table；
4. 构建 sample_index 底座；
5. 明确 warm-up 数据只能用于初始化窗口，不进入任何 train/val/test 样本。

## 必须创建或补全

- src/common/leakage_guard.py
- src/data/build_feature_panel.py
- src/data/build_forward_returns.py
- src/training/sample_index.py（先做无图版骨架）
- tests/leakage/test_feature_cutoff.py
- tests/leakage/test_label_cutoff.py
- tests/leakage/test_sample_index_cutoff.py
- data/processed/feature_panel_v1.parquet
- data/processed/forward_returns.parquet
- data/processed/sample_index.parquet（先可不含 graph_date，但要有 split/date/ticker/label 基础字段）

## 特征主线

- 使用价格衍生特征为主；
- 至少包括 ret_1/5/20、vol_5/20、volume zscore、hl spread、oc return；
- 在 feature panel 中加入简单市场特征 feat_mkt_ret_1d。

## 硬约束

- 所有用于样本构建的函数必须接受 cutoff_date；
- 所有样本 as_of_date 必须 >= 2016-01-01；
- warm-up 数据（2015-09-01 到 2015-12-31）只允许参与 rolling 计算，不允许进入 train/val/test 样本；
- 标签必须严格来自 as_of_date 之后。

## 修改后运行 smoke test

- feature panel 生成成功；
- label table 生成成功；
- sample_index 至少生成 S1 的初版；
- 至少一个未来数据注入测试必须失败，证明 leakage guard 生效。

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
