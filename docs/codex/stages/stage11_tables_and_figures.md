# Stage 11: 论文图表与结果汇总

你现在负责“阶段11：论文图表与结果汇总”。

先阅读当前 repo、所有 run 结果与实验配置，再开始修改。
只做分析、导出、报告与复现整理，不再改模型定义。

## 本阶段目标

1. 汇总 A1~A6、MASTER、RSR 等结果；
2. 输出 split-wise、half-year/quarterly 稳定性图；
3. 输出 Static vs Rolling 图；
4. 输出 Full Model vs MASTER 图；
5. 输出阶段4可行性验证表；
6. 生成论文实验章节所需表格与图；
7. 整理可复现实验说明。

## 必须创建或补全

- src/analysis/plot_rankic_by_split.py
- src/analysis/plot_rankic_by_halfyear.py
- src/analysis/plot_performance_decay.py
- src/analysis/export_tables.py
- docs/EXPERIMENT_PROTOCOL.md
- docs/MASTER_REPRO_DIFFS.md
- outputs/tables/
- outputs/figures/

## 必须导出的内容

- RankIC(S1~S4)
- Mean RankIC across 4 splits
- RankIC std across 4 splits
- Decay = RankIC(S1_test) - RankIC(S4_test)
- Static vs Rolling split-wise 图
- Full Model vs MASTER split-wise 图
- half-year 或 quarterly 稳定性图
- 阶段4可行性验证表
- MASTER 差异说明表

## 论文写法要求

必须在导出的 MASTER 说明中保留这句标准文字：

> “为确保比较公平性，MASTER 在与本文相同的数据协议、特征集和评估流程下进行复现，与原论文实现存在以下差异：[列出差异]。”

## 修改后运行 smoke test

- 至少能从已有 predictions 自动重算表格；
- 至少能自动生成一张 split-wise 图和一张稳定性图；
- 不允许手工拼接主结果表。

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
