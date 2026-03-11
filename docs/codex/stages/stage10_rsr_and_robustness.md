# Stage 10: RSR、经典 baseline 与 backbone robustness

你现在负责“阶段10：RSR、经典 baseline 与 backbone robustness”。

先阅读当前 repo 和主实验结果，再开始修改。
只做当前阶段，不改主模型结构和主实验协议。

## 本阶段目标

1. 实现或重包装 RSR；
2. 实现 ALSTM 或 SFM（二选一）；
3. 实现 iTransformer backbone；
4. 做有限度的 backbone robustness 实验。

## 必须创建或补全

- src/baselines/rsr_runner.py
- src/baselines/alstm_or_sfm_runner.py
- src/models/backbones/itransformer_backbone.py
- configs/experiment/rsr.yaml
- configs/experiment/alstm_or_sfm.yaml
- configs/model/temporal_itransformer.yaml

## 主线要求

- RSR 在统一 sample_index / walk-forward / RankIC 评估协议下运行；
- 经典 baseline 只选一个，优先实现更容易复现的那一个；
- iTransformer 只做 robustness，不替换主线 GRU。

## robustness 最小范围

- Temporal-only + iTransformer
- Full Model + iTransformer
- 优先先跑 S1 和 S4，或至少跑 S4

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
