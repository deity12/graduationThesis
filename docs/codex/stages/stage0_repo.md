# Stage 0: Repo 骨架

你现在负责“阶段0：repo骨架”。

先阅读当前 repo；如果 repo 基本空白，就创建最小但规范的工程骨架。
只做当前阶段，不要下载数据，不要实现训练，不要实现图模型，不要实现 LLM 抽取。

## 本阶段目标

1. 创建项目目录结构；
2. 创建最基础的配置文件；
3. 固定研究边界与时间边界；
4. 固定 warm-up 与评估区间；
5. 固定输出目录、数据目录与日志目录；
6. 补最小 README 和项目说明。

## 必须创建或补全的内容

- README.md
- requirements.txt 或 environment 文件
- configs/paths.yaml
- configs/data/walk_forward_2016_2023.yaml
- configs/llm/runtime_qwen25_32b_awq.yaml
- configs/llm/spillover_schema_v2_vllm_compatible.json（可先放占位版）
- configs/model/temporal_gru.yaml
- configs/experiment/ 下的占位配置文件
- src/、tests/、scripts/ 的最小目录骨架

## 关键硬约束

- 实验评估区间固定为 2016-01-01 到 2023-12-31；
- warm-up 起点固定为 2015-09-01；
- 在配置中明确写出：warm-up 数据只用于窗口初始化，不进入任何 train/val/test 样本；
- walk-forward 固定为 4 个 split：
  - **S1** 2016/2018 train, 2019 val, 2020 test
  - **S2** 2017/2019 train, 2020 val, 2021 test
  - **S3** 2018/2020 train, 2021 val, 2022 test
  - **S4** 2019/2021 train, 2022 val, 2023 test

## 要求

- 不要实现具体业务逻辑；
- 只把工程结构、配置和约束写清楚；
- 所有路径优先从 yaml 读取；
- 给后续阶段预留清晰入口。

## 修改后运行 smoke test

- 至少验证关键配置文件可被成功读取；
- 至少验证目录结构完整；
- 至少验证 walk_forward 配置能被解析。

## 最终输出

- 改了什么
- 为什么这么改
- 是否跑通
- 剩余风险
