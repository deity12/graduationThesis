你现在负责本项目的【阶段4：可行性验证】实现。

请严格基于当前项目既有设定继续，不要重置项目背景，不要改写阶段定义，不要越界到后续阶段。

---

## 维护备注（状态同步）

- 截至 2026-03-11：阶段4已在服务器完成真实推理验收（1 条 smoke + 100 条正式抽取）并产出 `outputs/stage4/stage4_report.*`。
- 当前下一步不实现阶段5全量抽取；仅做口径可复核与 `can_enter_stage5` gate 逻辑对齐与准入决策落地。

## 一、当前项目固定基线（必须遵守）

### 论文主线
- 股票横截面排序
- 用 LLM 从金融新闻中抽取事件溢出关系
- 构建 rolling semantic graph
- 同时构建 rolling price correlation graph
- 模型主线：2-layer GRU backbone + semantic graph + price graph + adaptive fusion + RankIC loss

### 固定方法约束
1. 语义边不是文本相似度边，而是 LLM 抽取的 source -> targets
2. LLM 只做结构抽取：source / targets / event_type
3. 禁止 polarity / strength / confidence
4. 语义图必须 rolling
5. 价格图必须 rolling correlation graph
6. 边权优先 binary 或 time-decayed count
7. 所有图构建、样本构建、特征切片、标签生成都必须显式接受 cutoff_date，并有断言防止使用 cutoff_date 之后的数据

### 时间协议
- 总评估区间：2016-01-01 ~ 2023-12-31
- warm-up 起点：2015-09-01
- warm-up 只用于窗口初始化，不能进入 train/val/test
- 4 个 walk-forward split：
  - S1: train 2016-2018 / val 2019 / test 2020
  - S2: train 2017-2019 / val 2020 / test 2021
  - S3: train 2018-2020 / val 2021 / test 2022
  - S4: train 2019-2021 / val 2022 / test 2023

### LLM 抽取方案（固定）
- 模型：Qwen/Qwen2.5-32B-Instruct-AWQ
- 引擎：vLLM
- schema 兼容策略：
  - 所有字段都用 string
  - 空值用 ""
  - 禁止 JSON null
  - 再在 structured_decode.py 中做 "" -> None
- 顶层字段只允许：source / targets / event_type

### 新闻正式口径（阶段2已封板）
- 正式新闻主语料 = All_external.csv + nasdaq_exteral_data.csv
- 统一标准化后去重合并
- 主产物：data/interim/news_normalized.parquet
- source mapping 主产物：data/processed/news_source_mapped.parquet
- 阶段2已正式封板，不得修改其口径

### 研究宇宙固定
- 最终研究宇宙固定为 SP500
- FNSPID 仅作为新闻来源，而非股票宇宙定义来源
- 上游标准化新闻表可以保留双源全量新闻用于审计与复现
- 但论文主线建模仅使用能够映射到研究宇宙内股票的新闻记录

### 阶段3已完成，不得改写其定义
已存在产物：
- data/processed/feature_panel_v1.parquet
- data/processed/forward_returns.parquet
- data/processed/sample_index.parquet
- data/processed/prices_daily.parquet

阶段4不得重算、改口径或破坏阶段2/3既有产物定义。

---

## 二、本阶段边界（必须严格遵守）

你现在只做【阶段4：可行性验证】。

### 允许做
1. 实现 Qwen/Qwen2.5-32B-Instruct-AWQ 的 vLLM 调用接口
2. 实现 vLLM-compatible schema compatibility precheck
3. 实现 structured_decode 后处理
4. 实现阶段4的 smoke test 与 100 条可行性验证流程
5. 实现可行性验证报告输出
6. 给出三级降级建议

### 明确禁止
1. 不要实现阶段5全量抽取
2. 不要实现阶段6语义图/价格图构建
3. 不要实现阶段7训练框架
4. 不要实现阶段8/9/10/11任何实验、baseline、图表总汇
5. 不要修改阶段2/3数据口径、时间协议、研究宇宙定义
6. 不要把阶段4写成“已完成全量抽取”

---

## 三、本轮工作模式：先本地开发，再服务器正式执行

本轮请按“两段式”工作：

### A. 本地开发阶段（当前必须完成）
目标：先把代码、配置、测试骨架、报告骨架写好，并尽可能完成不依赖真实模型权重的验证。

在本地阶段：
- 可以修改代码
- 可以补全 schema / prompts / decode / client / pipeline / report
- 可以做 mock、单元测试、非模型依赖 smoke
- 可以做数据抽样与样本准备
- 但如果本地没有模型、没有 vLLM 服务、没有 GPU，就不要伪造“正式推理已跑通”的结论

### B. 服务器正式执行阶段（当前只准备，不在本地伪造结果）
正式阶段4验收依赖：
- 可用 NVIDIA GPU
- 可运行 vLLM
- 已下载 Qwen/Qwen2.5-32B-Instruct-AWQ
- vLLM 服务可成功启动
- 真实跑完 1 条 smoke + 100 条抽取

如果本地不具备以上条件，请在最终汇报中明确区分：
- 哪些已经在本地完成
- 哪些需要上服务器后正式执行
- 当前是否只是“实现准备完成”而不是“正式验证完成”

---

## 四、优先复用已有文件，缺失时再创建

请先阅读 repo 中现有实现，遵循“最小必要修改原则”。

### 需要优先检查并补全/必要时创建的文件
- src/llm/vllm_client.py
- src/llm/schema_compat.py
- src/llm/structured_decode.py
- src/llm/prompts.py
- src/llm/extract_event_spillover.py
- src/llm/stage4_feasibility.py
- src/analysis/stage4_report.py
- tests/smoke/test_vllm_schema_compat.py
- tests/smoke/test_stage4_guided_json.py
- configs/llm/spillover_schema_v2_vllm_compatible.json
- configs/llm/stage4_feasibility_2016.yaml

### 命名要求
- 不要继续保留 stage0_* 命名
- 统一改成 stage4_*，并同步文档、测试、配置引用
- 若 repo 中已有 stage0_feasibility.py / stage0_report.py 等历史文件，请评估是否重命名并修正引用；避免阶段命名继续漂移

---

## 五、硬约束（必须全部满足）

1. 模型固定：Qwen/Qwen2.5-32B-Instruct-AWQ
2. 引擎固定：vLLM
3. 输出必须使用 guided JSON / structured outputs
4. schema 中禁止 anyOf、oneOf、nullable、null
5. 所有原本 string|null 的字段改成 string，空值统一用 ""
6. structured_decode.py 必须把 "" 转回 Python None
7. warm-up 新闻允许读取，但不得进入正式 100 条样本分母
8. LLM 只允许抽取 source / targets / event_type
9. 阶段4只验证抽取可行性，不做图构建、训练或 baseline
10. 所有与时间相关的处理必须显式遵守 cutoff_date / warm-up / evaluation window 协议
11. 正式样本只允许来自 2016-01-01 ~ 2016-12-31
12. 如需窗口初始化，可读取 2015-09-01 ~ 2015-12-31，但不能计入正式 100 条样本分母

---

## 六、阶段4环境预检查（必须先做）

在任何真实推理前，请先实现并检查以下 preflight：

1. 是否存在可用 NVIDIA GPU
2. CUDA / 驱动 / vLLM 环境是否可用
3. 模型路径或 HuggingFace 模型名是否可解析
4. vLLM 服务是否能成功启动
5. structured outputs / guided JSON 的最小调用是否具备运行条件

要求：
- 如果本地环境不满足，请不要硬跑真实推理
- 应在代码与报告中明确记录“环境未满足，正式执行待服务器完成”
- 环境检查失败不等于阶段4设计失败，但必须诚实报告

---

## 七、可行性验证执行顺序（必须严格）

### 第一步：阅读现有 repo
必须先阅读与阶段2/3、source mapping、sample_index、已有 llm 目录、配置目录、测试目录相关的文件，再开始改动。

### 第二步：完成本地代码与测试骨架
至少实现：
- vLLM client
- schema compatibility precheck
- structured decode
- prompts
- extract pipeline
- stage4 feasibility runner
- stage4 report generator
- smoke tests

### 第三步：准备正式抽样逻辑
正式抽样范围固定：
- 2016-01-01 ~ 2016-12-31

要求：
- 样本来自正式新闻主表 / source_mapped 结果
- 新闻需满足主线研究宇宙口径
- warm-up 可读取但不进正式 100 条分母
- 样本抽取逻辑要可复现，需支持 seed 固定

### 第四步：服务器真实验证时的执行顺序
1. 启动 vLLM 服务
2. 先做 1 条测试新闻的 schema compatibility smoke test
3. 只有 smoke test 通过，才开始正式 100 条抽取
4. 跑完后生成阶段4报告
5. 再根据指标给出继续/降级建议

### 第四步-A：本地模型与 vLLM 启动（服务器无法访问 Hugging Face 时）

当服务器外网到 HF 不通时，采用**离线下载 → 放入项目 → 本地路径加载**：

1. **在有 HF 的机器上下载**：
   ```bash
   pip install -U "huggingface_hub[cli]"
   hf download Qwen/Qwen2.5-32B-Instruct-AWQ --local-dir models/Qwen2.5-32B-Instruct-AWQ
   ```
   将 `models/Qwen2.5-32B-Instruct-AWQ` 放入项目根目录（或上传到服务器项目目录）。

2. **配置已指向项目内路径**：`configs/llm/runtime_qwen25_32b_awq.yaml` 中 `model_name: "models/Qwen2.5-32B-Instruct-AWQ"`。

3. **在项目根目录启动 vLLM**（单卡 48GB 可用较宽松参数）：
   ```bash
   vllm serve models/Qwen2.5-32B-Instruct-AWQ \
     --quantization awq \
     --dtype half \
     --max-model-len 8192 \
     --gpu-memory-utilization 0.95 \
     --port 8000
   ```

4. **验证服务**：`curl http://127.0.0.1:8000/v1/models`

5. **跑正式验证**：`python -m src.llm.stage4_feasibility --mode real --overwrite`

---

## 八、阶段4正式验证必须输出的指标

报告中必须有：

1. source 可映射比例
2. 有效 target 比例
3. 平均有效 target 数
4. 月度平均出度
5. 2016 年新闻密度
6. 活跃 source 节点数
7. “2016 年新闻覆盖是否足够”的明确结论

同时要明确区分：
- 本地 mock / 非正式结果
- 服务器真实推理后的正式结果

如果本地尚未正式跑模型，请不要伪造数值。

---

## 九、失败后的决策顺序（必须写入报告）

1. 若 2016 年足够，按 2016-2023、4 split 继续
2. 若 2016 年不足，先尝试 semantic_window_days: 63 -> 126，或月度更新降为季度更新
3. 若扩窗后 2016 仍不足，但 2017 足够，则降级到 2017-2023、3 split 继续
4. 只有上述三级都不满足，才建议暂停项目主线

要求：
- 决策建议必须写入 stage4 report
- 不得直接跳到后续阶段实现，只能给建议

---

## 十、测试要求

请至少补全并运行适合当前环境的测试。

### 必须覆盖
- schema compatibility precheck
- structured decode 的 "" -> None
- guided JSON smoke 路径
- 正式抽样范围与 warm-up 不入分母
- 关键配置可被正确读取
- 如果本地无法真实连 vLLM，则至少用 mock/skip 方式保证测试结构完整

### 测试命名
- tests/smoke/test_vllm_schema_compat.py
- tests/smoke/test_stage4_guided_json.py

如果已有类似测试，请最小改动复用，不要重复造轮子。

---

## 十一、产物要求

本轮完成后，至少应具备以下产物中的代码与配置骨架：

- vLLM 调用实现
- schema compatibility 检查器
- structured decode
- 阶段4 runner
- 阶段4 report 生成器
- 2016 可行性配置文件
- smoke tests

若服务器正式执行成功，再补充：
- 阶段4正式报告
- smoke 日志
- 100 条验证结果汇总
- 指标摘要

---

## 十二、输出格式（必须严格按此格式汇报）

请按以下固定格式输出结果：

1. 本次任务目标
2. 阅读了哪些现有文件/模块
3. 改了什么
4. 为什么这么改
5. 修改了哪些文件
6. 如何运行
   - 本地开发运行方式
   - 服务器正式执行方式
7. 运行了哪些测试
8. 是否跑通
   - 本地是否跑通
   - 服务器正式验证是否已完成
9. 生成了哪些产物
10. 关键统计或关键检查结果
11. 剩余风险
12. 明确说明是否可以进入下一阶段

### 其中第12项必须明确区分
- 是否可以进入“服务器上的阶段4正式验证”
- 是否已经可以宣称“阶段4正式完成”
- 是否可以进入阶段5

默认原则：
- 只有真实跑完 1 条 smoke + 100 条正式验证，并产出正式报告，才能宣称阶段4正式完成
- 只有阶段4正式完成并满足验收标准，才可以建议进入阶段5