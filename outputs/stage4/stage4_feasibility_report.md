# Stage 4 Feasibility Report

- Generated at (UTC): 2026-03-08T05:28:55.975234+00:00
- Run mode: mock
- Config: `configs/llm/stage4_feasibility_2016.yaml`

## Preflight

- NVIDIA GPU available: True
- nvidia-smi available: True
- vLLM import available: False
- Model identifier parseable: True
- Server reachable: False
- Hosted model resolved: False
- Structured-output minimal call ready: False
- Minimal call checked live: False
- Ready for real inference: False

## Sampling

- Official sampled set means the fixed 100-row 2016 feasibility sample prepared for server validation.
- Local mock processed set means only a small local subset used to verify code paths without real vLLM inference.
- Warm-up candidate count (2015-09-01 to 2015-12-31): 106525
- Official candidate count (2016 only): 197164
- Official sampled rows: 100
- Local mock processed rows: 5
- Official sampled month coverage: ['2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11', '2016-12']

## Execution

- Local mock executed: True
- Formal run complete: False
- Results path: outputs\stage4\stage4_local_mock_results.parquet
- Smoke results path: 
- Execution note: The official 100-row 2016 sample has been prepared, but only a small local mock subset was processed. Real 1-row smoke plus 100-row formal extraction remains pending on the server.

## Formal Metrics

- Source mapping ratio: None
- Valid target ratio: None
- Average valid target count: None
- Monthly average out-degree: None
- 2016 news density: None
- Active source nodes: None
- Coverage conclusion: pending_server_validation

## Decision Order

- 若 2016 覆盖足够，按 2016-2023 / 4 split 继续。
- 若 2016 覆盖不足，先尝试 semantic_window_days: 63 -> 126，或改为季度更新。
- 若扩窗后 2016 仍不足但 2017 足够，则降级到 2017-2023 / 3 split。
- 只有上述三级都不满足，才建议暂停项目主线。

## Stage Gate

- Can start server Stage 4 validation: True
- Stage 4 formally complete: False
- Can enter Stage 5: False

## Universe Scope

- Research universe: SP500
- News source policy: Only news rows mapped into the SP500 research universe are eligible for Stage 4 sampling.
