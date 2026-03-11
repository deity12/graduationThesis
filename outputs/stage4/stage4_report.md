# Stage 4 Feasibility Report

- Generated at (UTC): 2026-03-11T09:55:53.783609+00:00
- Run mode: real
- Config: `configs/llm/stage4_feasibility_2016.yaml`

## Preflight

- NVIDIA GPU available: True
- nvidia-smi available: True
- vLLM import available: True
- Model identifier parseable: True
- Server reachable: True
- Hosted model resolved: True
- Structured-output minimal call ready: True
- Minimal call checked live: False
- Ready for real inference: True

## Sampling

- Official sampled set means the fixed 100-row 2016 feasibility sample prepared for server validation.
- Local mock processed set means only a small local subset used to verify code paths without real vLLM inference.
- Warm-up candidate count (2015-09-01 to 2015-12-31): 106525
- Official candidate count (2016 only): 197164
- Official sampled rows: 100
- Local mock processed rows: 0
- Official sampled month coverage: ['2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11', '2016-12']

## Sample Lineage (Audit)

- Official sample parquet: `outputs/stage4/official_sample_2016.parquet`
- Direct input parquet: `data/processed/news_source_mapped.parquet`
- Upstream main table parquet: `data/interim/news_normalized.parquet`
- Sampling window: {'warmup_start': '2015-09-01', 'warmup_end': '2015-12-31', 'official_start': '2016-01-01', 'official_end': '2016-12-31'}
- Sampling seed: 7
- Lineage statement: Stage4 official sample is directly sampled from data/processed/news_source_mapped.parquet. The sampled news_id are 100% traceable in data/interim/news_normalized.parquet with key fields consistent, and source_file is restricted to the official dual-source corpus.

### Traceability Check

- Available: True
- Matched ratio in upstream table: 1.0
- published_date equal ratio: 1.0
- title equal ratio: 1.0
- source_file unique: ['All_external.csv', 'nasdaq_exteral_data.csv']
- source_file dual-source only: True

## Execution

- Local mock executed: False
- Formal run complete: True
- Results path: outputs/stage4/formal_results.parquet
- Smoke results path: outputs/stage4/smoke_results.parquet
- Execution note: Real server-side smoke and formal extraction completed.

## Formal Metrics

- Source mapping ratio: 0.83
- Valid target ratio: 0.52
- Average valid target count: 1.49
- Monthly average out-degree: 3.465116
- 2016 news density: 16430.333333
- Active source nodes: 32
- Coverage conclusion: sufficient

## Decision Order

- 若 2016 覆盖足够，按 2016-2023 / 4 split 继续。
- 若 2016 覆盖不足，先尝试 semantic_window_days: 63 -> 126，或改为季度更新。
- 若扩窗后 2016 仍不足但 2017 足够，则降级到 2017-2023 / 3 split。
- 只有上述三级都不满足，才建议暂停项目主线。

## Stage Gate

- Can start server Stage 4 validation: True
- Stage 4 formally complete: True
- Can enter Stage 5: True

## Universe Scope

- Research universe: SP500
- News source policy: Only news rows mapped into the SP500 research universe are eligible for Stage 4 sampling.
