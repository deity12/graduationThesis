from __future__ import annotations

from src.analysis.stage4_report import compute_can_enter_stage5


def test_stage5_gate_accepts_passing_metrics() -> None:
    assert (
        compute_can_enter_stage5(
            {
                "source_mapping_ratio": 0.80,
                "valid_target_ratio": 0.25,
                "monthly_avg_out_degree": 1.0,
                "coverage_conclusion": "sufficient",
            }
        )
        is True
    )


def test_stage5_gate_rejects_insufficient_coverage() -> None:
    assert (
        compute_can_enter_stage5(
            {
                "source_mapping_ratio": 0.99,
                "valid_target_ratio": 0.99,
                "monthly_avg_out_degree": 9.0,
                "coverage_conclusion": "insufficient",
            }
        )
        is False
    )

