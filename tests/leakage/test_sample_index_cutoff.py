from __future__ import annotations

import pandas as pd
import pytest

from src.common.leakage_guard import LeakageError, validate_sample_index_frame


def test_sample_index_rejects_warmup_as_of_date() -> None:
    sample_frame = pd.DataFrame(
        [
            {
                "as_of_date": "2015-12-31",
                "cutoff_date": "2015-12-31",
                "label_start_date": "2016-01-04",
                "label_end_date": "2016-01-04",
            }
        ]
    )

    with pytest.raises(LeakageError):
        validate_sample_index_frame(
            sample_frame,
            cutoff_date="2016-01-04",
            evaluation_start="2016-01-01",
        )
