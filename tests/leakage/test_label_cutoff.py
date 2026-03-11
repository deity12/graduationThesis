from __future__ import annotations

import pandas as pd
import pytest

from src.common.leakage_guard import LeakageError, validate_forward_returns_frame


def test_forward_returns_reject_same_day_label_start() -> None:
    label_frame = pd.DataFrame(
        [
            {
                "as_of_date": "2016-01-04",
                "label_start_date": "2016-01-04",
                "label_end_date": "2016-01-04",
            }
        ]
    )

    with pytest.raises(LeakageError):
        validate_forward_returns_frame(label_frame, cutoff_date="2016-01-04")
