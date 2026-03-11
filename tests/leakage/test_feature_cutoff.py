from __future__ import annotations

import pandas as pd
import pytest

from src.common.leakage_guard import LeakageError, validate_feature_frame


def test_feature_frame_rejects_future_window_end() -> None:
    feature_frame = pd.DataFrame(
        [
            {
                "as_of_date": "2016-01-04",
                "feature_window_end_date": "2016-01-05",
            }
        ]
    )

    with pytest.raises(LeakageError):
        validate_feature_frame(feature_frame, cutoff_date="2016-01-04")
