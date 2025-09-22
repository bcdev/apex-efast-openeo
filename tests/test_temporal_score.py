from pathlib import Path

import pandas as pd
import pytest

from efast_openeo.algorithms.distance_to_cloud import compute_cloud_mask_s3, distance_to_cloud_s3
from efast_openeo.algorithms.weighted_composite import compute_weighted_composite


# Fixtures
@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "temporal_score"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.mark.openeo
@pytest.mark.manual
def test_temporal_score_aggregate(time_frame, s3_cube, persistent_output_dir, execute, file_extension):
    target_times = pd.date_range(start=time_frame[0], end=time_frame[1], freq="2D")
    target_times = target_times.strftime("%Y-%m-%d").tolist()
    scl = s3_cube.band("CLOUD_flags")

    dtc = distance_to_cloud_s3(compute_cloud_mask_s3(scl), 100, 50)
    score = compute_weighted_composite(dtc, target_times)
    execute(score)((persistent_output_dir / "temporal_score_agg").with_suffix(file_extension))