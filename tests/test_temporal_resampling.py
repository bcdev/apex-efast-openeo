from pathlib import Path

import pytest

from efast_openeo.algorithms.temporal_interpolation import (
    interpolate_time_series_to_target_extent,
)


# Fixtures
@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "temporal_resampling"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def s3_at_s2_times_mock(s3_cube, s2_time_series):
    return s3_cube.filter_temporal(s2_time_series)


@pytest.mark.openeo
@pytest.mark.manual
def test_merge_cubes(s3_cube, s2_time_series, persistent_output_dir, run_openeo):
    """
    This test uses S3 input products instead of the S3 composites used by efast.
    The procedure for temporal resampling should be the same, as the composite cube
    should emulate the original S3 cube exactly.
    """

    s3_at_s2_time = interpolate_time_series_to_target_extent(s3_cube, s2_time_series)
    run_openeo(s3_at_s2_time, persistent_output_dir / "s3_at_s2_time")
