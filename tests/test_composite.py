from pathlib import Path

import pytest
import xarray as xr

from efast_openeo.algorithms.weighted_composite import compute_weighted_composite
from efast_openeo.algorithms.distance_to_cloud import (
    distance_to_cloud,
    compute_cloud_mask_s3,
    compute_distance_score,
)

CLOUD_BAND_S3 = "CLOUD_flags"
METRES_PER_PIXEL_S2 = 20
DEGREES_PER_PIXEL_S3 = 0.0027  # TODO check accuracy


@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "composite"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.mark.openeo
@pytest.mark.manual
def test_compute_composite(
    s3_bands,
    s3_cube,
    time_frame,
    connection,
    persistent_output_dir,
    image_size_pixels,
    overlap_size_pixels,
    file_extension,
    execute,
):
    D = 20 * 20
    t_start, t_end = time_frame
    interval_days = 2
    sigma_doy=10.0
    scl = s3_cube.band(CLOUD_BAND_S3)
    dtc = distance_to_cloud(
        compute_cloud_mask_s3(scl),
        image_size_pixels,
        max_distance_pixels=overlap_size_pixels,
        pixel_size_native_units=DEGREES_PER_PIXEL_S3,
    )
    distance_score = compute_distance_score(dtc, D)
    data_band_names = [band for band in s3_bands if band != CLOUD_BAND_S3]
    data_bands = s3_cube.filter_bands(data_band_names)
    data_bands_with_distance_score = data_bands.merge_cubes(distance_score)

    composite = compute_weighted_composite(
        data_bands_with_distance_score,
        temporal_extent=[t_start, t_end],
        temporal_extent_target=[t_start, t_end],
        interval_days=interval_days,
        sigma_doy=sigma_doy,
    )
    execute(composite)(
        (persistent_output_dir / "s3_composite").with_suffix(file_extension)
    )


@pytest.mark.openeo
@pytest.mark.manual
def test_download_input(s3_cube, persistent_output_dir):
    s3_cube.download(persistent_output_dir / "input.nc")


NOOP_UDF = """
import xarray as xr
from openeo.udf import inspect

def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    return cube
"""
