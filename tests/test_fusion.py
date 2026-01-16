from pathlib import Path

import pytest

from efast_openeo.algorithms.distance_to_cloud import (
    distance_to_cloud,
    compute_cloud_mask_s2,
    compute_distance_score,
)
from efast_openeo.algorithms.temporal_interpolation import (
    interpolate_time_series_to_target_extent,
)
from efast_openeo.algorithms.weighted_composite import compute_weighted_composite
from efast_openeo.algorithms.fusion import fusion
from tests.conftest import S2_COLLECTION, S3_COLLECTION

MASK_BAND_S2 = "SCL"
MASK_BAND_S3 = "CLOUD_flags"


# Fixtures
@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "fusion"
    path.mkdir(parents=True, exist_ok=True)
    return path

@pytest.fixture
def sigma_doy():
    return 20

@pytest.fixture
def interval_days():
    return 3

@pytest.fixture
def time_frame_target(time_frame):
    return time_frame


@pytest.fixture
def s3_at_target_times(time_frame, time_frame_target, interval_days, s3_cube, s2_band_cube, s2_bands, s3_bands):
    s3_at_target_time = interpolate_time_series_to_target_extent(
        s3_cube,
        temporal_extent=time_frame,
        temporal_extent_target=time_frame_target,
        interval_days=interval_days,
    )
    return s3_at_target_time


@pytest.fixture
def s3_at_s2_times(connection, aoi_bounding_box, time_frame, time_frame_target, interval_days, s3_bands, s2_time_series):
    s3_cube_at_s3_time = connection.load_collection(
        S3_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=s3_bands,
    )

    s3_at_s2_time = interpolate_time_series_to_target_extent(
        s3_cube_at_s3_time,
        temporal_extent=time_frame,
        temporal_extent_target=time_frame_target,
        interval_days=interval_days,
    )
    return s3_at_s2_time


@pytest.fixture
def s3_at_s2_times_mock(
    connection, aoi_bounding_box, time_frame, s3_cube, s2_bands, s3_bands
):
    s2_band_names = [b for b in s2_bands if b != MASK_BAND_S2]
    s3_band_names = [b for b in s3_bands if b != MASK_BAND_S3]
    s2_band_cube_for_s3 = connection.load_collection(
        S2_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=s2_band_names,
    )

    s3_named_cube = s2_band_cube_for_s3.rename_labels(
        "bands", s3_band_names, s2_band_names
    )
    return s3_named_cube.resample_cube_spatial(s3_cube)


@pytest.mark.openeo
@pytest.mark.manual
def test_s3_at_s2_times_mock(s3_at_s2_times_mock, persistent_output_dir, run_openeo):
    run_openeo(s3_at_s2_times_mock, persistent_output_dir / "s3_at_s2_times_mock")


@pytest.fixture
def s2_dtc_cube(s2_scl_cube, image_size_pixels, overlap_size_pixels, dtc_max_distance):
    s2_dtc = distance_to_cloud(
        compute_cloud_mask_s2(s2_scl_cube.band("SCL")),
        image_size_pixels,
        max_distance_pixels=overlap_size_pixels,
        pixel_size_native_units=20,
    )
    s2_dtc_score_cube = compute_distance_score(s2_dtc, dtc_max_distance)
    return s2_dtc_score_cube


@pytest.fixture
def s2_band_cube(connection, aoi_bounding_box, time_frame, s2_bands):
    bands = [band for band in s2_bands if band != "SCL"]
    return connection.load_collection(
        S2_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=bands,
    )


@pytest.fixture
def s2_scl_cube(connection, aoi_bounding_box, time_frame):
    return connection.load_collection(
        S2_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=["SCL"],
    )


@pytest.fixture
def pre_aggregate_merge(s2_band_cube, s3_at_s2_times, s2_dtc_cube):
    # When this is executed, the band names will be s2 band names for some reason
    s3_at_s2_times_resampled = s3_at_s2_times.resample_cube_spatial(s2_band_cube)

    # s3_at_s2_times_resampled = s3_at_s2_times
    merged = s3_at_s2_times_resampled.merge_cubes(s2_band_cube)
    merged = merged.merge_cubes(s2_dtc_cube.filter_bands(["distance_score"]))
    return merged


@pytest.mark.openeo
@pytest.mark.manual
def test_merge_cubes(
    pre_aggregate_merge,
    s2_band_cube,
    s2_dtc_cube,
    s3_at_s2_times_mock,
    persistent_output_dir,
    run_openeo,
):
    """
    This test uses S3 input products instead of the S3 composites used by efast.
    The procedure for temporal resampling should be the same, as the composite cube
    should emulate the original S3 cube exactly.
    """

    # The dimension labels are helpful to discover and analyze the issue mentioned in this post:
    # https://forum.dataspace.copernicus.eu/t/combination-of-apply-neighborhood-and-merge-cubes-leads-to-additional-labels-in-the-time-dimension/4189/4
    # s2_dtc_cube.dimension_labels("t").download(persistent_output_dir / "s2_dtc_time_labels.json")
    # s2_band_cube.dimension_labels("t").download(persistent_output_dir / "s2_band_cube_time_labels.json")
    # s3_at_s2_times_mock.dimension_labels("t").download(persistent_output_dir / "s3_at_s2_times_mock_time_labels.json")
    # pre_aggregate_merge.dimension_labels("t").download(persistent_output_dir / "merged_time_labels.json")
    run_openeo(pre_aggregate_merge, persistent_output_dir / "pre_aggregate_merge")


@pytest.mark.openeo
@pytest.mark.manual
def test_combined_aggregation(
    time_frame,
    time_frame_target,
    interval_days,
    sigma_doy,
    pre_aggregate_merge,
    persistent_output_dir,
    run_openeo
):
    merged = pre_aggregate_merge

    s2_s3_aggregate = compute_weighted_composite(
        merged,
        temporal_extent=time_frame,
        temporal_extent_target=time_frame_target,
        interval_days=interval_days,
        sigma_doy=sigma_doy,
    )

    run_openeo(s2_s3_aggregate, persistent_output_dir / "s2_s3_aggregate")


@pytest.mark.openeo
@pytest.mark.manual
def test_get_s2_cube(s2_cube, persistent_output_dir, run_openeo):
    run_openeo(s2_cube, persistent_output_dir / "s2_input")


@pytest.mark.openeo
@pytest.mark.manual
def test_get_dtc_cube(s2_dtc_cube, persistent_output_dir, run_openeo):
    run_openeo(s2_dtc_cube, persistent_output_dir / "s2_dtc_input")


def test_fusion(
    time_frame,
    time_frame_target,
    interval_days,
    sigma_doy,
    s2_bands,
    s3_bands,
    s3_at_target_times,
    pre_aggregate_merge,
    persistent_output_dir,
    run_openeo,
):
    merged = pre_aggregate_merge

    selected_s3_bands = [band for band in s3_bands if band != MASK_BAND_S3]
    selected_s2_bands = [band for band in s2_bands if band != MASK_BAND_S2]

    s2_s3_aggregate = compute_weighted_composite(
        merged,
        temporal_extent=time_frame,
        temporal_extent_target=time_frame_target,
        interval_days=interval_days,
        sigma_doy=sigma_doy,
    )
    merged.dimension_labels("bands").download(
        persistent_output_dir / "merged_bands.json"
    )
    s2_s3_aggregate.dimension_labels("bands").download(
        persistent_output_dir / "s2_s3_aggregate_bands.json"
    )
    lr_m = s2_s3_aggregate.filter_bands(selected_s3_bands)
    hr_m = s2_s3_aggregate.filter_bands(selected_s2_bands)
    lr_p = s3_at_target_times.resample_cube_spatial(s2_s3_aggregate)

    fused = fusion(
        lr_m, hr_m, lr_p, selected_s3_bands, selected_s2_bands, ["MyBand02", "MyBand03"]
    )

    run_openeo(fused, persistent_output_dir / "s2_s3_aggregate")
