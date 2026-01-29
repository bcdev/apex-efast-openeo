from pathlib import Path
import itertools
from datetime import datetime

import pandas as pd
import pytest
import numpy as np

from efast_openeo.algorithms.temporal_interpolation import (
    interpolate_time_series_to_target_extent,
)
from efast_openeo.algorithms.weighted_composite import compute_weighted_composite
from efast_openeo.algorithms.distance_to_cloud import (
    distance_to_cloud,
    compute_distance_score,
    compute_cloud_mask_s3,
)
from efast_openeo.data_loading import load_and_scale

from efast_openeo import constants


@pytest.fixture
def temporal_extent_target():
    return ["2022-09-09", "2022-09-25"]


@pytest.fixture
def interval_days():
    return 2


def test_date_range_s3_composites(
    connection,
    time_frame,
    s3_bands,
    s2_bands,
    aoi_bounding_box,
    dtc_max_distance,
    temporal_extent_target,
    interval_days,
    tmp_path,
):
    s3_data_bands = list(set(s3_bands) - {constants.S3_FLAG_BAND})

    s3_dtc_overlap_length_px = 100
    s3_dtc_patch_length_px = 250
    max_distance_to_cloud_s3_px = 20

    out_path = tmp_path / "outputs"
    out_path.mkdir()

    s3_bands = load_and_scale(
        connection=connection,
        use_binning=True,
        collection_id=constants.S3_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=s3_data_bands,
    )
    s3_flags = connection.load_collection(
        constants.S3_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=[constants.S3_FLAG_BAND],
    ).band(constants.S3_FLAG_BAND)
    s3_cloud_mask = compute_cloud_mask_s3(s3_flags)
    s3_distance_to_cloud = distance_to_cloud(
        s3_cloud_mask,
        image_size_pixels=s3_dtc_patch_length_px,
        max_distance_pixels=s3_dtc_overlap_length_px,
        pixel_size_native_units=constants.S3_RESOLUTION_DEG,
    )
    s3_distance_score = compute_distance_score(
        s3_distance_to_cloud, max_distance_to_cloud_s3_px
    )
    s3_bands_and_distance_score = s3_bands.merge_cubes(s3_distance_score)

    cube = compute_weighted_composite(
        s3_bands_and_distance_score,
        temporal_extent=time_frame,
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        sigma_doy=constants.S3_TEMPORAL_SCORE_STDDEV,
    )

    time_labels = cube.dimension_labels("t").execute()
    time_series = pd.DatetimeIndex(time_labels)
    delta = time_series.diff().days[1].astype(np.int64)

    assert np.all(delta == interval_days)

    start = datetime.strptime(temporal_extent_target[0], "%Y-%m-%d")
    end = datetime.strptime(temporal_extent_target[1], "%Y-%m-%d")
    assert len(time_series) == (end - start).days // interval_days


def test_date_range_interpolation(
    connection,
    aoi_bounding_box,
    time_frame,
    temporal_extent_target,
    s3_bands,
    interval_days,
):
    s3_data_bands = list(set(s3_bands) - {constants.S3_FLAG_BAND})
    s3_bands = load_and_scale(
        connection=connection,
        use_binning=True,
        collection_id=constants.S3_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=s3_data_bands,
    )
    s3_target_interp = interpolate_time_series_to_target_extent(
        s3_bands,
        temporal_extent=time_frame,
        interval_days=interval_days,
        temporal_extent_target=temporal_extent_target,
    )

    time_labels = s3_target_interp.dimension_labels("t").execute()
    time_series = pd.DatetimeIndex(time_labels)
    delta = time_series.diff().days[1].astype(np.int64)

    assert np.all(delta == interval_days)

    start = datetime.strptime(temporal_extent_target[0], "%Y-%m-%d")
    end = datetime.strptime(temporal_extent_target[1], "%Y-%m-%d")
    assert len(time_series) == (end - start).days // interval_days
