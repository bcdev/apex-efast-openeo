from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from efast_openeo.algorithms.distance_to_cloud import distance_to_cloud, cloud_mask_s3, distance_to_cloud_s3
from efast_openeo.algorithms.weighted_composite import compute_score_band_wise, compute_weighted_composite


# Fixtures
@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "temporal_score"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.mark.openeo
@pytest.mark.manual
def test_temporal_score(time_frame, s3_cube, persistent_output_dir, execute, file_extension):
    target_times = pd.date_range(start=time_frame[0], end=time_frame[1], freq="2D").strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    scl = s3_cube.band("CLOUD_flags")

    dtc = distance_to_cloud_s3(cloud_mask_s3(scl), 100, 50)
    #dtc = distance_to_cloud(cloud_mask_s3(scl), 100, max_distance_pixels=50, pixel_size_native_units=0.0026996)
    score = compute_score_band_wise(dtc, target_times)
    execute(score)((persistent_output_dir / "temporal_score").with_suffix(file_extension))
    #execute(dtc)((persistent_output_dir / "dtc").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_temporal_score_aggregate(time_frame, s3_cube, persistent_output_dir, execute, file_extension):
    target_times = pd.date_range(start=time_frame[0], end=time_frame[1], freq="2D")
    target_times = target_times.strftime("%Y-%m-%d").tolist()
    scl = s3_cube.band("CLOUD_flags")

    dtc = distance_to_cloud_s3(cloud_mask_s3(scl), 100, 50)
    score = compute_weighted_composite(dtc, target_times)
    execute(score)((persistent_output_dir / "temporal_score_agg").with_suffix(file_extension))


# TODO remove
def test_merge_cubes(time_frame, s3_cube, persistent_output_dir, execute, file_extension):
    #["2022-09-22", "2022-09-26"]
    start = s3_cube.filter_temporal([time_frame[0], time_frame[0]])
    #end = s3_cube.filter_temporal([time_frame[1], time_frame[1]])
    end = s3_cube.filter_temporal(["2022-09-24", time_frame[1]])
    merged = start.merge_cubes(end, overlap_resolver="max")
    execute(merged)((persistent_output_dir / "merge_cubes").with_suffix(file_extension))


def test_merge_cubes_with_manual_time(s3_cube, time_frame, persistent_output_dir, execute, file_extension):
    #s3_cube.dimension_labels("t").download(persistent_output_dir / "time_steps.json")
    first = s3_cube.filter_temporal(["2022-09-23", "2022-09-24"])
    #first.download(persistent_output_dir / "input_without_time.nc" )
    dropped = first.drop_dimension("t")
    dropped = dropped.drop_dimension("bands")
    with_manual_time_a = dropped.add_dimension("t","2025-06-06T00:00:00.000Z", type="temporal")
    with_manual_time_b = dropped.add_dimension("t","2025-06-07T00:00:00.000Z", type="temporal")
    #with_manual_time_c = dropped.add_dimension("t","2025-06-08T00:00:00.000Z", type="temporal")
    #merged = with_manual_time_a.merge_cubes(with_manual_time_b, overlap_resolver="sum")
    merged = with_manual_time_a.merge_cubes(with_manual_time_b)
    #merged.dimension_labels("t").download("dimension_labels_ab.json")
    #merged = merged.drop_dimension("bands")
    #merged = merged.merge_cubes(with_manual_time_c)
    #merged.dimension_labels("t").download("dimension_labels_abc.json")
    #merged.add_dimension("bands", "score", type="bands")
    execute(merged)((persistent_output_dir / "manual_time_steps").with_suffix(file_extension))

def test_merge_cubes_merging_on_bands(s3_cube, time_frame, persistent_output_dir, execute, file_extension):
    first = s3_cube.filter_temporal(["2022-09-23", "2022-09-24"])
    dropped = first.drop_dimension("t")
    dropped = dropped.drop_dimension("bands")
    with_manual_time_a = dropped.add_dimension("bands", "2025-06-06", type="bands")
    with_manual_time_b = dropped.add_dimension("bands","2025-06-07", type="bands")
    with_manual_time_c = dropped.add_dimension("bands","2025-06-08", type="bands")
    #merged = with_manual_time_a.merge_cubes(with_manual_time_b, overlap_resolver="sum")
    merged = with_manual_time_a.merge_cubes(with_manual_time_b)
    #merged.dimension_labels("t").download("dimension_labels_ab.json")
    #merged = merged.drop_dimension("bands")
    merged = merged.merge_cubes(with_manual_time_c)
    #merged.dimension_labels("t").download("dimension_labels_abc.json")
    execute(merged)((persistent_output_dir / "merging_on_bands").with_suffix(file_extension))
