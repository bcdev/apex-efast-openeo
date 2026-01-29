import importlib
from typing import List

import openeo


UDF_TEMPORAL_INTERPOLATION = importlib.resources.files(
    "efast_openeo.algorithms.udf"
).joinpath("udf_temporal_interpolation.py")


def interpolate_time_series_to_target_extent(cube, *, temporal_extent, interval_days, temporal_extent_target, target_band_name_suffix=""):
    udf = openeo.UDF.from_file(
        UDF_TEMPORAL_INTERPOLATION,
        context={"from_parameter": "context"},
        runtime="Python",
    )
    context = dict(
        temporal_extent_input=temporal_extent,
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        target_band_name_suffix=target_band_name_suffix,
    )
    interpolated = cube.apply_dimension(process=udf, dimension="t", context=context)
    return interpolated


def interpolate_time_series_to_target_labels(cube, target_labels):
    udf = openeo.UDF.from_file(
        UDF_TEMPORAL_INTERPOLATION,
        context={"from_parameter": "context"},
        runtime="Python",
    )  # , version="3")
    interpolated = cube.apply_dimension(
        process=udf, dimension="t", context=target_labels
    )
    return interpolated
