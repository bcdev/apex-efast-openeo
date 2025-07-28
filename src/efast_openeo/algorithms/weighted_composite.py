import importlib
import warnings
from typing import List

import numpy as np
from numpy.typing import NDArray

import openeo

UDF_TEMPORAL_SCORE_SINGLE_PATH = importlib.resources.files("efast_openeo.algorithms.udf").joinpath("udf_temporal_score_single.py")
# Relies on aggregate_temporal supporting UDF reducers, which appears to not be the case (2025-07-16 Hannes Neuschmidt)
UDF_TEMPORAL_SCORE_AGGREGATE_PATH = importlib.resources.files("efast_openeo.algorithms.udf").joinpath("udf_temporal_score_aggregate.py")


def compute_score_band_wise(distance_to_cloud_cube, target_time_series):
    """
    Computes the combined score for each
    """
    udf = openeo.UDF.from_file(UDF_TEMPORAL_SCORE_SINGLE_PATH, context={"from_parameter": "context"})
    target_bands = []
    reducer = udf
    for t_target in target_time_series:
        cube_with_score = distance_to_cloud_cube.reduce_dimension(reducer=reducer, dimension="t", context={"target_time": t_target})
        score_band = cube_with_score.band("DTC") #
        target_bands.append((t_target, score_band))

    t_target_first, score_band_first = target_bands[0]
    cube = score_band_first.add_dimension("bands", t_target_first, type="bands")
    for (t_target, score_band) in target_bands[1:]:
        print(t_target)
        cube_with_dim = score_band.add_dimension("bands", t_target, type="bands")
        cube = cube.merge_cubes(cube_with_dim)

    return cube

def compute_weighted_composite(distance_to_cloud_cube: openeo.DataCube, target_time_series: List[str]):
    udf = openeo.UDF.from_file(UDF_TEMPORAL_SCORE_AGGREGATE_PATH, context={"from_parameter": "context"})
    weighted = distance_to_cloud_cube.apply_dimension(process=udf, context=dict(t_target=target_time_series))
    return weighted
