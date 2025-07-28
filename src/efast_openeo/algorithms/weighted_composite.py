import importlib
import warnings
from typing import List

import numpy as np
from numpy.typing import NDArray

import openeo

UDF_TEMPORAL_SCORE = importlib.resources.files("efast_openeo.algorithms.udf").joinpath("udf_temporal_score_aggregate.py")

def compute_weighted_composite(distance_to_cloud_cube: openeo.DataCube, target_time_series: List[str]):
    """
    Computes a score weighted by the distance to the target date from the distance to cloud score.
    """
    udf = openeo.UDF.from_file(UDF_TEMPORAL_SCORE, context={"from_parameter": "context"})
    weighted = distance_to_cloud_cube.apply_dimension(process=udf, context=dict(t_target=target_time_series))
    return weighted
