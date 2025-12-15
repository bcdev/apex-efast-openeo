import importlib
from typing import List

import openeo

UDF_TEMPORAL_SCORE = importlib.resources.files("efast_openeo.algorithms.udf").joinpath(
    "udf_temporal_score_aggregate.py"
)


def compute_weighted_composite(
    cube_with_distance_score: openeo.DataCube,
    temporal_extent: List[str],
    interval_days: int,
    sigma_doy: float = None,
):
    """
    Computes a score weighted by the distance to the target date from the distance to cloud score.
    The target time series is defined as every ``interval_days`` days starting at the lower limit
    of ``temporal_extent`` (inclusive) up to the upper limit of ``temporal_extent`` (exclusive)
    """
    udf = openeo.UDF.from_file(
        UDF_TEMPORAL_SCORE, context={"from_parameter": "context"}, runtime="Python"
    )  # , version="3")
    context = dict(
        temporal_extent=temporal_extent,
        interval_days=interval_days,
        sigma_doy=sigma_doy,
    )
    weighted = cube_with_distance_score.apply_dimension(
        process=udf, dimension="t", context=context
    )
    return weighted
