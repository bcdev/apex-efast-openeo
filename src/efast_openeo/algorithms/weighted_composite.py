import importlib
from typing import List

import openeo
from openeo.api.process import Parameter

UDF_TEMPORAL_SCORE = importlib.resources.files("efast_openeo.algorithms.udf").joinpath(
    "udf_temporal_score_aggregate.py"
)


def compute_weighted_composite(
    cube_with_distance_score: openeo.DataCube,
    temporal_extent: List[str] | Parameter | None,
    temporal_extent_target: List[str] | Parameter | None,
    interval_days: int,
    sigma_doy: float,
    use_stepwise_aggregation: bool = False,
):
    """
    Computes a score weighted by the distance to the target date from the distance to cloud score.
    The target time series is defined as every ``interval_days`` days starting at the lower limit
    of ``temporal_extent_target`` (inclusive) up to the upper limit of ``temporal_extent_target`` (exclusive).
    If ``temporal_extent_target`` is not set, ``temporal_extent_input`` is used. One of these two parameters
    must be set.

    :param use_stepwise_aggregation: If True use alternative implementation of the composite UDF that computes
        each target time step separately. This should reduce memory requirements.
    """
    udf = openeo.UDF.from_file(
        UDF_TEMPORAL_SCORE, context={"from_parameter": "context"}, runtime="Python"
    )  # , version="3")
    context = dict(
        temporal_extent_input=temporal_extent,
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        sigma_doy=sigma_doy,
        use_stepwise_aggregation=use_stepwise_aggregation,
    )
    weighted = cube_with_distance_score.apply_dimension(
        process=udf, dimension="t", context=context
    )
    return weighted
