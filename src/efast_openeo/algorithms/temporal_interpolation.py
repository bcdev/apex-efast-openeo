import importlib
from typing import List

import openeo


UDF_TEMPORAL_INTERPOLATION = importlib.resources.files("efast_openeo.algorithms.udf").joinpath("udf_temporal_interpolation.py")

def interpolate_time_series_to_target_labels(cube, target_time_series: List[str]):
    udf = openeo.UDF.from_file(UDF_TEMPORAL_INTERPOLATION, context={"from_parameter": "context"}, runtime="Python")#, version="3")
    interpolated = cube.apply_dimension(process=udf, dimension="t", context=target_time_series)
    return interpolated