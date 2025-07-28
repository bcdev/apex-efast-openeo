import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube
def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    assumes cube to be a slice shapes (t, bands) to be reduced into a single value
    """

    array = cube.get_array()

    t_target_str = context["target_time"]
    t_target = np.datetime64(t_target_str)
    t_obs = array.coords["t"]
    time_delta_in_days = t_obs - t_target
    score = (time_delta_in_days * array).sum(dim="t")

    # TODO normalize weights

    weights = xr.DataArray(
        score,
        # TODO extract dimension names as constants
        dims=["bands", "y", "x"],
    )

    return XarrayDataCube(weights)
