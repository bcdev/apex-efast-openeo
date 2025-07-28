from openeo.udf.debug import inspect
import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube
def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()
    cube = np.ones_like(array, shape=array.shape[-4:])
    score = cube.sum(axis=0)
    inspect(None, f"shape of input: {array.shape}", "User", "warning")
    #inspect(None, f"dims of input: {getattr(array, 'dims', 'nodims')}", "User", "warning")
    weights = xr.DataArray(
        score,
        dims=["bands", "y", "x"],
    )
    inspect(None, "I AM MOCK GROOT", "User", "warning")
    return XarrayDataCube(weights)
