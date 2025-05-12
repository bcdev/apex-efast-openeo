from openeo.udf.debug import inspect
from scipy.ndimage import distance_transform_edt
import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Expects the cloud mask as input (in contrast to ``distance_transform_edt``).
    This is necessary, because ``apply_neighbourhood`` pads the input with zeros and not ones.
    """
    array = cube.get_array()
    # This special case appears to create some issues, so we skip it
    if not array.any():
        return XarrayDataCube(xr.DataArray(np.full_like(array, np.nan), dims=["t", "y", "x"]))
    distance = distance_transform_edt(np.logical_not(array))
    return XarrayDataCube(xr.DataArray(distance, dims=["t", "y", "x"]))
