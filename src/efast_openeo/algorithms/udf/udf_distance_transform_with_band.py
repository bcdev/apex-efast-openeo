from scipy.ndimage import distance_transform_edt
import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube

def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    """
    Expects the cloud mask as input (in contrast to ``distance_transform_edt``).
    This is necessary, because ``apply_neighborhood`` pads the input with zeros and not ones.
    """
    array = cube.get_array()
    # This special case appears to create some issues, so we skip it
    if not array.any():
        return xr.DataArray(np.full_like(array, np.nan), dims=["bands", "t", "y", "x"])
    distance = distance_transform_edt(np.logical_not(array.isel(bands=0)))
    return xr.DataArray(distance.expand_dims(dim="bands", axis=0), dims=["bands", "t", "y", "x"])
