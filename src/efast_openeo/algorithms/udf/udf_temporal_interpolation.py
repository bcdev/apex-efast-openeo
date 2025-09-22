import pandas as pd
import xarray as xr
from openeo.metadata import CubeMetadata

from datetime import datetime, timezone


def apply_datacube(cube: xr.DataArray, context: list) -> xr.DataArray:
    """
    Expects ``cube`` to be an array of dimensions (t, bands, y, x)
    """

    assert "bands" in cube.dims, f"cube must have a 'bands' dimension, found '{cube.dims}'"


    t_target = pd.DatetimeIndex(context)
    interpolated = cube.interp(t=t_target)#, **interp_kwargs)
    dims = ('t' ,'bands','y', 'x')
    return interpolated.transpose(*dims)


def apply_metadata(metadata: CubeMetadata, context: list) -> CubeMetadata:
    # If context is passed from the client, it will be a lits of strings
    # If it is passed from the backend, it will be a list of datetimes
    if isinstance(context[0], str):
        t_target = [datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in context]
    else:
        t_target = context
    metadata = metadata.rename_labels(dimension="t", target=t_target)
    return metadata