import pandas as pd
import xarray as xr
from openeo.metadata import CubeMetadata

from datetime import datetime, timezone


def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    """
    Expects ``cube`` to be an array of dimensions (t, bands, y, x)
    """

    assert "bands" in cube.dims, f"cube must have a 'bands' dimension, found '{cube.dims}'"
    assert "t_target" in context, f"The target time dimension 't_target' must be provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."

    interp_kwargs = context.get("kwargs_interp", {})

    t_target = pd.DatetimeIndex(context["t_target"])
    interpolated = cube.interp(t=t_target, **interp_kwargs)
    dims = ('t' ,'bands','y', 'x')
    return interpolated.transpose(*dims)


def apply_metadata(metadata: CubeMetadata, context: dict) -> CubeMetadata:
    t_target = [datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in context["t_target"]]
    metadata = metadata.rename_labels(dimension="t", target=t_target)
    return metadata