import pandas as pd
import xarray as xr
from openeo.metadata import CubeMetadata

from datetime import datetime, timezone


def apply_datacube(cube: xr.DataArray, context: list) -> xr.DataArray:
    """
    Interpolate cube to the time series passed as context.
    Currently (2025-09-30), on the CDSE OpenEO backend, the target time series can only be passed as the complete context,
    when using the ``dimension_labels`` process. This is necessary to pass the S2 time series directly
    from an S2 cube with a user-defined temporal extent.

    This means, no further parameters can be passed via context, as it must be a list, not a dict.

    Expects ``cube`` to be an array of dimensions (t, bands, y, x)
    """
    # TODO link CDSE forum post in doctring

    assert "bands" in cube.dims, f"cube must have a 'bands' dimension, found '{cube.dims}'"

    t_target = pd.DatetimeIndex(context)
    interpolated = cube.interp(t=t_target)
    dims = ('t' ,'bands','y', 'x')
    return interpolated.transpose(*dims)


def apply_metadata(metadata: CubeMetadata, context: list) -> CubeMetadata:
    if isinstance(context[0], str):
        t_target = [datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in context]
    else:
        t_target = context
    metadata = metadata.rename_labels(dimension="t", target=t_target)
    return metadata