import pandas as pd
import xarray as xr
from openeo.metadata import CubeMetadata

from datetime import datetime, timezone


def apply_datacube(cube: xr.DataArray, context) -> xr.DataArray:
    """
    Interpolate cube to the time series passed as context.
    Currently (2025-09-30), on the CDSE OpenEO backend, the target time series can only be passed as the complete context,
    when using the ``dimension_labels`` process. This is necessary to pass the S2 time series directly
    from an S2 cube with a user-defined temporal extent.
    This UDF supports two cases: passing the target time series ``t_target`` as a pd.Datetimeidex
    (this is what happens when chaining with the dimension_labels process) and passing dictionary as a context,
    which defines the time series through the parameters ``temporal_extent`` (left incl, right excl)
    and ``interval_days``.

    Expects ``cube`` to be an array of dimensions (t, bands, y, x)
    """
    assert "bands" in cube.dims, (
        f"cube must have a 'bands' dimension, found '{cube.dims}'"
    )

    t_target = get_t_target_from_context(context)

    # The Wizard passes the temporal extent as a xr.IndexVariable which cannot be understood by xr.interp
    if isinstance(t_target, xr.IndexVariable) or isinstance(t_target, xr.DataArray):
        t_target = t_target.values
    t_target = pd.to_datetime(t_target)

    if getattr(t_target, "tz", None) is not None:
        t_target = t_target.tz_localize(None)

    interpolated = cube.interp(t=t_target)
    dims = ("t", "bands", "y", "x")
    return interpolated.transpose(*dims)


def apply_metadata(metadata: CubeMetadata, context) -> CubeMetadata:
    t_target = get_t_target_from_context(context)
    metadata = metadata.rename_labels(dimension="t", target=t_target)
    return metadata


def get_t_target_from_context(context):
    if isinstance(context, dict):  # from user parameters
        temporal_extent = context.get("temporal_extent_target")
        if temporal_extent is None or len(temporal_extent) == 0: # use input temporal extent as a fallback if temporal extent target is not set
            temporal_extent = context["temporal_extent_input"]
        interval_days = context["interval_days"]
        t_target = compute_t_target(temporal_extent, interval_days)
    else:
        t_target = context

    return t_target


def compute_t_target(temporal_extent, interval_days) -> pd.DatetimeIndex:
    t_target = xr.date_range(
        temporal_extent[0],
        temporal_extent[1],
        freq=f"{interval_days}D",
        inclusive="left",
    )
    return t_target
