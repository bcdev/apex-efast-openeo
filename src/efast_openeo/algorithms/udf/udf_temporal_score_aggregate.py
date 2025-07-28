import numpy as np
import xarray as xr
from openeo.metadata import CubeMetadata
from openeo.udf import XarrayDataCube, inspect
from datetime import datetime, timezone

def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    """
    assumes cube to be a slice shapes (t, bands) to be reduced into a single value
    """

    inspect(cube.shape)
    t_target = [datetime.fromisoformat(t) for t in context["t_target"]]
    selected = cube.isel(t=range(len(t_target)))
    inspect(selected.shape)
    inspect(None, f"after selection: {selected.shape}", "warning")

    res = xr.DataArray(
        selected,
        dims=["t", "bands", "y", "x"],
        coords={
            "t": t_target
        }
    )

    return res

def apply_metadata(metadata: CubeMetadata, context: dict) -> CubeMetadata:
    t_target = [datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in context["t_target"]]
    return metadata.rename_labels(dimension="t", target=t_target)


def compute_weights(dtc, t_target):
    pass