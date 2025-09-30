import xarray as xr
import numpy as np
from openeo.metadata import CubeMetadata


def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    assert "bands" in cube.dims, f"cube must have a 'bands' dimension, found '{cube.dims}'"
    assert "hr_mosaic_bands" in context, f"The high resolution mosaic bands 'hr_mosaic_bands' must be provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."
    assert "lr_mosaic_bands" in context, f"The low resolution bands 'lr_mosaic_bands' must be provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."
    assert "lr_interpolated_bands" in context, f"The bands of the low resolution interpolated cube 'lr_interpolated_bands' must be provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."

    hr_mosaic_bands = context["hr_mosaic_bands"]
    lr_mosaic_bands = context["lr_mosaic_bands"]
    lr_interpolated_bands = context["lr_interpolated_bands"]
    target_bands = context.get("target_bands", hr_mosaic_bands)

    fused = fuse(cube, hr_mosaic_bands, lr_mosaic_bands, lr_interpolated_bands, target_bands)

    return fused


def apply_metadata(metadata: CubeMetadata, context: dict) -> CubeMetadata:
    target_bands = context.get("target_bands", context["hr_mosaic_bands"])
    metadata = metadata.rename_labels(dimension="bands", target=target_bands)
    return metadata


def fuse(cube, hr_mosaic_bands, lr_mosaic_bands, lr_interpolated_bands, target_bands):

    fused_list = [
        xr.where(
            (cube.sel(bands=lr_m) == 0) | np.isnan(cube.sel(bands=lr_m)) |
            (cube.sel(bands=lr_p) == 0) | np.isnan(cube.sel(bands=lr_p))
        ,
        cube.sel(bands=hr_m).squeeze(),
        cube.sel(bands=[hr_m, lr_p]).sum(dim="bands") - cube.sel(bands=lr_m).squeeze())
        for (hr_m, lr_m, lr_p) in zip(hr_mosaic_bands, lr_mosaic_bands, lr_interpolated_bands)
    ]

    fused = xr.concat(fused_list, dim="bands")
    fused = fused.assign_coords(bands=target_bands)
    return fused
