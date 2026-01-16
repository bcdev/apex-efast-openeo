import xarray as xr
import numpy as np
from openeo.metadata import CubeMetadata


def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    """
    Computes the main fusion procedure, equation (3) in [1].

    [1]: Senty, Paul, Radoslaw Guzinski, Kenneth Grogan, et al. “Fast Fusion of Sentinel-2 and Sentinel-3 Time Series over Rangelands.” Remote Sensing 16, no. 11 (2024): 11. https://doi.org/10.3390/rs16111833.
    """
    assert "bands" in cube.dims, (
        f"cube must have a 'bands' dimension, found '{cube.dims}'"
    )
    assert "hr_mosaic_bands" in context, (
        f"The high resolution mosaic bands 'hr_mosaic_bands' must be provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."
    )
    assert "lr_mosaic_bands" in context, (
        f"The low resolution bands 'lr_mosaic_bands' must be provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."
    )
    assert "lr_interpolated_band_name_suffix" in context, (
        "The suffix differentiating low resolution interpolated bands from composited bands "
        f"'lr_interpolated_band_name_suffix' provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."
    )

    hr_mosaic_bands = context["hr_mosaic_bands"]
    lr_mosaic_bands = context["lr_mosaic_bands"]
    interpolated_band_suffix = context["lr_interpolated_band_name_suffix"]
    lr_interpolated_bands = [f"{b}{interpolated_band_suffix}" for b in lr_mosaic_bands]
    target_bands = context.get("target_bands")
    if target_bands is None or len(target_bands) != len(hr_mosaic_bands):
        target_bands = hr_mosaic_bands
    output_ndvi = context.get("output_ndvi", False)

    fused = fuse(
        cube, hr_mosaic_bands, lr_mosaic_bands, lr_interpolated_bands, target_bands
    )
    if output_ndvi:
        nir = cube.sel(bands="B8A")
        red = cube.sel(bands="B04")
        ndvi = (nir - red) / (nir + red)
        # TODO may break on older version of xarray
        ndvi_formatted = ndvi.expand_dims({"bands": ["ndvi"]}, axis=fused.dims.index("bands"))
        return ndvi_formatted

    return fused


def apply_metadata(metadata: CubeMetadata, context: dict) -> CubeMetadata:
    target_bands = context.get("target_bands", context["hr_mosaic_bands"])
    output_ndvi = context.get("output_ndvi", False)
    if output_ndvi:
        target_bands = ["ndvi"]
    metadata = metadata.rename_labels(dimension="bands", target=target_bands)
    return metadata


def fuse(cube, hr_mosaic_bands, lr_mosaic_bands, lr_interpolated_bands, target_bands):
    fused_list = [
        # Pixels where there is no data in either lr_m or lr_p are skipped, to avoid
        # only adding or only subtracting from the S2 composite, which would result in unusually high or low
        # (e.g. negative values).
        xr.where(
            (cube.sel(bands=lr_m) == 0)
            | np.isnan(cube.sel(bands=lr_m))
            | (cube.sel(bands=lr_p) == 0)
            | np.isnan(cube.sel(bands=lr_p)),
            cube.sel(bands=hr_m).squeeze(),
            cube.sel(bands=[hr_m, lr_p]).sum(dim="bands")
            - cube.sel(bands=lr_m).squeeze(),
        )
        for (hr_m, lr_m, lr_p) in zip(
            hr_mosaic_bands, lr_mosaic_bands, lr_interpolated_bands
        )
    ]

    fused = xr.concat(fused_list, dim="bands")
    fused = fused.assign_coords(bands=target_bands)
    return fused
