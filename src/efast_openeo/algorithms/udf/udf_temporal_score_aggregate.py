import numpy as np
import pandas as pd

import xarray as xr
from openeo.metadata import CubeMetadata
from datetime import datetime, timezone


EPS = 1e-5


def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    """
    Expects ``cube`` to be an array of dimensions (t, bands, y, x)
    """

    band_names = cube.get_index("bands")
    assert "bands" in cube.dims, f"cube must have a 'bands' dimension, found '{cube.dims}'"
    assert "distance_score" in band_names, f"Input cube must have a band 'distance_score' in addition to the input bands. Found bands '{band_names}'"
    assert "t_target" in context, f"The target time dimension 't_target' must be provided in the 'context' dict. Found keys '{context.keys()}' in 'context'."

    t_target = pd.DatetimeIndex([datetime.fromisoformat(t) for t in context["t_target"]])
    # TODO pass default properly
    sigma_doy = context.get("sigma_doy", 5)
    temporal_score = compute_temporal_score(cube.t, t_target, sigma_doy)
    distance_score = cube.sel(bands="distance_score")
    data_bands = cube.sel(bands=[b for b in band_names if b != "distance_score"])

    # alternative
    #combined = compute_combined_score(distance_score, temporal_score)
    #composite = xr.dot(combined, data_bands, dim="t")

    composite = _compute_combined_score_no_intermediates(distance_score, temporal_score, data_bands)

    renamed = composite.rename({"t_target": "t"})
    dims = ('t' ,'bands','y', 'x')
    return renamed.transpose(*dims)


def apply_metadata(metadata: CubeMetadata, context: dict) -> CubeMetadata:
    t_target = [datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in context["t_target"]]
    metadata = metadata.rename_labels(dimension="t", target=t_target)
    metadata = metadata.filter_bands([band.name for band in metadata.band_dimension.bands if band.name != "distance_score"])
    return metadata


def compute_temporal_score(t: pd.DatetimeIndex, t_target: pd.DatetimeIndex, sigma_doy: float):
    t_values = t.values.astype("datetime64[D]")
    t_target_values = t_target.values.astype("datetime64[D]")
    difference_matrix = t_values[:, np.newaxis] - t_target_values[np.newaxis, :]

    arr = np.exp(-0.5 * np.square(difference_matrix.astype(int))  / np.square(sigma_doy))
    return xr.DataArray(
        arr,
        coords={"t": t, "t_target": t_target},
        dims=["t", "t_target"],
    )


def compute_combined_score(distance_score: xr.DataArray, temporal_score: xr.DataArray) -> xr.DataArray:
    # equivalent to (distance_score * temporal_score).sum(dim="t")
    #return xr.dot(distance_score, temporal_score, dim="t")

    combined = distance_score * temporal_score
    # TODO remove EPS and use mask instead
    return combined / (combined.sum(dim="t") + EPS)


def _compute_combined_score_no_intermediates(distance_score: xr.DataArray, temporal_score: xr.DataArray, bands: xr.DataArray) -> xr.DataArray:
    res = xr.apply_ufunc(
        _compute_normalized_composite,
        distance_score, temporal_score, bands,
        input_core_dims=[['t', 'y', 'x'], ['t_target', 't'], ['t', 'bands', 'y', 'x']],
        output_core_dims=[['t_target', 'bands', 'y', 'x']],
        vectorize=True
    )
    return res

def _compute_normalized_composite(distance_score, temporal_score, bands, **kwargs):
    # TODO proper masking
    bands = np.where(np.isfinite(bands), bands, EPS)
    numerator = np.einsum('tyx,Tt,tbyx->Tbyx', distance_score, temporal_score, bands, **kwargs)
    # TODO consider deleting EPS
    normalization = np.einsum('tyx,Tt->Tyx', distance_score, temporal_score) + EPS
    res = numerator / np.expand_dims(normalization, 1)
    return res
