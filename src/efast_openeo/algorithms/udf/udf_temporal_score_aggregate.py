import numpy as np
import pandas as pd

import xarray as xr
from openeo.metadata import CubeMetadata
from datetime import datetime, timezone


EPS = 1e-5


def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    """
    Computes a composite time series. The input time series is converted to the time series passed
    as ``"t_target"`` in ``context``, by computing a weighted sum of input images for each time step in
    ``t_target``. The inputs are weighted by their temporal distance to the target time step and by the distance to
    cloud score (the ``"distance_score"`` band of the inputs).

    Expects ``cube`` to be an array of dimensions (t, bands, y, x)
    """
    if context.get("use_stepwise_aggregation", False):
        return apply_datacube_stepwise(cube, context)

    band_names = cube.get_index("bands")
    assert "bands" in cube.dims, (
        f"cube must have a 'bands' dimension, found '{cube.dims}'"
    )
    assert "distance_score" in band_names, (
        f"Input cube must have a band 'distance_score' in addition to the input bands. Found bands '{band_names}'"
    )

    sigma_doy = context["sigma_doy"]
    t_target = get_t_target_from_context(context)
    temporal_score = compute_temporal_score(cube.t, t_target, sigma_doy)
    distance_score = cube.sel(bands="distance_score")
    data_bands = cube.sel(bands=[b for b in band_names if (b != "distance_score" and b != "CLOUD_flags")])

    composite = _compute_combined_score_no_intermediates(
        distance_score, temporal_score, data_bands
    )

    renamed = composite.rename({"t_target": "t"})
    dims = ("t", "bands", "y", "x")
    return renamed.transpose(*dims)


def apply_metadata(metadata: CubeMetadata, context: dict) -> CubeMetadata:
    t_target = get_t_target_from_context(context)
    t_target_str = [d.isoformat() for d in t_target.to_pydatetime()]

    metadata = metadata.rename_labels(dimension="t", target=t_target_str)
    metadata = metadata.filter_bands(
        [
            band.name
            for band in metadata.band_dimension.bands
            if (band.name != "distance_score" and band.name != "CLOUD_flags")
        ]
    )
    return metadata


def compute_temporal_score(
    t: pd.DatetimeIndex, t_target: pd.DatetimeIndex, sigma_doy: float
) -> xr.DataArray:
    """
    Compute the temporal weight for each input and output time step.
    Generates a two-dimensional score, mapping input time steps to output time steps (``len(t) * len(t_target)`` entries).

    :param t: time stamps of the input time series
    :param t_target: target time stamps for which the composites are to be computed
    :param sigma_doy: standard deviation of the gaussian window used for temporal weighting
    """
    t_values = t.values.astype("datetime64[D]")
    t_target_values = t_target.values.astype("datetime64[D]")
    difference_matrix = t_values[:, np.newaxis] - t_target_values[np.newaxis, :]

    arr = np.exp(-0.5 * np.square(difference_matrix.astype(int)) / np.square(sigma_doy))
    return xr.DataArray(
        arr,
        coords={"t": t, "t_target": t_target},
        dims=["t", "t_target"],
    )


def compute_combined_score(
    distance_score: xr.DataArray, temporal_score: xr.DataArray
) -> xr.DataArray:
    # equivalent to (distance_score * temporal_score).sum(dim="t")
    # return xr.dot(distance_score, temporal_score, dim="t")

    combined = distance_score * temporal_score
    # TODO remove EPS and use mask instead
    return combined / (combined.sum(dim="t") + EPS)


def _compute_combined_score_no_intermediates(
    distance_score: xr.DataArray, temporal_score: xr.DataArray, bands: xr.DataArray
) -> xr.DataArray:
    res = xr.apply_ufunc(
        _compute_normalized_composite,
        distance_score,
        temporal_score,
        bands,
        input_core_dims=[["t", "y", "x"], ["t_target", "t"], ["t", "bands", "y", "x"]],
        output_core_dims=[["t_target", "bands", "y", "x"]],
        vectorize=True,
    )
    return res


def _compute_normalized_composite(distance_score, temporal_score, bands, **kwargs):
    """
    Compute the combined distance-to-cloud and temporal score and the weighted sum applying the score by pixel and
    input/target time stamp to generate the composites
    """
    score = np.einsum("tyx,Tt->Ttyx", distance_score, temporal_score)
    # consider pixels as not-observed if the first band has a nan value
    score_masked = np.where(np.isnan(bands[:, 0, ...]), 0, score)

    normalization_flat = np.sum(score_masked, axis=1)
    normalization = normalization_flat[:, np.newaxis, ...]
    score_normalized = score_masked / normalization

    finite_bands = np.where(np.isfinite(bands), bands, 0)
    weighted_composite = np.einsum(
        "Ttyx,tbyx->Tbyx", score_normalized, finite_bands
    )  # original

    no_data_mask = (normalization_flat == 0)[:, np.newaxis, ...] | (
        weighted_composite <= 0
    )
    weighted_composite_masked = np.where(no_data_mask, np.nan, weighted_composite)
    return weighted_composite_masked


def compute_t_target(temporal_extent, interval_days) -> pd.DatetimeIndex:
    t_target = xr.date_range(
        temporal_extent[0],
        temporal_extent[1],
        freq=f"{interval_days}D",
        inclusive="left",
    )
    return t_target


def apply_datacube_stepwise(cube: xr.DataArray, context: dict) -> xr.DataArray:
    band_names = cube.get_index("bands")
    assert "bands" in cube.dims, (
        f"cube must have a 'bands' dimension, found '{cube.dims}'"
    )
    assert "distance_score" in band_names, (
        f"Input cube must have a band 'distance_score' in addition to the input bands. Found bands '{band_names}'"
    )

    sigma_doy = context["sigma_doy"]
    t_target = get_t_target_from_context(context)
    temporal_score = compute_temporal_score(cube.t, t_target, sigma_doy)
    distance_score = cube.sel(bands="distance_score")
    data_bands = cube.sel(bands=[b for b in band_names if b != "distance_score"])

    composite = _compute_combined_score_ng(distance_score, temporal_score, data_bands)

    renamed = composite.rename({"t_target": "t"})
    dims = ("t", "bands", "y", "x")
    return renamed.transpose(*dims)


def _compute_combined_score_ng(distance_score, temporal_score, bands):
    # TODO make mosaic_days a parameter
    mosaic_days = 100
    composites = {}

    # TODO convert to "rolling"?
    for middle_date in temporal_score.t_target:
        window_start = middle_date - pd.Timedelta(days=mosaic_days / 2)
        window_end = middle_date + pd.Timedelta(days=mosaic_days / 2)

        windowed_bands = bands.sel(t=slice(window_start, window_end))
        windowed_bands = xr.where(
            np.abs(windowed_bands.mean(dim="t")) < 5, windowed_bands, np.nan
        )
        windowed_distance_score = distance_score.sel(t=slice(window_start, window_end))
        windowed_temporal_score = temporal_score.sel(
            t=slice(window_start, window_end), t_target=middle_date
        )

        score = windowed_distance_score * windowed_temporal_score
        score_nan_masked = xr.where(np.isnan(windowed_bands.isel(bands=0)), 0, score)
        # score_nan_masked = score
        normalizing_coefficient = score_nan_masked.sum(dim="t")# + 1e-5
        normalized_score = score_nan_masked / normalizing_coefficient

        weighted_bands = normalized_score * windowed_bands
        composite = weighted_bands.sum(skipna=True, dim="t")
        composite = xr.where(normalized_score.sum(dim="t") == 0 | (composite <= 0), np.nan, composite)
        composites[pd.to_datetime(middle_date.item()).strftime("%Y-%m-%d")] = composite
    composite_da = xr.concat(
        [composites[t_target] for t_target in composites],
        dim=xr.IndexVariable("t_target", temporal_score.t_target),
    )
    return composite_da


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
