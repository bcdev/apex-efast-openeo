import pytest
from scipy.ndimage import distance_transform_edt

import xarray as xr
import numpy as np
from efast_openeo.algorithms.udf.udf_temporal_score_aggregate import compute_temporal_score, compute_combined_score, \
    _compute_combined_score_no_intermediates, apply_datacube


def test_temporal_score_shape():
    t_start = "2022-09-01"
    t_end = "2022-09-30"
    t = xr.date_range(t_start, t_end, freq="5D")
    t_target = xr.date_range(t_start, t_end, freq="2D")

    score = compute_temporal_score(t, t_target, 5)
    assert score.ndim == 2
    assert score.t.shape == (len(t),)
    assert score.t_target.shape == (len(t_target),)

def test_combined_score_shape():
    t_start = "2022-09-01"
    t_end = "2022-09-30"
    t = xr.date_range(t_start, t_end, freq="5D")
    t_target = xr.date_range(t_start, t_end, freq="2D")

    days, y_len, x_len = len(t), 11, 10
    pixel_size_m = 20
    D = 20 * 20

    cloud_mask = np.zeros((days, y_len, x_len), dtype=bool)
    for i in range(days):
        cloud_mask[i, :, i] = True

    temporal_score = compute_temporal_score(t, t_target, 5)
    dtc_score_raw = np.zeros_like(cloud_mask, dtype=float)
    for day in range(days):
        dtc_score_raw[day, :, :] = distance_transform_edt(np.logical_not(cloud_mask[day, ...])) * pixel_size_m
    dtc_score = xr.DataArray(np.clip((dtc_score_raw - 1) / D, 0, 1), dims=["t", "y", "x"], coords={"t": t})

    combined_score = dtc_score * temporal_score
    #assert combined_score.dims == ["t", "t_target", "y", "x"]
    assert all([dim in combined_score.dims for dim in ["t", "t_target", "y", "x"]])


def test_combined_score_variants_produce_equal_results():
    t_start = "2022-09-01"
    t_end = "2022-09-04"
    t = xr.date_range(t_start, t_end, freq="2D")
    t_target = xr.date_range(t_start, t_end, freq="1D")
    temporal_score = compute_temporal_score(t, t_target, 5)

    days, y_len, x_len = len(t), 4, 5
    pixel_size_m = 20
    D = 20 * 20

    cloud_mask = np.zeros((days, y_len, x_len), dtype=bool)
    for i in range(days):
        cloud_mask[i, :, i] = True

    dtc_score_raw = np.zeros_like(cloud_mask, dtype=float)
    for day in range(days):
        dtc_score_raw[day, :, :] = distance_transform_edt(np.logical_not(cloud_mask[day, ...])) * pixel_size_m
    distance_score = xr.DataArray(np.clip((dtc_score_raw - 1) / D, 0, 1), dims=["t", "y", "x"], coords={"t": t})

    combined_score = compute_combined_score(distance_score, temporal_score)

    band = np.ones_like(cloud_mask, dtype=float)
    band_nan = np.full_like(band, fill_value=np.nan)

    bands_cube = xr.DataArray(np.stack((band, band_nan), axis=1), dims=["t", "bands", "y", "x"], coords={"t": t})

    cube = xr.DataArray(np.stack([band, band_nan, distance_score], axis=1), dims=["t", "bands", "y", "x"], coords={"t": t, "bands": ["band1", "band2", "distance_score"]})

    dims = ("t", "bands", "y", "x")
    composite = apply_datacube(cube, {"t_target": t_target.strftime("%Y-%m-%d").to_list()})
    composite = composite.transpose(*dims)
    composite2 = _compute_combined_score_no_intermediates(distance_score, temporal_score, bands_cube).rename({"t_target": "t"})
    composite2 = composite2.transpose(*dims)

    assert composite.shape == composite2.shape
    assert np.isclose(composite.values, composite2.values).all()


def test_composite_masking():
    t_start = "2022-09-01"
    t_end = "2022-09-04"
    t = xr.date_range(t_start, t_end, freq="2D")
    t_target = xr.date_range(t_start, t_end, freq="1D")

    days, y_len, x_len = len(t), 4, 5
    pixel_size_m = 10
    D = 10 * 10

    band = np.ones(shape=(days, y_len, x_len), dtype=float)

    band_nan_idx = (
        np.array([0, 0, 1]),
        np.array([3, 1, 1]),
        np.array([3, 1, 1]),
    )
    band[band_nan_idx] = np.nan

    # all pixels on left border are cloudy
    dtc_score_raw = np.tile(np.arange(x_len), reps=(days, y_len, 1)) * pixel_size_m
    distance_score = xr.DataArray(np.clip((dtc_score_raw - 1) / D, 0, 1), dims=["t", "y", "x"], coords={"t": t})

    cube = xr.DataArray(np.stack([band, band, distance_score], axis=1), dims=["t", "bands", "y", "x"], coords={"t": t, "bands": ["band1", "band2", "distance_score"]})

    composite = apply_datacube(cube, {"t_target": t_target.strftime("%Y-%m-%d").to_list()})
    assert composite.sizes["t"] == len(t_target)
    # All inputs at y=1, x=1 are NaN, so the composite should also be NaN
    assert np.isnan(composite.isel(y=1, x=1)).all()
    # Only one of the inputs at y=3, x=3 is NaN, so the output should not be NaN
    assert np.logical_not(np.isnan(composite.isel(y=3, x=3))).any()
    # All pixel time series should either be NaN (no input) or sum up to 1 for each time step, because the input is either 1 or NaN
    # When summing over time, we should receive the number of time steps as a value for each pixel, as each input to the sum is 1
    nan_mask = np.isnan(composite)
    close_to_number_of_time_steps = np.isclose(composite.sum(dim="t").isel(bands=0), composite.sizes["t"], atol=0.1)
    assert (nan_mask | close_to_number_of_time_steps).all()
