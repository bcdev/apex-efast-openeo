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

    band = np.ones_like(cloud_mask)
    bands_cube = xr.DataArray(band, dims=["t", "y", "x"], coords={"t": t}).expand_dims("bands")

    cube = xr.DataArray(np.stack([band, distance_score], axis=1), dims=["t", "bands", "y", "x"], coords={"t": t, "bands": ["band", "distance_score"]})

    dims = ("t", "bands", "y", "x")
    composite = apply_datacube(cube, {"t_target": t_target.strftime("%Y-%m-%d").to_list()})
    composite = composite.transpose(*dims)
    composite2 = _compute_combined_score_no_intermediates(distance_score, temporal_score, bands_cube).rename({"t_target": "t"})
    composite2 = composite2.transpose(*dims)

    assert composite.shape == composite2.shape
    assert np.isclose(composite.values, composite2.values).all()