import pytest
from scipy.ndimage import distance_transform_edt

import xarray as xr
import numpy as np
from efast_openeo.algorithms.udf.udf_fusion import apply_datacube, fuse


def test_temporal_score_shape():
    t_start = "2022-09-01"
    t_end = "2022-09-30"
    t = xr.date_range(t_start, t_end, freq="5D")
    t_target = xr.date_range(t_start, t_end, freq="2D")

    score = compute_temporal_score(t, t_target, 5)
    assert score.ndim == 2
    assert score.t.shape == (len(t),)
    assert score.t_target.shape == (len(t_target),)


def test_fuse():
    t_start = "2022-09-01"
    t_end = "2022-09-30"
    t = xr.date_range(t_start, t_end, freq="5D")
    hr_mosaic_bands = ["HRM1", "HRM2"]
    lr_mosaic_bands = ["LRM1", "LRM2"]
    lr_interp_bands = ["LRP1", "LRP3"]
    target_bands = ["TGT1", "TGT3"]

    input_bands = hr_mosaic_bands + lr_mosaic_bands + lr_interp_bands
    n_x = 2
    n_y = 3

    data = np.zeros((len(t), len(input_bands), n_y, n_x))
    cube = xr.DataArray(
        data,
        coords={
            "t": t,
            "bands": input_bands,
        },
        dims=["t", "bands", "y", "x"],
    )
    hr_m_val = 10
    lr_m_val = 1
    lr_interp_val = 2
    cube.loc[:, hr_mosaic_bands, ...] = hr_m_val
    cube.loc[:, lr_mosaic_bands, ...] = lr_m_val
    cube.loc[:, lr_interp_bands, ...] = lr_interp_val

    fused = fuse(cube, hr_mosaic_bands, lr_mosaic_bands, lr_interp_bands, target_bands)

    assert len(fused["bands"]) == len(target_bands)
    assert all([b in target_bands for b in fused["bands"]])
    target_value = lr_interp_val + hr_m_val - lr_m_val
    assert (fused.sel(bands=target_bands) == target_value).all()
