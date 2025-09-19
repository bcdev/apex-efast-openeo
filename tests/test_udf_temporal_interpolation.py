import pytest

import xarray as xr
import numpy as np
from efast_openeo.algorithms.udf.udf_temporal_interpolation import apply_datacube


def test_temporal_interpolation_shape():
    t_start = "2022-09-01"
    t_end = "2022-09-30"
    t = xr.date_range(t_start, t_end, freq="1D")
    t_target = xr.date_range(t_start, t_end, freq="5D")
    n_bands = 2
    y, x = 3, 4

    data = np.arange(np.prod((len(t), n_bands, y, x)), dtype=np.float32).reshape(len(t), n_bands, y, x)
    band0 = np.tile([
        [1, 2, 3, 4],
        [0.1, 0.2, 0.3, 0.4],
        [0.01, 0.02, 0.03, 0.04],
    ], (len(t), 1,1))
    data[:, 0, :, :] = band0

    cube = xr.DataArray(data, dims=["t", "bands", "y", "x"], coords={"t": t})

    interpolated = apply_datacube(cube, {"t_target": t_target})

    assert interpolated.ndim == 4
    assert interpolated.t.shape == (len(t_target),)
    assert (interpolated.t == t_target).all()
    assert np.isclose(interpolated.isel(bands=0), band0[0]).all()
    assert np.logical_not(np.isclose(interpolated.isel(bands=1), data[0, 1]).all())
