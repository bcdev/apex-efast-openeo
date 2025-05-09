from pathlib import Path

import pytest

from efast_openeo.algorithms.distance_to_cloud import distance_to_cloud_s2


# Fixtures
@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "distance_to_cloud_s2"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.mark.openeo
@pytest.mark.manual
def test_distance_to_cloud_s2(s2_cube, connection, persistent_output_dir):
    scl = s2_cube.band("SCL")
    dtc = distance_to_cloud_s2(scl)
    print("downloading to",  persistent_output_dir / "dtc.tif")
    dtc.download(persistent_output_dir / "dtc.tif")

