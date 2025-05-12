from pathlib import Path

import pytest

from efast_openeo.algorithms.distance_to_cloud import distance_to_cloud_s2, cloud_mask_s2


# Fixtures
@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "distance_to_cloud_s2"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.mark.openeo
@pytest.mark.manual
def test_distance_to_cloud_s2(s2_cube, connection, persistent_output_dir, file_extension, execute):
    scl = s2_cube.band("SCL")
    dtc = distance_to_cloud_s2(cloud_mask_s2(scl), 100, 50)
    execute(dtc)((persistent_output_dir / "dtc").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_download_input_cube(s2_cube, connection, persistent_output_dir, file_extension):
    s2_cube.download((persistent_output_dir / "input").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_cloud_mask(s2_cube, connection, persistent_output_dir, file_extension, execute):
    cloud_mask = cloud_mask_s2(s2_cube.band("SCL"))

    execute(cloud_mask)((persistent_output_dir / "cloud_mask").with_suffix(file_extension))