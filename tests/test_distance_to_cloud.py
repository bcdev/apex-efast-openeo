from pathlib import Path

import pytest

from efast_openeo.algorithms.distance_to_cloud import distance_to_cloud, cloud_mask_s2, cloud_mask_s3

CLOUD_BAND_S2 = "SCL"
CLOUD_BAND_S3 = "CLOUD_flags"

METRES_PER_PIXEL_S2 = 20
DEGREES_PER_PIXEL_S3 = 0.0027  # TODO check accuracy


@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "distance_to_cloud"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def image_size_pixels() -> int:
    return 100


@pytest.fixture
def overlap_size_pixels() -> int:
    return 50


################################################################################
# Sentinel-2
################################################################################


@pytest.mark.openeo
@pytest.mark.manual
def test_distance_to_cloud_s2_pixels(s2_cube, connection, persistent_output_dir, image_size_pixels, overlap_size_pixels,
                                     file_extension, execute):
    scl = s2_cube.band(CLOUD_BAND_S2)
    dtc = distance_to_cloud(cloud_mask_s2(scl), image_size_pixels, max_distance_pixels=overlap_size_pixels,
                            pixel_size_native_units=METRES_PER_PIXEL_S2)
    execute(dtc)((persistent_output_dir / "dtc_s2_pixels").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_distance_to_cloud_s2_metres(s2_cube, connection, persistent_output_dir, image_size_pixels, overlap_size_pixels,
                                     file_extension, execute):
    scl = s2_cube.band(CLOUD_BAND_S2)
    dtc = distance_to_cloud(cloud_mask_s2(scl), image_size_pixels,
                            max_distance_native_units=overlap_size_pixels * METRES_PER_PIXEL_S2,
                            pixel_size_native_units=METRES_PER_PIXEL_S2)
    execute(dtc)((persistent_output_dir / "dtc_s2_metres").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_download_input_cube_s2(s2_cube, connection, persistent_output_dir, file_extension):
    s2_cube.download((persistent_output_dir / "s2_input").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_download_cloud_mask(s2_cube, connection, persistent_output_dir, file_extension, execute):
    cloud_mask = cloud_mask_s2(s2_cube.band(CLOUD_BAND_S2))
    execute(cloud_mask)((persistent_output_dir / "cloud_mask_s2").with_suffix(file_extension))


################################################################################
# Sentinel-3
################################################################################

@pytest.mark.openeo
@pytest.mark.manual
def test_distance_to_cloud_s3_pixels(s3_cube, connection, persistent_output_dir, image_size_pixels, overlap_size_pixels,
                                     file_extension, execute):
    scl = s3_cube.band(CLOUD_BAND_S3)
    dtc = distance_to_cloud(cloud_mask_s3(scl), image_size_pixels, max_distance_pixels=overlap_size_pixels,
                            pixel_size_native_units=DEGREES_PER_PIXEL_S3)
    execute(dtc)((persistent_output_dir / "dtc_s3_pixels").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_distance_to_cloud_s3_degrees(s3_cube, connection, persistent_output_dir, image_size_pixels, overlap_size_pixels,
                                     file_extension, execute):
    scl = s3_cube.band(CLOUD_BAND_S3)
    dtc = distance_to_cloud(cloud_mask_s3(scl), image_size_pixels,
                            max_distance_native_units=overlap_size_pixels * DEGREES_PER_PIXEL_S3,
                            pixel_size_native_units=DEGREES_PER_PIXEL_S3)
    execute(dtc)((persistent_output_dir / "dtc_s3_metres").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_download_input_cube_s3(s3_cube, connection, persistent_output_dir, file_extension):
    s3_cube.download((persistent_output_dir / "s3_input").with_suffix(file_extension))


@pytest.mark.openeo
@pytest.mark.manual
def test_download_cloud_mask(s3_cube, connection, persistent_output_dir, file_extension, execute):
    cloud_mask = cloud_mask_s3(s3_cube.band(CLOUD_BAND_S3))
    execute(cloud_mask)((persistent_output_dir / "cloud_mask_s3").with_suffix(file_extension))
