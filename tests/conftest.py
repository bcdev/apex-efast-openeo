import os
from pathlib import Path

import pytest
import openeo


S2_COLLECTION = "SENTINEL2_L2A"
S3_COLLECTION = "SENTINEL3_SYN_L2_SYN"

TEST_OUTPUT_DIR_DEFAULT = "test_outputs"
TEST_OUTPUT_DIR_ENV_VAR = "TEST_OUTPUT_DIR"

@pytest.fixture
def persistent_output_dir_base() -> Path:
    leaf = Path(os.getenv(TEST_OUTPUT_DIR_ENV_VAR, TEST_OUTPUT_DIR_DEFAULT))
    # leaf is relative to the parent of "tests" which is the parent of contest.py
    path = Path(__file__).resolve().parent.parent / leaf
    path.mkdir(parents=True, exist_ok=True)
    return path

@pytest.fixture
def aoi_bounding_box():
    directions = ["west", "south", "east", "north"]
    bbox_list = [-15.456047, 15.665024, -15.425491, 15.687501]
    bbox = {d: c for (d, c) in zip(directions, bbox_list)}
    return bbox


@pytest.fixture
def time_frame():
    return ["2022-06-01", "2022-06-30"]


@pytest.fixture
def connection():
    return openeo.connect("https://openeo.dataspace.copernicus.eu/").authenticate_oidc()


@pytest.fixture
def s2_bands():
    return ["B02", "B03", "B04", "B8A", "SCL"]


@pytest.fixture
def s3_bands():
    return [
        "Syn_Oa04_reflectance",
        "Syn_Oa06_reflectance",
        "Syn_Oa08_reflectance",
        "Syn_Oa17_reflectance",
    ]


@pytest.fixture
def s2_cube(connection, aoi_bounding_box, time_frame, s2_bands):
    return connection.load_collection(
        S2_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=s2_bands,
    )

@pytest.fixture
def s3_cube(connection, aoi_bounding_box, time_frame, s3_bands):
    return connection.load_collection(
        S3_COLLECTION,
        spatial_extent=aoi_bounding_box,
        temporal_extent=time_frame,
        bands=s3_bands,
    )
