import os
from pathlib import Path
import tempfile
import json

import pytest
import openeo
import pandas as pd


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
def file_extension():
    return ".nc"
    # return ".nc"


@pytest.fixture
def download_style():
    return "execute_batch"


@pytest.fixture
def execute(download_style):
    def wrapper(dc):
        return getattr(dc, download_style)

    return wrapper


@pytest.fixture
def run_openeo(execute, file_extension):
    def run(cube, target):
        execute(cube)(target.with_suffix(file_extension))

    return run


@pytest.fixture
def aoi_bounding_box():
    directions = ["west", "south", "east", "north"]
    bbox_list = [-15.456047, 15.665024, -15.425491, 15.687501]
    # bbox_list = [-15.456047, 15.665024, -16.0, 16.0]
    bbox = {d: c for (d, c) in zip(directions, bbox_list)}
    return bbox


@pytest.fixture
def time_frame():
    # return ["2022-06-03", "2022-06-03"]
    # return ["2022-09-22", "2022-09-26"] # Interesting cloud/no cloud pattern in S3
    # return ["2022-09-26", "2022-09-27"] # single day with cloudy s2 observation
    return ["2022-09-07", "2022-09-27"]


@pytest.fixture
def interval_days():
    return 3


@pytest.fixture
def time_frame_target(time_frame):
    return time_frame


@pytest.fixture
def s2_dim_labels(s2_cube):
    dim_labels_path = tempfile.NamedTemporaryFile(delete_on_close=False)
    dim_labels_openeo = s2_cube.dimension_labels("t")
    # TODO does not have to be written to a file
    dim_labels_openeo.download(dim_labels_path.name)
    with open(dim_labels_path.name) as fp:
        dim_labels = json.load(fp)
    dim_labels_path.close()
    return dim_labels


@pytest.fixture
def s2_time_series(s2_dim_labels):
    dt = pd.to_datetime(s2_dim_labels)
    # TODO see if Z is necessary
    iso_date_time_strings = [f"{d.isoformat()}" for d in dt.to_pydatetime()]
    return iso_date_time_strings


@pytest.fixture
def connection(capsys):
    with capsys.disabled():
        # conn = openeo.connect("http://cate:8080").authenticate_oidc()
        # conn =  openeo.connect("http://localhost:8080").authenticate_oidc()
        conn = openeo.connect(
            "https://openeo.dataspace.copernicus.eu/"
        ).authenticate_oidc()
    return conn


@pytest.fixture
def s2_bands():
    return ["SCL", "B02", "B03"]
    # return ["B02", "B03", "B04", "B8A", "SCL"]


@pytest.fixture
def s3_bands():
    return [
        # "Syn_Oa04_reflectance",
        # "Syn_Oa06_reflectance",
        "Syn_Oa08_reflectance",
        "Syn_Oa17_reflectance",
        "CLOUD_flags",
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


@pytest.fixture
def image_size_pixels() -> int:
    return 100


@pytest.fixture
def overlap_size_pixels() -> int:
    return 50


@pytest.fixture()
def dtc_max_distance() -> float:
    return 400
