import pytest
from pathlib import Path

@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "udp_from_process_graph"
    path.mkdir(parents=True, exist_ok=True)
    return path

@pytest.mark.manual
@pytest.mark.openeo
def test_create_and_parameterize_udp(connection, execute, file_extension, aoi_bounding_box, persistent_output_dir):
    cube = connection.datacube_from_process(
        "efast",
        namespace="https://raw.githubusercontent.com/bcdev/efast-process-graph/refs/heads/hn-validation/process_graph.json",
        s2_data_bands=["B04", "B8A"],
        s3_data_bands=["SDR_Oa08", "SDR_Oa17"],
        temporal_extent=["2023-09-01", "2023-09-07"],
        temporal_extent_target=["2023-09-02", "2023-09-06"],
        spatial_extent=aoi_bounding_box,
        interval_days=3,
        output_ndvi=True,
    )
    execute(cube)(
        (persistent_output_dir / "s3_composite").with_suffix(file_extension)
    )
