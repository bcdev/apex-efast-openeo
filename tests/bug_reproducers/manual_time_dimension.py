import openeo


def aoi_bounding_box():
    directions = ["west", "south", "east", "north"]
    bbox_list = [-15.456047, 15.665024, -15.425491, 15.687501]
    bbox = {d: c for (d, c) in zip(directions, bbox_list)}
    return bbox


TIME_FRAME = ["2022-09-01", "2022-09-02"]


def main():
    connection = openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc()
    s2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["SCL"],
    )
    dropped = s2_cube.drop_dimension("t")
    dropped = dropped.drop_dimension("bands")
    with_manual_time_a = dropped.add_dimension(
        "t", "2025-06-06T00:00:00.000Z", type="temporal"
    )
    with_manual_time_b = dropped.add_dimension(
        "t", "2025-06-07T00:00:00.000Z", type="temporal"
    )
    merged = with_manual_time_a.merge_cubes(
        with_manual_time_b
    )  # , overlap_resolver="sum")
    print(merged.to_json())
    merged.download("manual_time_steps.nc")


if __name__ == "__main__":
    main()
