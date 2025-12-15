import openeo


def aoi_bounding_box():
    directions = ["west", "south", "east", "north"]
    bbox_list = [-15.456047, 15.665024, -15.425491, 15.687501]
    bbox = {d: c for (d, c) in zip(directions, bbox_list)}
    return bbox


AOI = aoi_bounding_box()

TIME_FRAME = ["2022-09-07", "2022-09-27"]


def main():
    connection = openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc()
    s2_bands = ["B02", "B04", "B08"]
    s2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=s2_bands,
    )
    s3_cube = connection.load_collection(
        "SENTINEL3_SYN_L2_SYN",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["Syn_Oa08_reflectance"],
    )

    resampled = s2_cube.resample_cube_spatial(s3_cube)
    assert all([actual.name in s2_bands for actual in s2_cube.metadata.bands])  # okay
    assert all([actual.name in s2_bands for actual in resampled.metadata.bands]), (
        f"expected bands '{s2_bands}', found {resampled.metadata.bands}"
    )


if __name__ == "__main__":
    main()
