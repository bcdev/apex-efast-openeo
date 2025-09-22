import openeo

def aoi_bounding_box():
    directions = ["west", "south", "east", "north"]
    bbox_list = [-15.456047, 15.665024, -15.425491, 15.687501]
    bbox = {d: c for (d, c) in zip(directions, bbox_list)}
    return bbox

TIME_FRAME = ["2022-09-07", "2022-09-27"]


NOOP_UDF = """
def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    return cube
"""


def main():
    connection = openeo.connect("https://openeo.dataspace.copernicus.eu/").authenticate_oidc()
    s2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["SCL", "B02", "B04", "B08"],
    )
    scl = s2_cube.filter_bands(["SCL"])
    s3_cube = connection.load_collection(
        "SENTINEL3_SYN_L2_SYN",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["Syn_Oa08_reflectance"],
    )

    noop_udf = openeo.UDF(NOOP_UDF)
    image_size_pixels = 100
    border_pixels = 50
    scl_applied = scl.apply_neighborhood(
        noop_udf,
        size=[
            {"dimension": "t", "value": "P1D"},
            {"dimension": "x", "value": image_size_pixels, "unit": "px"},
            {"dimension": "y", "value": image_size_pixels, "unit": "px"},
        ],
        overlap=[
            {"dimension": "x", "value": border_pixels, "unit": "px"},
            {"dimension": "y", "value": border_pixels, "unit": "px"},
        ],
    )
    bands = s2_cube.filter_bands(["B02", "B04"])
    other_bands = s2_cube.filter_bands(["B08"])
    other_bands = other_bands.resample_cube_spatial(s3_cube)

    merged1 = scl_applied.merge_cubes(bands)
    merged2 = merged1.merge_cubes(other_bands)

    merged1.dimension_labels("t").download("merged1_time_labels.json")
    merged2.dimension_labels("t").download("merged2_time_labels.json")

    merged2.download("merged2.nc")


def main_with_workaround():
    connection = openeo.connect("https://openeo.dataspace.copernicus.eu/").authenticate_oidc()
    s2_cube_b08 = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["B08"],
    )
    s2_cube_bands = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["B02", "B04"],
    )
    scl = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["SCL"],
    )
    s3_cube = connection.load_collection(
        "SENTINEL3_SYN_L2_SYN",
        spatial_extent=aoi_bounding_box(),
        temporal_extent=TIME_FRAME,
        bands=["Syn_Oa08_reflectance"],
    )

    noop_udf = openeo.UDF(NOOP_UDF)

    image_size_pixels = 100
    border_pixels = 50
    scl_applied = scl.apply_neighborhood(
        noop_udf,
        size=[
            {"dimension": "t", "value": "P1D"},
            {"dimension": "x", "value": image_size_pixels, "unit": "px"},
            {"dimension": "y", "value": image_size_pixels, "unit": "px"},
        ],
        overlap=[
            {"dimension": "x", "value": border_pixels, "unit": "px"},
            {"dimension": "y", "value": border_pixels, "unit": "px"},
        ],
    )

    s2_cube_b08 = s2_cube_b08.resample_cube_spatial(s3_cube)
    merged1 = scl_applied.merge_cubes(s2_cube_bands)
    merged2 = merged1.merge_cubes(s2_cube_b08)

    merged1.dimension_labels("t").download("wa_merged1_time_labels.json")
    merged2.dimension_labels("t").download("wa_merged2_time_labels.json")

    #merged2.download("merged2.nc")

if __name__ == '__main__':
    #main()
    main_with_workaround()
