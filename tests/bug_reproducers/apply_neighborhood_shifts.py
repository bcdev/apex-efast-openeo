import openeo

def aoi_bounding_box():
    directions = ["west", "south", "east", "north"]
    bbox_list = [-15.456047, 15.665024, -15.425491, 15.687501]
    bbox = {d: c for (d, c) in zip(directions, bbox_list)}
    return bbox
AOI = aoi_bounding_box()

NOOP_UDF = """
def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    return cube
"""

TIME_FRAME = ["2022-09-26", "2022-09-26"]

def s2_cloud_mask(conn):
    cube = conn.load_collection("SENTINEL2_L2A", temporal_extent=TIME_FRAME, spatial_extent=AOI)
    cloud_mask = cube.band("SCL") > 7
    return cloud_mask

def main():
    conn = openeo.connect("https://openeo.dataspace.copernicus.eu/").authenticate_oidc()
    noop_udf = openeo.UDF(NOOP_UDF)
    cloud_mask = s2_cloud_mask(conn)
    # overlap = 50 # works fine
    overlap = 51 # fails with error
    #overlap = 200 # 150, 200, ... gives incorrect result
    udf_result = cloud_mask.apply_neighborhood(
        noop_udf,
        size=[
            {"dimension": "t", "value": "P1D"},
            {"dimension": "x", "value": 50, "unit": "px"},
            {"dimension": "y", "value": 50, "unit": "px"},
        ],
        overlap=[
            {"dimension": "x", "value": overlap, "unit": "px"},
            {"dimension": "y", "value": overlap, "unit": "px"},
        ],
    )
    cloud_mask.download("cloud_mask.nc")
    udf_result.download("udf_result.nc")

if __name__ == "__main__":
    main()