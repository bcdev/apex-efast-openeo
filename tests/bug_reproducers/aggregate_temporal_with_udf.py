import openeo

def aoi_bounding_box():
    directions = ["west", "south", "east", "north"]
    bbox_list = [-15.456047, 15.665024, -15.425491, 15.687501]
    bbox = {d: c for (d, c) in zip(directions, bbox_list)}
    return bbox
AOI = aoi_bounding_box()

UDF = '''
import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube, inspect

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()

    res = xr.DataArray(
        array.sum(dim="t"),
        dims=["y", "x"],
    )

    return XarrayDataCube(res)
'''

TIME_FRAME = ["2022-09-20", "2022-09-25"]

def s3_cloud_mask(conn):
    cube = conn.load_collection("SENTINEL3_SYN_L2_SYN", temporal_extent=TIME_FRAME, spatial_extent=AOI, bands=["CLOUD_flags"])
    cloud_mask = cube.band("CLOUD_flags") > 0
    return cloud_mask

def main():
    conn = openeo.connect("https://openeo.dataspace.copernicus.eu/").authenticate_oidc()
    udf = openeo.UDF(UDF)
    cloud_mask = s3_cloud_mask(conn)
    time_frames = [
        ("2022-09-20", "2022-09-22"),
        ("2022-09-21", "2022-09-23"),
        ("2022-09-22", "2022-09-24"),
        ("2022-09-23", "2022-09-25"),
    ]
    time_centers = [
        "2022-09-21",
        "2022-09-22",
        "2022-09-23",
        "2022-09-24",
    ]
    agg = cloud_mask.aggregate_temporal(
        intervals=time_frames,
        reducer=udf,
        dimension="t",
        labels=time_centers,
    )
    print(agg.to_json())
    agg.download("agg.nc")

if __name__ == "__main__":
    main()
