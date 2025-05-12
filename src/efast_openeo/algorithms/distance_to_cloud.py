import importlib

import openeo
import warnings

from efast_openeo.constants import S2Scl, S3SynCloudFlags

UDF_DISTANCE_TRANSFORM_PATH = importlib.resources.files("efast_openeo.algorithms.udf").joinpath("udf_distance_transform.py")

# TODO move
def cloud_mask_s2(s2_scl: openeo.DataCube) -> openeo.DataCube:
    return s2_scl > S2Scl.WATER

# TODO move
def cloud_mask_s3(s3_scl: openeo.DataCube) -> openeo.DataCube:
    return s3_scl > S3SynCloudFlags.CLEAR


def distance_to_cloud_s2(cloud_mask: openeo.DataCube, image_size_pixels=512, max_distance_pixels=255):
    """
    Computes the distance to the closest cloud
    """
    # TODO convert pixels to metres
    warnings.warn("Not yet implemented")
    return euclidean_distance_transform(cloud_mask, image_size_pixels, max_distance_pixels)


def distance_to_cloud_s3(cloud_mask: openeo.DataCube, image_size_pixels=512, max_distance_pixels=255):
    """
    Computes the distance to the closest cloud
    """
    # TODO convert pixels to metres
    warnings.warn("Not yet implemented")
    return euclidean_distance_transform(cloud_mask, image_size_pixels, max_distance_pixels)


def euclidean_distance_transform(band: openeo.DataCube, image_size_pixels, border_pixels) -> openeo.DataCube:
    """
    Computes the distance (in pixels) to the closest background pixel value of ``False``.
    The distance is computed with a border of ``border_pixels`` pixels around the region of interest
    (of size ``image_size_pixels``).

    This means, the maximum possible distance to be computed is ``border_pixels + image_size_pixels - 1``.
    from a pixel of interest (``False``) situated on one edge of the border to the edge of the image (without border)
    on the opposite side.
    """
    # TODO dummy implementation
    udf = openeo.UDF.from_file(UDF_DISTANCE_TRANSFORM_PATH)
    dt = band.apply_neighborhood(
        udf,
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
    return dt
