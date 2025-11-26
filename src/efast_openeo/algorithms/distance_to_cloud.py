import importlib

import openeo

from efast_openeo.constants import S2Scl, S3SynCloudFlags

UDF_DISTANCE_TRANSFORM_PATH = importlib.resources.files("efast_openeo.algorithms.udf").joinpath("udf_distance_transform.py")

# TODO move
def compute_cloud_mask_s2(s2_scl: openeo.DataCube) -> openeo.DataCube:
    #return s2_scl > S2Scl.WATER
    return (s2_scl == S2Scl.NO_DATA) | (s2_scl == S2Scl.CLOUD_SHADOW) | (s2_scl > S2Scl.UNCLASSIFIED)

# TODO move
def compute_cloud_mask_s3(s3_scl: openeo.DataCube) -> openeo.DataCube:
    return s3_scl > S3SynCloudFlags.CLEAR


def distance_to_cloud(cloud_mask: openeo.DataCube, image_size_pixels: int, *, max_distance_pixels: int | None=None, pixel_size_native_units: int | float | None=None, max_distance_native_units: int | float | None=None):
    """
    Compute the distance to cloud on a binary ``cloud_mask``. Distance is computed for all ``False`` pixels to all ``True`` pixels.
    The maximum distance returned by this function can be specified either in pixels, using ``max_distance_pixels``
    or using the native units (degrees, m, ...) of the grid, using ``max_distance_native_units``.
    Only one of the two parameters may be specified. If given in native units, ``pixel_size_native_units`` must
    also be specified.

    The distance to cloud is returned either as a distance in pixels or as a distance in native units.
    Native units are chosen if ``pixel_size_native_units`` is specified.

    :param cloud_mask: The cloud mask (``True`` means cloud)
    :param image_size_pixels: Chunk size for the computation. Should be larger than ``max_distance_pixels``.
    :param max_distance_pixels: Maximum cloud distance that can be detected, given as the number of pixels from the cloud.
    :param max_distance_native_units: Maximum cloud distance that can be detected in the native units of the raster.
    :param pixel_size_native_units: Length of one pixel in the raster in its native units, assumed to be constant.
        If this parameter is specified, the distance to cloud is returned in native units.

    :return distance the nearest cloud (value of ``True`` in ``cloud_mask`` for each pixel that is ``False`` in
        ``cloud_mask``, either in native units (if ``pixel_size_native_units`` is set) or in pixels otherwise.
    """

    assert (max_distance_pixels is None) ^ (max_distance_native_units is None), (
        "Pixel size must be specified either in pixels or in native units, not both. "
        f"Found {max_distance_pixels=}, {max_distance_native_units=}"
    )
    if max_distance_native_units is not None:
        assert (pixel_size_native_units is not None) and (pixel_size_native_units > 0), (
            "pixel_size_in_native_units must be larger than 0 and specified if max_distance_in_native_units is set."
        )
        max_distance_pixels = int(max_distance_native_units / pixel_size_native_units)

    dtc_in_pixels = euclidean_distance_transform(cloud_mask, image_size_pixels=image_size_pixels, border_pixels=max_distance_pixels)
    if max_distance_native_units is not None:  # pixel_size_native_units must be specified
        return dtc_in_pixels * pixel_size_native_units

    return dtc_in_pixels


def euclidean_distance_transform(band: openeo.DataCube, image_size_pixels, border_pixels) -> openeo.DataCube:
    """
    Computes the distance (in pixels) to the closest background pixel value of ``False``.
    The distance is computed with a border of ``border_pixels`` pixels around the region of interest
    (of size ``image_size_pixels``).

    This means, the maximum possible distance to be computed is ``border_pixels + image_size_pixels - 1``.
    from a pixel of interest (``False``) situated on one edge of the border to the edge of the image (without border)
    on the opposite side.
    """
    udf = openeo.UDF.from_file(UDF_DISTANCE_TRANSFORM_PATH, runtime="Python")#, version="3")
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


def compute_distance_score(distance_to_cloud: openeo.DataCube, max_distance) -> openeo.DataCube:
    rescaled = (distance_to_cloud - 1) / max_distance

    score = rescaled.apply(lambda x: x.clip(min=0, max=1))
    return score.add_dimension("bands", "distance_score", type="bands")
