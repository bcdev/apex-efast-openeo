from pathlib import Path
from typing import Tuple, List

import openeo
from openeo.api.process import Parameter

from efast_openeo.efast import efast_openeo


def create_efast_udp(connection) -> Tuple[List[Parameter], openeo.DataCube]:
    """
    Creates the parameters and parameterized process graph for EFAST.

    :param connection: authenticated connection to an OpenEO backend
    :returns: (parameters, process graph) the parameters to be defined when calling the EFAST UDP and the
        parameterized process graph

    """
    temporal_extent = Parameter(
        name="temporal_extent",
        description=(
            "The date range of the Sentinel-2 L2A and Sentinel-3 SY_2_SYN inputs. "
            "The fused (output) time series can optionally be defined by the temporal_extent_target parameter. "
            "If temporal_extent_target is not set, the output time series will cover temporal_extent by default"
        ),
        schema={"type": "array", "subtype": "temporal-interval"},
    )

    spatial_extent = Parameter(
        name="spatial_extent",
        description="Region of interest",
        schema={"type": "object", "subtype": "geojson"},
    )

    temporal_extent_target = Parameter.temporal_interval(
        name="temporal_extent_target",
        description=(
            "The date range of the fused outputs. Must be completely contained in temporal_extent. "
            "This parameter is optional, if no temporal_extent_target is not set, the output will cover the "
            "temporal extent of the inputs (defined by the parameter temporal_extent). "
            "As per openeo convention, the start date of the temporal extent is included and the end date excluded if "
            "if no time of day is set."
        ),
        default=None,
    )

    interval_days = Parameter.integer(
        name="interval_days",
        description=(
            "Interval (in days) of the time series of the output. "
            "If, for example, temporal_extent_target is set to [2022-01-01, 2022-01-10] and interval_days is set to 3, "
            "the time series of the output will consist of images for [2022-01-01, 2022-01-04, 2022-01-07]. "
            "2022-01-10 is excluded, as the temporal extent is defined including the start date and excluding "
            "the end date."
        )
    )

    output_ndvi = Parameter.boolean(
        name="output_ndvi",
        description=(
            "If set to True, the output includes a single NDVI band. Otherwise, "
            "the output includes the bands of the Sentinel-2 input (defined by parameter s2_data_bands). "
            "If set to True, the bands B04 and B8A must be included in s2_data_bands, and the bands "
            "Syn_Oa08_reflectance and Syn_Oa17_reflectance must be included in s3_data_bands."
        ),
        default=False,
    )

    # max_distance_to_cloud_m can't be a parameter, because it determines the overlap size in the ``apply_neighborhood``
    # call used to compute the distance to cloud scores

    # max_distance_to_cloud_m = Parameter.number(
    #    name="max_distance_to_cloud_m",
    #    description=(
    #        "Maximum distance to cloud in metres to be considered for the distance to cloud weighting. "
    #        " This parameter determines the overlap size used in the distance to cloud computation (via apply_neighborhood). Default 5000"
    #    ),
    #    default=5000,
    # )

    # temporal_score_stddev = Parameter.number(
    #    name="temporal_score_stddev",
    #    description=(
    #        "Standard deviation (in days) of the gaussian window used for temporal composites. "
    #        "A larger number means that observations further away from the target date receive a stronger weight."
    #    ),
    #    default=10,
    # )

    s3_data_bands = Parameter.array(
        name="s3_data_bands",
        description=(
            "Sentinel-3 SYN L2 SYN bands (names follow the SENTINEL3_SYN_L2_SYN collection) used in the fusion "
            "procedure. The order of s3_data_bands must match the order of s2_data_bands, so that bands with closely "
            "matching center wavelengths are fused. "
            "If the NDVI should be computed (parameter output_ndvi is set to True), s3_data_bands must be set to "
            "[Syn_Oa08_reflectance, Syn_Oa17_reflectance]."
        ),
        item_schema={"type": "string"},
        default=[
            "Syn_Oa04_reflectance",
            "Syn_Oa06_reflectance",
            "Syn_Oa08_reflectance",
            "Syn_Oa17_reflectance",
        ],
    )

    s2_data_bands = Parameter.array(
        name="s2_data_bands",
        description=(
            "Sentinel-2 L2A bands (names follow the SENTINEL2_L2A collection) used in the fusion procedure. "
            "The order should match the corresponding s3_data_bands and fused_band_names parameters."
            "If the NDVI should be computed (parameter output_ndvi is set to True), s2_data_bands must be set to "
            "[B04, B8A]."
        ),
        item_schema={"type": "string"},
        default=["B02", "B03", "B04", "B8A"],
    )

    # FIXME this isn't a parameter because arithmetic operations with parameters need to be handled specially.
    # May stay hardcoded, or a solution to correctly handle arithmetic / comparisons with a parameter must be found.
    cloud_tolerance_percentage = Parameter.number(
        name="cloud_tolerance_percentage",
        description=(
            "Percentage cloud coverage (from Sentinel-2 L2A cloud mask) of a Sentinel 3 SYN pixel from which it is considered cloudy."
        ),
        default=0.05,
    )

    params = [
        temporal_extent,
        temporal_extent_target,
        interval_days,
        spatial_extent,
        # max_distance_to_cloud_m, # parameter can't define overlap of apply_neighborhood
        # temporal_score_stddev,
        s2_data_bands,
        s3_data_bands,
        output_ndvi,
        # cloud_tolerance_percentage, Unexpected error in backend when using gte process
    ]

    # hard coded parameters

    fused_band_names = None
    cloud_tolerance_percentage = 0.05
    max_distance_to_cloud_m = 5000

    # non-UDP parameters

    output_dir = None
    save_intermediates = False
    skip_intermediates = []
    file_format = None
    synchronous = None

    process_graph = efast_openeo(
        connection=connection,
        max_distance_to_cloud_m=max_distance_to_cloud_m,
        temporal_extent=temporal_extent,
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        bbox=spatial_extent,
        s3_data_bands=s3_data_bands,
        s2_data_bands=s2_data_bands,
        fused_band_names=fused_band_names,
        output_dir=output_dir,
        save_intermediates=save_intermediates,
        synchronous=synchronous,
        skip_intermediates=skip_intermediates,
        file_format=file_format,
        cloud_tolerance_percentage=cloud_tolerance_percentage,
        output_ndvi=output_ndvi,
    )

    return params, process_graph


if __name__ == "__main__":
    # TODO make a function that takes command line arguments
    connection = openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc()
    params, process_graph = create_efast_udp(connection)
    process_id = "efast"

    connection.save_user_defined_process(
        user_defined_process_id=process_id,
        process_graph=process_graph,
        parameters=params,
    )
    cube = connection.datacube_from_process(
        process_id=process_id,
        spatial_extent={
            "west": -15.456047,
            "south": 15.665024,
            # "east": -15.425491,
            # "north": 15.687501,
            "east": -15.325491,
            "north": 15.787501,
        },
        temporal_extent=["2022-09-07", "2022-09-27"],
        temporal_extent_target=["2022-09-09", "2022-09-25"],
        interval_days=2,
        s2_data_bands=["B02", "B03", "B04", "B8A"],
        fused_band_names=["B02_fused", "B03_fused", "B04_fused", "B8A_fused"],
    )

    out_path = Path(__file__).parent.parent.parent / "test_outputs" / "full_chain_udp"
    # cube.download("fused_udp.nc")
    cube.execute_batch(outputfile=(out_path / "fused_udp.nc"))
