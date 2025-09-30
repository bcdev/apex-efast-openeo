from pathlib import Path

import openeo
from openeo.api.process import Parameter

from efast_openeo.efast import efast_openeo


def create_efast_udp(connection):
    temporal_extent = Parameter(
        name="temporal_extent",
        description=(
            "The date range of the Sentinel-2 L2A and Sentinel-3 SY_2_SYN inputs. "
            " The fused (output) time series is passed as a different parameter"
        ),
        schema={"type": "array", "subtype": "temporal-interval"},
    )

    spatial_extent = Parameter(
        name="spatial_extent",
        description="Region of interest",
        schema={"type": "object", "subtype": "geojson"}
    )

    target_time_series = Parameter(
        name="target_time_series",
        description="List of dates for which to compute the fused product. Dates must be ordered and inside the temporal extent",
        schema={
            "type": "array",
            "items": {
                "type": "string",
                "format": "date",
                "subtype": "date",
            }
        }

    )

    params = [temporal_extent, spatial_extent, target_time_series]

    # hard coded parameters

    max_distance_to_cloud_m = 5000
    temporal_score_stddev = 10
    s3_data_bands = ["Syn_Oa04_reflectance", "Syn_Oa06_reflectance", "Syn_Oa08_reflectance", "Syn_Oa17_reflectance"]
    s2_data_bands = ["B02", "B03", "B04", "B8A"]
    fused_band_names = ["B02_fused", "B03_fused", "B04_fused", "B8A_fused"]
    cloud_tolerance_percentage = 0.05
    t_s3_composites = target_time_series

    # non-UDP parameters

    output_dir = None
    save_intermediates = False
    skip_intermediates = []
    file_format = None
    synchronous = None


    process_graph = efast_openeo(connection=connection, max_distance_to_cloud_m=max_distance_to_cloud_m, temporal_score_stddev=temporal_score_stddev, t_s3_composites=t_s3_composites, t_target=target_time_series, temporal_extent=temporal_extent,
                 bbox=spatial_extent, s3_data_bands=s3_data_bands, s2_data_bands=s2_data_bands, fused_band_names=fused_band_names, output_dir=output_dir, save_intermediates=save_intermediates, synchronous=synchronous,
                 skip_intermediates=skip_intermediates, file_format=file_format, cloud_tolerance_percentage=cloud_tolerance_percentage)

    return params, process_graph


if __name__ == '__main__':
    # TODO add parameters like output dir and the UDP parameters
    connection = openeo.connect("https://openeo.dataspace.copernicus.eu/").authenticate_oidc()
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
            "east": -15.425491,
            "north": 15.687501,
        },
        temporal_extent=["2022-09-07", "2022-09-27"],
        target_time_series=[
            "2022-09-07",
            "2022-09-09",
            "2022-09-11",
            "2022-09-13",
            "2022-09-15",
            "2022-09-17",
            "2022-09-19",
            "2022-09-21",
            "2022-09-23",
            "2022-09-25",
            "2022-09-27",
        ]
    )

    out_path = Path(__file__).parent.parent / "test_outputs" / "full_chain_udp"
    #cube.download("fused_udp.nc")
    cube.execute_batch(out_path / "fused_udp.nc")
