#!/usr/bin/env python3

import json
from pathlib import Path

import click
import openeo
from efast_openeo.define_udp import create_efast_udp


@click.group()
def cli():
    pass


@cli.command()
@click.argument("json_path", type=click.Path(path_type=Path))
def export(json_path: Path):
    connection = openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc()
    params, process_graph = create_efast_udp(connection)
    pg_with_metadata = openeo.rest.udp.build_process_dict(
        process_graph,
        process_id="efast",
        summary=(
            "The Efficient Fusion Algorithm Across Spatio-Temporal Scales (EFAST) is a method to create time-series "
            "with a fine resolution in space and time from a fine spatial but coarse temporal resolution source "
            "(Sentinel-2) and a coarse temporal but fine spatial resolution source (Sentinel-3)."
        ),
        description=(
            """The Efficient Fusion Algorithm Across Spatio-Temporal Scales (EFAST) [1] is a method to create
            time-series with a fine resolution in space and time from a fine spatial but coarse temporal resolution
            source (Sentinel-2) and a coarse temporal but fine spatial resolution source (Sentinel-3).
            In comparison to other methods, EFAST aims to achieve results outperforming single-source interpolation
            without paying the computational cost of other methods leveraging spatial context.
            EFAST is focused on accurately capturing seasonal vegetation changes in homogeneous areas like range lands.
            DHI-GRAS has published a Python implementation of the algorithm [2]. In the context of the ESA funded APEx
            initiative [3], the algorithm has been ported to OpenEO and is implemented in this process graph.
            
            EFAST interpolates Sentinel-2 acquisitions, using a time (temporal distance to target time) and
            distance-to-cloud weighted compositing scheme. The Sentinel-3 time-series is incorporated to locally update
            the interpolated Sentinel-2 imagery to accurately track vegetation changes between cloud-free Sentinel-2
            acquisitions. The method is described in detail in [1].
            
            EFAST produces high temporal frequency time-series matching the Sentinel-2 L2A bands which have
            corresponding Sentinel-3 OLCI bands with matching centre frequencies. Because the application of EFAST is
            focused on NDVI time-series, the UDP includes a parameter (output_ndvi) to directly compute the NDVI
            for a given temporal and spatial extent.
            
            It should be noted that OpenEO provides bands from the SENTINEL_L2A collection in integer format. EFAST
            converts the data to floating point values for interpolation. Therefore, output bands of EFAST have a
            different data type (floating point) than the corresponding SENTINEL_L2A bands.
            
             
            [1]: Senty, Paul, Radoslaw Guzinski, Kenneth Grogan, et al. “Fast Fusion of Sentinel-2 and Sentinel-3 Time Series over Rangelands.” Remote Sensing 16, no. 11 (2024): 1833.
            [2]: https://github.com/DHI-GRAS/efast
            [3]: https://apex.esa.int/
            [4]: https://github.com/bcdev/efast-process-graph/
            """
        ),
        parameters=params,
    )

    with open(json_path, "w") as fh:
        fh.write(json.dumps(pg_with_metadata, indent=4))


@cli.command()
def run():
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
            "east": -15.325491,
            "north": 15.787501,
        },
        temporal_extent=["2022-09-07", "2022-09-27"],
        target_time_series=[
            "2022-09-07",
            "2022-09-09",
            "2022-09-11",
            "2022-09-13",
            "2022-09-15",
            "2022-09-17",
        ],
        s2_data_bands=["B02", "B03", "B04", "B8A"],
        fused_band_names=["B02_fused", "B03_fused", "B04_fused", "B8A_fused"],
    )
    job = cube.create_job()
    job.start()
    print(job.status())
    print(job.job_id)


if __name__ == "__main__":
    cli()
