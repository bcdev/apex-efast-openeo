#!/usr/bin/env python3

import json
from pathlib import Path
import importlib

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
    process_description_resource = importlib.resources.files("efast_openeo.data").joinpath("process-description.md")
    process_description = process_description_resource.read_text(encoding="utf-8")
    pg_with_metadata = openeo.rest.udp.build_process_dict(
        process_graph,
        process_id="efast",
        summary=(
            "The Efficient Fusion Algorithm Across Spatio-Temporal Scales (EFAST) is a method to create time-series "
            "with a fine resolution in space and time from a fine spatial but coarse temporal resolution source "
            "(Sentinel-2) and a coarse temporal but fine spatial resolution source (Sentinel-3)."
        ),
        description=process_description,
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
