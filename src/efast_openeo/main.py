from pathlib import Path

import openeo
import click
import xarray as xr

from efast_openeo.util.log import logger
from efast_openeo import constants
from efast_openeo.efast import efast_openeo


def parse_bbox(ctx, param, value):
    try:
        west, south, east, north = map(float, value.split(","))
        return {"west": west, "south": south, "east": east, "north": north}
    except Exception:
        raise click.BadParameter(
            "Bounding box must be 4 comma separated floats, 'west,south,east,north'"
        )


def parse_bands(ctx, param, value):
    if value is None:
        return None

    try:
        bands_list = [band.strip() for band in value.strip("[]()").split(",")]
        return bands_list
    except Exception:
        raise click.BadParameter(
            f"Bands must be a comma separated list of bands, got '{value}'"
        )


@click.command()
@click.option(
    "--max-distance-to-cloud-m",
    type=float,
    default=5000,
    show_default=True,
    help=("Maximum distance (m) to consider in the distance-to-cloud score "),
)
@click.option(
    "--t-start",
    required=True,
    type=str,
    help=("Start of the time frame to load inputs for (inclusive)"),
)
@click.option(
    "--t-end-excl",
    type=str,
    required=True,
    help=("End of the time frame to load inputs for (exclusive)"),
)
@click.option(
    "--t-target-start",
    required=False,
    type=str,
    help=("Start of the time frame of the fused output (inclusive)"),
)
@click.option(
    "--t-target-end-excl",
    type=str,
    required=False,
    help=("End of the time frame of the fused output (exclusive)"),
)
@click.option(
    "--interval-days",
    type=str,
    help=(
        "Interval at which to compute the target time series and S3 composites. Uses xr.date_range syntax"
    ),
)
@click.option(
    "--temporal-score-stddev",
    type=float,
    required=False,
    default=constants.S2_TEMPORAL_SCORE_STDDEV,
    help=(
        "Standard deviation (in days) of the gaussian window used to temporally weigh observations in the fusion procedure"
    ),
)
@click.option(
    "--bbox",
    callback=parse_bbox,
    required=True,
    help="Bounding box as 'west,south,east,north'",
)
@click.option(
    "--s3-data-bands",
    callback=parse_bands,
    default="Syn_Oa04_reflectance,Syn_Oa06_reflectance",
    help="S3 bands (excluding flag band)",
)
@click.option(
    "--s2-data-bands",
    callback=parse_bands,
    default="B02,B03",
    help="S2 bands (excluding flag band)",
)
@click.option(
    "--fused-band-names",
    required=False,
    callback=parse_bands,
    help="Names of the bands after fusion",
)
@click.option("-o", "--output-dir", default="fused.nc", help="Output directory.")
@click.option(
    "--save-intermediates",
    is_flag=True,
    help="Output path. The file extension determines the output data type.",
)
@click.option(
    "--skip-intermediates",
    callback=lambda ctx, param, value: set(parse_bands(ctx, param, value)),
    help="Intermediates to not compute, even if --save-intermediates is set",
)
@click.option(
    "--synchronous",
    is_flag=True,
    help="Run with synchronous mode (download) instead of as a batch job.",
)
@click.option(
    "--file-format",
    default="netcdf",
    help="File format for downloading intermediate and complete results",
)
@click.option(
    "--cloud-tolerance-percentage",
    default=0.05,
    type=float,
    help="Percentage of a S3 pixel covered by S2 cloud from which it is considered cloudy.",
)
@click.option(
    "--output-ndvi",
    is_flag=True,
    help="If set, produce the normalized difference vegetation index (NDVI) as output instead of the fused bands",
)
@click.option(
    "--use-stepwise-aggregation", is_flag=True,
    help="If set, use alternative stepwise (per target time stamp) UDF composite implementation"
)
def main(
    max_distance_to_cloud_m,
    t_start,
    t_end_excl,
    t_target_start,
    t_target_end_excl,
    interval_days,
    bbox,
    s3_data_bands,
    s2_data_bands,
    fused_band_names,
    output_dir,
    save_intermediates,
    synchronous,
    skip_intermediates,
    file_format,
    cloud_tolerance_percentage,
    output_ndvi,
    temporal_score_stddev,
    use_stepwise_aggregation,
):
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(exist_ok=True)

    if fused_band_names is None:
        fused_band_names = s2_data_bands

    connection = openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc()

    max_distance_to_cloud_s3_px = max_distance_to_cloud_m / constants.S3_RESOLUTION_M

    logger.info(
        f"Running EFAST, temporal extent: '{[t_start, t_end_excl]}, spatial extent: '{bbox}'"
    )
    logger.info(f"S3 Data Bands: {s3_data_bands}")
    logger.info(f"S2 Data Bands: {s2_data_bands}")
    logger.info(f"Output band names: {fused_band_names}")
    logger.info(f"Saving results to '{output_dir}'")
    logger.info(f"Max distance to cloud: '{max_distance_to_cloud_m} m'")
    logger.info(f"Max distance to cloud: '{max_distance_to_cloud_s3_px:.2f} pixels'")

    if not t_target_start or t_target_end_excl:
        temporal_extent_target=[]
    else:
        temporal_extent_target=[t_target_start, t_target_end_excl]

    fused = efast_openeo(
        connection=connection,
        max_distance_to_cloud_m=max_distance_to_cloud_m,
        temporal_extent=[t_start, t_end_excl],
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        bbox=bbox,
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
        temporal_score_stddev=temporal_score_stddev,
        use_stepwise_aggregation=use_stepwise_aggregation,
    )
    # inputs

    print(fused.to_json())
    if synchronous:
        fused.download(output_dir / "fused.nc")
    else:
        fused.execute_batch(output_dir / "fused.nc", title="EFAST full chain")
    logger.info("Done")


if __name__ == "__main__":
    main()
