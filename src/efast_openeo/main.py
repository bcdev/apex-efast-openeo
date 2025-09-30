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
        return {
            "west": west,
            "south": south,
            "east": east,
            "north": north
        }
    except Exception:
        raise click.BadParameter("Bounding box must be 4 comma separated floats, 'west,south,east,north'")


def parse_bands(ctx, param, value):
    if value is None:
        return None

    try:
        bands_list = [band.strip() for band in value.strip("[]()").split(",")]
        return bands_list
    except Exception:
        raise click.BadParameter(f"Bands must be a comma separated list of bands, got '{value}'")


@click.command()
@click.option(
    "--max-distance-to-cloud-m",
    type=float,
    default=5000,
    show_default=True,
    help=(
            "Maximum distance (m) to consider in the distance-to-cloud score "
    )
)
@click.option(
    "--temporal-score-stddev",
    type=float,
    default=5,
    show_default=True,
    help=(
            # TODO reference paper
            "Standard deviation (in days) of the temporal window used to compute weighted aggregates. Sigma_doy in the original"
    )
)
@click.option(
    "--t-start",
    required=True,
    type=str,
    help=(
            "Start of the time frame to load inputs for (inclusive)"
    )
)
@click.option(
    "--t-end-excl",
    type=str,
    required=True,
    help=(
            "End of the time frame to load inputs for (exclusive)"
    )
)
@click.option(
    "--s3-composite-interval",
    default="2D",
    type=str,
    help=(
            "Interval at which to compute S3 composites. Uses xr.date_range syntax"
    )
)
# TODO may also be a list of dates
@click.option(
    "--target-interval",
    type=str,
    help=(
            "Interval at which to compute the target time series. Uses xr.date_range syntax"
    )
)
@click.option(
    "--bbox",
    callback=parse_bbox,
    required=True,
    help="Bounding box as 'west,south,east,north'"
)
@click.option(
    "--s3-data-bands",
    callback=parse_bands,
    default="Syn_Oa08_reflectance,Syn_Oa17_reflectance",
    help="S3 bands (excluding flag band)"
)
@click.option(
    "--s2-data-bands",
    callback=parse_bands,
    default="B02,B03",
    help="S2 bands (excluding flag band)"
)
@click.option(
    "--fused-band-names",
    required=False,
    callback=parse_bands,
    help="Names of the bands after fusion"
)
@click.option(
    "-o", "--output-dir",
    default="fused.nc",
    help="Output directory."
)
@click.option(
    "--save-intermediates",
    is_flag=True,
    help="Output path. The file extension determines the output data type."
)
@click.option(
    "--skip-intermediates",
    callback=lambda ctx, param, value: set(parse_bands(ctx, param, value)),
    help="Intermediates to not compute, even if --save-intermediates is set"
)
@click.option(
    "--synchronous",
    is_flag=True,
    help="Run with synchronous mode (download) instead of as a batch job."
)
@click.option(
    "--file-format",
    default="netcdf",
    help="File format for downloading intermediate and complete results"
)
@click.option(
    "--cloud-tolerance-percentage",
    default=0.05,
    type=float,
    help="Percentage of a S3 pixel covered by S2 cloud from which it is considered cloudy."
)
def main(max_distance_to_cloud_m, temporal_score_stddev, t_start, t_end_excl, s3_composite_interval, target_interval,
         bbox, s3_data_bands, s2_data_bands, fused_band_names, output_dir, save_intermediates, synchronous,
         skip_intermediates, file_format, cloud_tolerance_percentage):
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(exist_ok=True)

    if fused_band_names is None:
        fused_band_names = s2_data_bands

    connection = openeo.connect("https://openeo.dataspace.copernicus.eu/").authenticate_oidc()

    max_distance_to_cloud_s3_px = max_distance_to_cloud_m / constants.S3_RESOLUTION_M

    logger.info(f"Running EFAST, temporal extent: '{[t_start, t_end_excl]}, spatial extent: '{bbox}'")
    logger.info(f"S3 Data Bands: {s3_data_bands}")
    logger.info(f"S2 Data Bands: {s2_data_bands}")
    logger.info(f"Output band names: {fused_band_names}")
    logger.info(f"Saving results to '{output_dir}'")
    logger.info(f"Max distance to cloud: '{max_distance_to_cloud_m} m'")
    logger.info(f"Max distance to cloud: '{max_distance_to_cloud_s3_px:.2f} pixels'")

    # TODO verify that t_start and t_end_excl are formatted correctly
    t_s3_composites_dt = xr.date_range(t_start, t_end_excl, freq=s3_composite_interval)
    t_s3_composites = [d.isoformat() for d in t_s3_composites_dt.to_pydatetime()]
    t_target_dt = xr.date_range(t_start, t_end_excl, freq=target_interval)
    t_target = [d.isoformat() for d in t_target_dt.to_pydatetime()]

    if save_intermediates:
        with open(output_dir / "t_target.txt", "w") as fp:
            fp.write(str(t_target))
        with open(output_dir / "t_s3_composites.txt", "w") as fp:
            fp.write(str(t_s3_composites))

    fused = efast_openeo(connection=connection, max_distance_to_cloud_m=max_distance_to_cloud_m, temporal_score_stddev=temporal_score_stddev, temporal_extent=[t_start, t_end_excl], t_s3_composites=t_s3_composites, t_target=t_target,
                         bbox=bbox, s3_data_bands=s3_data_bands, s2_data_bands=s2_data_bands, fused_band_names=fused_band_names, output_dir=output_dir, save_intermediates=save_intermediates, synchronous=synchronous,
                         skip_intermediates=skip_intermediates, file_format=file_format, cloud_tolerance_percentage=cloud_tolerance_percentage)
    # inputs

    print(fused.to_json())
    fused.download(output_dir / "fused.nc")
    logger.info("Done")


if __name__ == '__main__':
    main()
