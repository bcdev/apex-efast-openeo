from pathlib import Path

import openeo
import click
import xarray as xr

from efast_openeo.algorithms.fusion import fusion
from efast_openeo.algorithms.temporal_interpolation import interpolate_time_series_to_target_labels
from efast_openeo.algorithms.weighted_composite import compute_weighted_composite
from efast_openeo.util.log import logger
from efast_openeo import constants
from efast_openeo.data_loading import load_and_scale

from efast_openeo.algorithms.distance_to_cloud import distance_to_cloud, compute_cloud_mask_s3, compute_cloud_mask_s2, compute_distance_score


def save_intermediate(cube, name, out_dir, file_format, synchronous, *, to_skip: list | set | None = None):
    if to_skip is not None and name in to_skip:
        logger.info(f"Skipping intermediate '{name}'")
        return cube

    if synchronous:
        suffix = ".nc"
        if file_format.lower().strip(".") in ["tif", "geotiff", "tiff"]:
            suffix = ".tif"
        name = Path(name).with_suffix(suffix)
        logger.info(f"downloading '{name}' (sync)")
        cube.download(Path(out_dir) / name)
        return cube

    with_save_result = cube.save_result(format=file_format)
    logger.info(f"Adding '{name}' to results (async)")
    return with_save_result


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
def main(max_distance_to_cloud_m, temporal_score_stddev, t_start, t_end_excl, s3_composite_interval, target_interval, bbox, s3_data_bands, s2_data_bands, fused_band_names, output_dir, save_intermediates, synchronous, skip_intermediates, file_format, cloud_tolerance_percentage):
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

    # inputs
    # TODO test binning
    s3_flags = connection.load_collection(
            constants.S3_COLLECTION,
            spatial_extent=bbox,
            temporal_extent=[t_start, t_end_excl],
            bands=[constants.S3_FLAG_BAND]
        ).band(constants.S3_FLAG_BAND)
    s3_bands = load_and_scale(
        connection=connection,
        collection_id=constants.S3_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=[t_start, t_end_excl],
        bands=s3_data_bands,
    )
    s2_flags = connection.load_collection(
        constants.S2_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=[t_start, t_end_excl],
        bands=[constants.S2_FLAG_BAND],
    ).band(constants.S2_FLAG_BAND)
    s2_bands = load_and_scale(
        connection=connection,
        collection_id=constants.S2_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=[t_start, t_end_excl],
        bands=s2_data_bands,
    )
    s2_bands = save_intermediate(s2_bands, "s2_bands", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    # s3 composites
    s3_dtc_overlap_length_px = int(max_distance_to_cloud_m * 1.1) // constants.S3_RESOLUTION_M
    #s3_dtc_overlap_length_px = int(max_distance_to_cloud_m * 2) // constants.S3_RESOLUTION_M
    s3_dtc_patch_length_px = s3_dtc_overlap_length_px * 2

    logger.info(f"Setting {s3_dtc_patch_length_px=} and {s3_dtc_overlap_length_px=}")

    s3_cloud_mask = compute_cloud_mask_s3(s3_flags)
    s3_cloud_mask = save_intermediate(s3_cloud_mask, "s3_cloud_mask", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    s3_distance_to_cloud = distance_to_cloud(s3_cloud_mask, image_size_pixels=s3_dtc_patch_length_px, max_distance_pixels=s3_dtc_overlap_length_px, pixel_size_native_units=constants.S3_RESOLUTION_DEG)
    s3_distance_to_cloud = save_intermediate(s3_distance_to_cloud, "s3_distance_to_cloud", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)
    s3_distance_score = compute_distance_score(s3_distance_to_cloud, max_distance_to_cloud_s3_px)
    s3_distance_score = save_intermediate(s3_distance_score, "s3_distance_score", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    s3_bands_and_distance_score = s3_bands.merge_cubes(s3_distance_score)
    s3_bands_and_distance_score = save_intermediate(s3_bands_and_distance_score, "s3_bands_and_distance_score", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)
    s3_composite = compute_weighted_composite(s3_bands_and_distance_score, t_s3_composites, sigma_doy=temporal_score_stddev)
    s3_composite_data_bands = s3_composite.filter_bands(s3_data_bands)
    s3_composite_data_bands = save_intermediate(s3_composite_data_bands, "s3_composite_data_bands", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    # s2 pre processing
    s2_cloud_mask = compute_cloud_mask_s2(s2_flags)
    s2_cloud_mask = save_intermediate(s2_cloud_mask, "s2_cloud_mask", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)
    s2_cloud_mask_mean = (s2_cloud_mask * 1.0).resample_cube_spatial(s3_bands, method="average")
    s2_cloud_mask_mean = save_intermediate(s2_cloud_mask_mean, "s2_cloud_mask_mean", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)
    s2_cloud_mask_coarse = s2_cloud_mask_mean >= cloud_tolerance_percentage
    s2_cloud_mask_coarse = save_intermediate(s2_cloud_mask_coarse, "s2_cloud_mask_coarse", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    s2_distance_to_cloud = distance_to_cloud(s2_cloud_mask_coarse, image_size_pixels=s3_dtc_patch_length_px, max_distance_pixels=s3_dtc_overlap_length_px, pixel_size_native_units=constants.S3_RESOLUTION_DEG)
    s2_distance_to_cloud = save_intermediate(s2_distance_to_cloud, "s2_distance_to_cloud", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    s2_distance_score = compute_distance_score(s2_distance_to_cloud, max_distance_to_cloud_s3_px)
    s2_distance_score = save_intermediate(s2_distance_score, "s2_distance_score", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    s2_bands_masked = s2_bands.mask(s2_cloud_mask)
    s2_bands_masked = save_intermediate(s2_bands_masked, "s2_bands_masked", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    # s3 temporal resampling
    s3_composite_target_interp = interpolate_time_series_to_target_labels(s3_composite_data_bands, t_target)
    s3_composite_target_interp = save_intermediate(s3_composite_target_interp, "s3_composite_target_interp", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    s3_composite_target_interp_band_names = [b + "_interpolated" for b in s3_data_bands]
    s3_composite_target_interp = s3_composite_target_interp.rename_labels("bands", target=s3_composite_target_interp_band_names, source=s3_data_bands)
    s3_composite_s2_interp = interpolate_time_series_to_target_labels(s3_composite_data_bands, s2_bands.dimension_labels("t"))

    # s2/3 aggregate
    s2_bands_dtc_merge = s2_bands_masked.merge_cubes(s2_distance_score)
    s2_bands_dtc_merge = save_intermediate(s2_bands_dtc_merge, "s2_bands_dtc_merge", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)
    s2_s3_pre_aggregate_merge = s2_bands_dtc_merge.merge_cubes(s3_composite_s2_interp)
    s2_s3_pre_aggregate_merge = save_intermediate(s2_s3_pre_aggregate_merge, "s2_s3_pre_aggregate_merge", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    s2_s3_aggregate = compute_weighted_composite(s2_s3_pre_aggregate_merge, target_time_series=t_target, sigma_doy=temporal_score_stddev)
    s2_s3_aggregate = save_intermediate(s2_s3_aggregate, "s2_s3_aggregate", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    fusion_input = s2_s3_aggregate.merge_cubes(s3_composite_target_interp)
    fusion_input = save_intermediate(fusion_input, "fusion_input", out_dir=output_dir, file_format=file_format, synchronous=synchronous, to_skip=skip_intermediates)

    #s3_aggregate = s2_s3_aggregate.filter_bands(s3_data_bands)
    #s2_aggregate = s2_s3_aggregate.filter_bands(s2_data_bands)
    #s3_interpolated = s3_composite_target_interp.resample_cube_spatial(s2_s3_aggregate)

    # fusion
    fused = fusion(
        fusion_input,
        high_resolution_mosaic_band_names=s2_data_bands,
        low_resolution_mosaic_band_names=s3_data_bands,
        low_resolution_interpolated_band_names=s3_composite_target_interp_band_names,
        target_band_names=fused_band_names,
    )

    print(fused.to_json())
    #fused.execute_batch(output_dir / "fused.nc")
    fused.download(output_dir / "fused.nc")



if __name__ == '__main__':
    main()
