from pathlib import Path
from typing import List

from efast_openeo.algorithms.fusion import fusion
from efast_openeo.algorithms.temporal_interpolation import (
    interpolate_time_series_to_target_extent,
    interpolate_time_series_to_target_labels,
)
from efast_openeo.algorithms.weighted_composite import compute_weighted_composite
from efast_openeo.constants import S3_INTERPOLATION_BAND_NAME_SUFFIX
from efast_openeo.smoothing import smoothing_kernel
from efast_openeo.util.log import logger
from efast_openeo import constants
from efast_openeo.data_loading import load_and_scale
from efast_openeo.algorithms.distance_to_cloud import (
    distance_to_cloud,
    compute_cloud_mask_s3,
    compute_cloud_mask_s2,
    compute_distance_score,
)

import openeo
from openeo.api.process import Parameter
from openeo import processes


def save_intermediate(
    cube,
    name: str,
    out_dir: str | Path,
    file_format: str,
    synchronous: bool,
    *,
    to_skip: list | set | None = None,
    skip_all=False,
):
    """
    Save an intermediate result, either by using ``download``, if execution is synchronous or via adding a ``save_result``
    node in the process graph.

    :param cube: the cube (ProcessBuilder) to save
    :param name: file name for saving the cube
    :param out_dir: directory where the downloaded cube will be saved (only sync)
    :param file_format: file format for saved cube, if ``tif`` GeoTiff, netcdf otherwise
    :param synchronous: whether to use ``download`` (synchronous execution) or ``save_result`` (asynchronous execution)
    :param to_skip: list of intermediate result names to skip in the execution. If ``name`` appears in ``to_skip``, the unmodified
        cube will be returned and no intermediate result saved. Skipping uninteresting intermediates saves
        significant time in synchronous execution.
    :param skip_all: Whether to skip all intermediates (default: False)

    :return: the unmodified process builder (sync) or the input process builder with a ``save_result`` node attached (async).
    """
    if skip_all or (to_skip is not None and name in to_skip):
        logger.info(f"Skipping intermediate '{name}'")
        return cube

    suffix = ".nc"
    if file_format.lower().strip(".") in ["tif", "geotiff", "tiff"]:
        suffix = ".tif"
    name = Path(name).with_suffix(suffix)

    if synchronous:
        logger.info(f"downloading '{name}' (sync)")
        cube.download(Path(out_dir) / name)
        return cube

    # TODO name should be used
    with_save_result = cube.save_result(format=file_format)
    logger.info(f"Execute batch: '{name}' (half sync)")
    with_save_result.execute_batch(outputfile=Path(out_dir) / name, title=str(name))
    logger.info(f"Adding '{name}' to results (async)")
    return cube


def efast_openeo(
    connection: openeo.Connection,
    *,
    max_distance_to_cloud_m: int,
    temporal_extent: List[str],
    bbox: dict[str, float],
    s3_data_bands: List[str],
    s2_data_bands: List[str],
    fused_band_names: List[str] | Parameter | None,
    output_dir: str | Path,
    save_intermediates: bool,
    synchronous: bool,
    skip_intermediates: List[str],
    file_format: str,
    cloud_tolerance_percentage: float,
    temporal_extent_target: List[str] | None,
    interval_days: int,
    temporal_score_stddev: float | Parameter,
    output_ndvi: bool,
) -> openeo.DataCube:
    """
    Main logic for the EFAST [1] Sentinel-2 / Sentinel-3 Fusion implemented as an OpenEO process graph.
    The algorithm proceeds with the following broad steps:

    - Load Sentinel-2 and Sentinel-3 bands and cloud flags separately
    - Compute Sentinel-3 weighted composites:
        - For each S3 time step: Compute a spatial score from the distance to the nearest cloud
        - Compute a temporal score for each combination of input and target time step
        - Combine the scores and create a weighted sum of input observations to generate the output observations
    - Interpolate the Sentinel-3 composite time series to the target time steps
    - Compute weighted composites of Sentinel-2
        - The procedure is the same as for Sentinel-3
        - The distance to cloud score is not computed on the Sentinel-2 native grid, but on the Sentinel-3 scale
        - The weights computed for the S2 composite are also applied to the S3 composites interpolated to
            the S2 time series. This creates a set of S2 weighted S3 composites
        - Finally, the "Fused" output is computed by adding to the S2 composites the interpolated (not weighted) S3
         observations and subtracting the S3 composites created with the S2 weights


    [1]: Senty, Paul, Radoslaw Guzinski, Kenneth Grogan, et al. “Fast Fusion of Sentinel-2 and Sentinel-3 Time Series over Rangelands.” Remote Sensing 16, no. 11 (2024): 11. https://doi.org/10.3390/rs16111833.

         :param connection: Authenticated connection to an openeo backend
         :param max_distance_to_cloud_m: Maximum distance to cloud to be considered in the distance score
         :param temporal_extent: temporal extent of all input cubes
            Should be passed as a list of strings, e.g ["2022-01-01", "2022-01-03", "2022-01-05"]
         :param bbox: bounding box of the area of interest. Dictionary with keys "west", "south", "east", "north",
            and optionally "crs".
         :param s3_data_bands: Sentinel-3 SYN L2 SYN bands (names follow the SENTINEL3_SYN_L2_SYN collection).
         :param s2_data_bands: Sentinel-2 L2A bands (names follow the SENTINEL2_L2A collection).
         :param fused_band_names: Band names of the output (correspond to the sentinel-2 band names)
         :param output_dir: directory where to save intermediate results, if ``synchronous`` and ``save_intermediates``
            are set.
         :param save_intermediates: Whether to save any intermediate results
         :param synchronous: Whether to use ``download`` (synchronous=True) or ``save_result`` (synchronous=False) to
            save intermediate results
         :param skip_intermediates: List of intermediate results to skip in this execution
         :param file_format: File format for intermediate results
         :param cloud_tolerance_percentage: Percentage of a Sentinel-3 pixel to be covered by cloud
            (using the S2 cloud mask) from which the S3 pixel will be considered cloudy.
        :param temporal_extent_target: temporal extent of the output time series. Setting `temporal_extent`
            to a shorter interval, contained in ``temporal_extent``, allows all outputs to have a larger context of
            preceding and following observations. If ``None``, fusion results will be generated for the entire
             ``temporal_extent``. Should be entirely contained in ``temporal_extent``.
        :param interval_days: Interval at which to generate fused composites. This parameter also determines the
            interval of Sentinel-3 composites used in the computation.

        :returns: Datacube with time series defined by the borders [incl, excl) ``termporal_extent_composites`` and step
         ``interval_days``, ``fused_band_names`` bands on S2 resolution.
    """
    skip_all_intermediates = not save_intermediates
    max_distance_to_cloud_s3_px = max_distance_to_cloud_m / constants.S3_RESOLUTION_M

    # Separate ``load_collection`` calls must be used (not filter_bands) because of a backend bug
    # https://forum.dataspace.copernicus.eu/t/combination-of-apply-neighborhood-and-merge-cubes-leads-to-additional-labels-in-the-time-dimension/4189/3
    s3_flags = connection.load_collection(
        constants.S3_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=temporal_extent,
        bands=[constants.S3_FLAG_BAND],
    ).band(constants.S3_FLAG_BAND)

    # TODO expose as CLI parameters
    binning_params = dict(
        super_sampling=2,
        flag_band=constants.S3_FLAG_BAND,
        flag_bitmask=0xff,
    )

    s3_bands = load_and_scale(
        connection=connection,
        use_binning=True,
        binning_params=binning_params,
        collection_id=constants.S3_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=temporal_extent,
        bands=s3_data_bands,
    )
    s3_bands = s3_bands.filter_labels(
        dimension="bands",
        condition= lambda b: b != constants.S3_FLAG_BAND,
    )
    s2_flags = connection.load_collection(
        constants.S2_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=temporal_extent,
        bands=[constants.S2_FLAG_BAND],
    ).band(constants.S2_FLAG_BAND)
    s2_bands = load_and_scale(
        connection=connection,
        collection_id=constants.S2_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=temporal_extent,
        bands=s2_data_bands,
    )
    # TODO collect intermediates in a dict and run save_intermediates at the end (also, avoid repeating the parameters)
    s2_bands = save_intermediate(
        s2_bands,
        "s2_bands",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    # s3 composites
    overlap_factor = 10
    s3_dtc_overlap_length_px = (
        int(max_distance_to_cloud_m * overlap_factor) // constants.S3_RESOLUTION_M
    )
    s3_dtc_patch_length_px = s3_dtc_overlap_length_px * 2

    logger.info(f"Setting {s3_dtc_patch_length_px=} and {s3_dtc_overlap_length_px=}")

    s3_cloud_mask = s3_bands.band(0).apply(lambda x: processes.is_nodata(x))
    s3_cloud_mask = save_intermediate(
        s3_cloud_mask,
        "s3_cloud_mask",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s3_distance_to_cloud = distance_to_cloud(
        s3_cloud_mask,
        image_size_pixels=s3_dtc_patch_length_px,
        max_distance_pixels=s3_dtc_overlap_length_px,
        pixel_size_native_units=constants.S3_RESOLUTION_DEG,
    )
    s3_distance_to_cloud = save_intermediate(
        s3_distance_to_cloud,
        "s3_distance_to_cloud",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )
    s3_distance_score = compute_distance_score(
        s3_distance_to_cloud, max_distance_to_cloud_s3_px
    )
    s3_distance_score = save_intermediate(
        s3_distance_score,
        "s3_distance_score",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s3_bands_and_distance_score = s3_bands.merge_cubes(s3_distance_score)
    s3_bands_and_distance_score = save_intermediate(
        s3_bands_and_distance_score,
        "s3_bands_and_distance_score",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )
    s3_composite = compute_weighted_composite(
        s3_bands_and_distance_score,
        temporal_extent=temporal_extent,
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        sigma_doy=constants.S3_TEMPORAL_SCORE_STDDEV,
    )
    s3_composite_data_bands = s3_composite.filter_bands(s3_bands.dimension_labels("bands"))
    s3_composite_data_bands = save_intermediate(
        s3_composite_data_bands,
        "s3_composite_data_bands",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s3_composite_data_bands_smoothed = s3_composite_data_bands.apply_kernel(
        kernel=smoothing_kernel()
    )
    s3_composite_data_bands_smoothed = save_intermediate(
        s3_composite_data_bands_smoothed,
        "s3_composite_data_bands_smoothed",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    # s2 pre processing
    # do not use output for next step, the conversion to int is only a workaround of a backend bug for downloads
    save_intermediate(
        s2_flags * 1,
        "s2_cloud_flags",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )
    s2_cloud_mask = (
        compute_cloud_mask_s2(s2_flags) * 1.0
    )  # convert to float for inspection and mean computation
    s2_cloud_mask = save_intermediate(
        s2_cloud_mask,
        "s2_cloud_mask",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )
    s2_cloud_mask_mean = s2_cloud_mask.resample_spatial(
        resolution=300, method="average"
    )
    s2_cloud_mask_mean = save_intermediate(
        s2_cloud_mask_mean,
        "s2_cloud_mask_mean",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )
    s2_cloud_mask_coarse = s2_cloud_mask_mean >= cloud_tolerance_percentage
    s2_cloud_mask_coarse = save_intermediate(
        s2_cloud_mask_coarse,
        "s2_cloud_mask_coarse",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s2_distance_to_cloud = distance_to_cloud(
        s2_cloud_mask_coarse,
        image_size_pixels=s3_dtc_patch_length_px,
        max_distance_pixels=s3_dtc_overlap_length_px,
        pixel_size_native_units=constants.S3_RESOLUTION_DEG,
    )
    s2_distance_to_cloud = save_intermediate(
        s2_distance_to_cloud,
        "s2_distance_to_cloud",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s2_distance_score = compute_distance_score(
        s2_distance_to_cloud, max_distance_to_cloud_s3_px
    )
    s2_distance_score = save_intermediate(
        s2_distance_score,
        "s2_distance_score",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s2_bands_masked = s2_bands.mask(s2_cloud_mask)
    s2_bands_masked = save_intermediate(
        s2_bands_masked,
        "s2_bands_masked",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    # s3 temporal resampling
    s3_composite_target_interp = interpolate_time_series_to_target_extent(
        s3_composite_data_bands_smoothed,
        temporal_extent=temporal_extent,
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        target_band_name_suffix=S3_INTERPOLATION_BAND_NAME_SUFFIX,
    )
    s3_composite_target_interp = save_intermediate(
        s3_composite_target_interp,
        "s3_composite_target_interp",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s3_composite_s2_interp = interpolate_time_series_to_target_labels(
        s3_composite_data_bands_smoothed, s2_bands.dimension_labels("t")
    )
    s3_composite_s2_interp = save_intermediate(
        s3_composite_s2_interp,
        "s3_composite_s2_interp",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    # s2/3 aggregate
    s2_bands_dtc_merge = s2_bands_masked.merge_cubes(s2_distance_score)
    s2_bands_dtc_merge = save_intermediate(
        s2_bands_dtc_merge,
        "s2_bands_dtc_merge",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )
    s2_s3_pre_aggregate_merge = s2_bands_dtc_merge.merge_cubes(s3_composite_s2_interp)
    s2_s3_pre_aggregate_merge = save_intermediate(
        s2_s3_pre_aggregate_merge,
        "s2_s3_pre_aggregate_merge",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    s2_s3_aggregate = compute_weighted_composite(
        s2_s3_pre_aggregate_merge,
        temporal_extent=temporal_extent,
        temporal_extent_target=temporal_extent_target,
        interval_days=interval_days,
        sigma_doy=temporal_score_stddev,
    )
    s2_s3_aggregate = save_intermediate(
        s2_s3_aggregate,
        "s2_s3_aggregate",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    fusion_input = s2_s3_aggregate.merge_cubes(s3_composite_target_interp)
    fusion_input = save_intermediate(
        fusion_input,
        "fusion_input",
        out_dir=output_dir,
        file_format=file_format,
        synchronous=synchronous,
        to_skip=skip_intermediates,
        skip_all=skip_all_intermediates,
    )

    fused = fusion(
        fusion_input,
        high_resolution_mosaic_band_names=s2_data_bands,
        low_resolution_mosaic_band_names=s3_data_bands,
        low_resolution_interpolated_band_name_suffix=S3_INTERPOLATION_BAND_NAME_SUFFIX,
        target_band_names=fused_band_names,
        output_ndvi=output_ndvi,
    )

    return fused
