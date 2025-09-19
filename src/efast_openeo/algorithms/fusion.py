from typing import List

def fusion(low_resolution_mosaic, high_resolution_mosaic, low_resolution_interpolated, low_resolution_band_names: List[str], high_resolution_band_names: List[str], target_band_names: List[str] | None=None):
    """
    The EFAST fusion procedure combines two temporally and spatially weighted composites (called "mosaics") of
    low resolution (Sentinel-3) and high resolution (Sentinel-2) imagery with a temporally interpolated
    low resolution image.

    This OpenEO implementation works the same way, but instead of working on single images per type, it combines
    time series (cubes) of mosaics and interpolated images that all already share the same temporal dimension,
    corresponding to the user-defined targe time series.

    Despite the name, the low resolution mosaic and interpolated cube have the same spatial resolution as
    the high resolution mosaic. "Low resolution" refers to the resolution of the sensor from which the original
    data is taken.

    :param low_resolution_mosaic: temporally weighted mosaics of the low resolution source (e.g. Sentinel-3)
        on the target time series. OpenEO data cube.
    :param high_resolution_mosaic: temporally weighted mosaics of the low resolution source (e.g. Sentinel-3)
        on the target time series
    :param low_resolution_interpolated: interpolated (to the target time series) and upsampled (to the resolution
        of the high resolution sensor) images of the low resolution sensor.
    :param low_resolution_band_names: band names of the low resolution data source in the order that they should be
        matched to ``high_resolution_band_names``.
    :param high_resolution_band_names: band names of the high resolution data source in the order that they should be
        matched to ``low_resolution_band_names``.
    :param target_band_names: Names to be assigned to the output data cube, same order as
        ``low_resolution_band_names`` and ``high_resolution_band_names``.
    """


    high_resolution_mosaic_matched_band_names = high_resolution_mosaic.rename_labels("bands", low_resolution_band_names, high_resolution_band_names)
    fused = low_resolution_interpolated + high_resolution_mosaic_matched_band_names - low_resolution_mosaic
    fused_renamed = fused.rename_labels(target_band_names, low_resolution_band_names)
    return fused_renamed