from pathlib import Path
import time
import logging

import pytest
from openeo import processes

from efast_openeo.algorithms.distance_to_cloud import compute_cloud_mask_s3
from efast_openeo import constants


@pytest.fixture
def persistent_output_dir(persistent_output_dir_base) -> Path:
    path = persistent_output_dir_base / "s3_cloud_mask"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def openeo_job_image_name():
    return "python311-dev"

@pytest.mark.openeo
@pytest.mark.manual
def test_cloud_masks(persistent_output_dir, connection, aoi_bounding_box, time_frame, openeo_job_image_name):
    bbox = {
        "west": -15.536301,
        "south": 15.288158,
        "east": -14.909093,
        "north": 15.778762,
    }
    temporal_extent = ["2023-09-11", "2023-09-15"]

    s3_band_name = "Syn_Oa08_reflectance"
    s3_flags_nn = connection.load_collection(
        constants.S3_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=temporal_extent,
        bands=[constants.S3_FLAG_BAND],
    ).band(constants.S3_FLAG_BAND)
    s3_cloud_mask_nn = compute_cloud_mask_s3(s3_flags_nn)

    s3_bands_binning = connection.load_collection(
        constants.S3_COLLECTION,
        spatial_extent=bbox,
        temporal_extent=temporal_extent,
        bands=[s3_band_name, constants.S3_FLAG_BAND],
    )

    s3_bands_binning.result_node().update_arguments(featureflags=dict(
        reprojection_type="binning",
        super_sampling=2,
        flag_band=constants.S3_FLAG_BAND,
        flag_bitmask=0xff,
    ))
    s3_cloud_mask_binning = s3_bands_binning.band(s3_band_name).apply(lambda x: processes.is_nodata(x))

    nn_job = s3_cloud_mask_nn.create_job(
        job_options={
            "image-name": openeo_job_image_name,
        },
        out_format="netcdf",
        title="S3 Cloud Mask NN"
    )
    binning_job = s3_cloud_mask_binning.create_job(
        job_options={
            "image-name": openeo_job_image_name
        },
        out_format="netcdf",
        title="S3 Cloud Mask Binning"
    )
    nn_job.start()
    logging.info(f"Started NN job with id '{nn_job.job_id}'")
    binning_job.start()
    logging.info(f"Started Binning job with id '{binning_job.job_id}'")

    nn_status, binning_status = nn_job.status(), binning_job.status()
    incomplete_statuses = ["created", "queued", "running"]
    poll_interval = 40 # seconds

    while nn_status in incomplete_statuses or binning_status in incomplete_statuses:
        time.sleep(poll_interval)
        nn_status, binning_status = nn_job.status(), binning_job.status()
        logging.info(f"[Test cloud masks] NN: {nn_status}\tBinning: {binning_status}")

    nn_job.get_results().download_file(persistent_output_dir / "s3_cloud_mask_nn.nc")
    binning_job.get_results().download_file(persistent_output_dir / "s3_cloud_mask_binning.nc")

    s3_bands_binning.download(persistent_output_dir / "s3_bands_binned.nc")




@pytest.mark.openeo
@pytest.mark.manual
def test_composites():
    pass