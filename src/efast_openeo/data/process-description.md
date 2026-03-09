# Introduction

The [Efficient Fusion Algorithm Across Spatio-Temporal Scales (EFAST)](https://doi.org/10.3390/rs16111833) [1] is a method to create
time-series with a fine resolution in space and time from a fine spatial but coarse temporal resolution
source (Sentinel-2) and a coarse temporal but fine spatial resolution source (Sentinel-3).

In comparison to other methods (e.g. STARFM), EFAST aims to achieve results outperforming single-source Sentinel-2
time-series interpolation by exploiting change information from Sentinel-3 with minimal computational cost,
assuming homogeneous temporal dynamics.
EFAST was originally designed to accurately capture seasonal vegetation changes in homogeneous areas like range lands
which present long temporal gaps in Sentinel-2 time series during the rainy season.
DHI has published a [Python implementation of the algorithm](https://github.com/DHI-GRAS/efast).
In the context of the ESA funded [APEx initiative](https://apex.esa.int/), the algorithm has been ported to OpenEO
by [Brockmann Consult GmbH](https://www.brockmann-consult.de/) and is implemented in this process graph.

EFAST interpolates Sentinel-2 acquisitions, using a time (temporal distance to target time) and
distance-to-cloud weighted compositing scheme. The Sentinel-3 time-series is incorporated to locally update
the interpolated Sentinel-2 imagery to accurately track vegetation changes between cloud-free Sentinel-2
acquisitions. The method is described in detail in [1].

[1]: Senty, Paul, Radoslaw Guzinski, Kenneth Grogan, et al. “Fast Fusion of Sentinel-2 and Sentinel-3 Time Series over Rangelands.” Remote Sensing 16, no. 11 (2024): 1833. https://doi.org/10.3390/rs16111833

# Usage remarks

- EFAST produces high temporal frequency time-series matching the Sentinel-2 L2A bands which have
corresponding Sentinel-3 OLCI bands with matching centre frequencies. Because the application of EFAST is
focused on NDVI time-series, the UDP includes a parameter (`output_ndvi`) to directly compute the NDVI
for a given temporal and spatial extent.

- It should be noted that OpenEO provides bands from the SENTINEL_L2A collection in integer format. EFAST
converts the data to floating point values for interpolation. Therefore, output bands of EFAST have a
different data type (floating point) than the corresponding SENTINEL_L2A bands.

- Please refer to the [Jupyter notebook example](https://esa-apex.github.io/apex_jupyterlite/lab/index.html?fromURL=https%3A%2F%2Fraw.githubusercontent.com%2Fbcdev%2Fefast-process-graph%2Frefs%2Fheads%2Fmain%2Fnotebooks%2FEFAST_example_jupyterlite.ipynb)
  for an interactive usage example and discussion of the parameters.
  A minimal example can be found in the process-graph repository's
  [readme](https://github.com/bcdev/efast-process-graph/).

## Job Parameters for long time series and large areas of extent

When applying EFAST to long time series and large areas, very large data volumes are processed.
EFAST is used to generate time series of combined high spatial and high temporal resolution, which by nature
results in large volumes of data.

The EFAST process graph is configured by default to support one-year time series at 0.25° by 0.25° spatial tiles for the
processing of 4 spectral bands. When time-series become longer, the `python-memory` job option may need to be increased.
For larger areas, the `executor-memory` job option may need to be increased.
By default, EFAST runs with 4 GB of Python memory and 11 GB of executor memory.
If you are running only small tests, you may want to reduce these values to save credits.

Be careful when setting job options, as setting the executor memory too high or too low may both result in slower
and more expensive jobs.

When running EFAST from the Python client, you can set job options, when creating the job:
```Python
connection = openeo.connect("https://openeo.dataspace.copernicus.eu").authenticate_oidc()
cube = connection.datacube_from_process(
    "efast",
    # ...
)
job = cube.create_job(
    out_format="netcdf",
    title=f"EFAST with job options",
    job_options={ # <----
      "executor-memory": "14G",
      "python-memory": "4G",
      # ...
    }
)
job.start_and_wait()
```

For more on setting job options, see the [CDSE documentation on OpenEO job configuration](https://documentation.dataspace.copernicus.eu/APIs/openEO/job_config.html)
and the Python client's documentation on [create_job](https://open-eo.github.io/openeo-python-client/api.html#openeo.rest.datacube.DataCube.create_job).