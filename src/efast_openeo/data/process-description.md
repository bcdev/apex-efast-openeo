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
