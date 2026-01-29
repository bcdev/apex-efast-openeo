import openeo

def load_and_scale(connection: openeo.Connection, use_binning: bool = False, binning_params: dict | None=None, **kwargs):
    """
    Applies offset and scale factor to a cube right after load_collection.
    Offset and scale factor are assumed to be constant across bands.

    :param connection: authenticated openeo connection
    :param use_binning: use binning if set, otherwise use nearest-neighbour
    :param binning_params: feature flags for the binning implementation in the OpenEO backend
    :param kwargs: Keyword arguments to be passed to the load_collection process
    """

    metadata = connection.describe_collection(kwargs["collection_id"])

    scale = (
        metadata.get("summaries", {}).get("raster:bands", [{}])[0].get("scale", None)
    )
    offset = (
        metadata.get("summaries", {}).get("raster:bands", [{}])[0].get("offset", None)
    )
    if scale is None:
        scale = metadata.get("summaries", {}).get("eo:bands", [{}])[0].get("scale", 1.0)
    if offset is None:
        offset = (
            metadata.get("summaries", {}).get("eo:bands", [{}])[0].get("offset", 1.0)
        )

    cube = connection.load_collection(**kwargs)
    if use_binning:
        binning_params = binning_params or {}
        cube.result_node().update_arguments(
            featureflags=dict(
                reprojection_type="binning",
                **binning_params,
            )
        )
    elif binning_params is not None:
        raise ValueError(f"Binning parameters have been set to '{binning_params}', but 'use_binning' is '{use_binning}'")
    cube_scaled = cube.apply(lambda x: (x + offset) * scale)
    return cube_scaled
