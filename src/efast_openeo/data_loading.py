import openeo

def load_and_scale(connection: openeo.Connection, **kwargs):
    """
    Applies offset and scale factor to a cube right after load_collection.
    Offset and scale factor are assumed to be constant across bands.

    :param connection: authenticated openeo connection
    :param kwargs: Keyword arguments to be passed to the load_collection process
    """

    metadata = connection.describe_collection(kwargs["collection_id"])

    scale = metadata.get("summaries", {}).get("raster:bands", [{}])[0].get("scale", None)
    offset = metadata.get("summaries", {}).get("raster:bands", [{}])[0].get("offset", None)
    if scale is None:
        scale = metadata.get("summaries", {}).get("eo:bands", [{}])[0].get("scale", 1.0)
    if offset is None:
        offset = metadata.get("summaries", {}).get("eo:bands", [{}])[0].get("offset", 1.0)

    cube = connection.load_collection(**kwargs)
    cube_scaled = cube.apply(lambda x: (x + offset) * scale)

    return cube_scaled