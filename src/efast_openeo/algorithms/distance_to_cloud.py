from efast_openeo.constants import S2Scl

def distance_to_cloud_s2(scl_s2):
    #return scl_s2 != S2Scl.CLOUD_HIGH
    return scl_s2 != 0
