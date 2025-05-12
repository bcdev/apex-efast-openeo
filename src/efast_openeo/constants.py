from enum import IntEnum, IntFlag

class S2Scl(IntEnum):
    NO_DATA = 0
    DEFECTIVE = 1
    TOPOGRAPHIC_SHADOW = 2
    CLOUD_SHADOW = 3
    VEGETATION = 4
    NOT_VEGTATED = 5
    WATER = 6
    UNCLASSIFIED = 7
    CLOUD_MEDIUM = 8
    CLOUD_HIGH = 9
    THIN_CIRRUS = 10
    SNOW_ICE = 11


class S3SynCloudFlags(IntFlag):
    CLEAR           = 0b0000
    CLOUD           = 0b0001
    CLOUD_AMBIGUOUS = 0b0010
    CLOUD_MARGIN    = 0b0100
    SNOW_ICE        = 0b1000
