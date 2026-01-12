from enum import Enum


class Source(str, Enum):
    TEC = "TEC"
    TEC_SITES = "TEC_SITES"
    TEC_SURVEYS = "TEC_SURVEYS"
    TEC_BOUNDARIES = "TEC_BOUNDARIES"
    TPFL = "TPFL"
    TFAUNA = "TFAUNA"


ALL_SOURCES = [s.value for s in Source]
