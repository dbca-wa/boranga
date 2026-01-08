from enum import Enum


class Source(str, Enum):
    TEC = "TEC"
    TEC_SITES = "TEC_SITES"
    TEC_SURVEYS = "TEC_SURVEYS"
    TPFL = "TPFL"
    TFAUNA = "TFAUNA"


ALL_SOURCES = [s.value for s in Source]
