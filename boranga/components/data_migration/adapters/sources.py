from enum import Enum

from boranga.components.species_and_communities.models import GroupType


class Source(str, Enum):
    TEC = "TEC"
    TEC_SITES = "TEC_SITES"
    TEC_SURVEYS = "TEC_SURVEYS"
    TEC_BOUNDARIES = "TEC_BOUNDARIES"
    TPFL = "TPFL"
    TFAUNA = "TFAUNA"


SOURCE_GROUP_TYPE_MAP = {
    Source.TPFL.value: GroupType.GROUP_TYPE_FLORA,
    Source.TEC.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TEC_SITES.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TEC_BOUNDARIES.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TFAUNA.value: GroupType.GROUP_TYPE_FAUNA,
}


ALL_SOURCES = [s.value for s in Source]
