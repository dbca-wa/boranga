from enum import Enum

from boranga.components.species_and_communities.models import GroupType


class Source(str, Enum):
    TEC = "TEC"
    TEC_SITE_VISITS = "TEC_SITE_VISITS"
    TEC_SITE_SPECIES = "TEC_SITE_SPECIES"
    TEC_SURVEYS = "TEC_SURVEYS"
    TEC_SURVEY_THREATS = "TEC_SURVEY_THREATS"
    TEC_BOUNDARIES = "TEC_BOUNDARIES"
    TPFL = "TPFL"
    TFAUNA = "TFAUNA"


SOURCE_GROUP_TYPE_MAP = {
    Source.TPFL.value: GroupType.GROUP_TYPE_FLORA,
    Source.TEC.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TEC_SITE_VISITS.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TEC_SITE_SPECIES.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TEC_SURVEYS.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TEC_SURVEY_THREATS.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TEC_BOUNDARIES.value: GroupType.GROUP_TYPE_COMMUNITY,
    Source.TFAUNA.value: GroupType.GROUP_TYPE_FAUNA,
}


ALL_SOURCES = [s.value for s in Source]
