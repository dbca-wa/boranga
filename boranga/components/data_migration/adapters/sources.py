import os
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

# Environment-variable prefix used to lock a source from being migrated.
# Only three top-level lock keys exist: TPFL, TEC, TFAUNA.
# Setting MIGRATED_LOCKED_TEC=True locks TEC and all its sub-sources.
# Example: MIGRATED_LOCKED_TPFL=True
MIGRATION_LOCK_ENV_PREFIX = "MIGRATED_LOCKED_"

# Maps every source to the top-level lock key that controls it.
# TEC sub-sources are covered by the single "TEC" lock.
SOURCE_LOCK_KEY_MAP = {
    Source.TPFL.value: "TPFL",
    Source.TFAUNA.value: "TFAUNA",
    Source.TEC.value: "TEC",
    Source.TEC_SITE_VISITS.value: "TEC",
    Source.TEC_SITE_SPECIES.value: "TEC",
    Source.TEC_SURVEYS.value: "TEC",
    Source.TEC_SURVEY_THREATS.value: "TEC",
    Source.TEC_BOUNDARIES.value: "TEC",
}


def is_source_locked(source: str) -> bool:
    """Return True if the given source is locked via its environment variable.

    All TEC sub-sources are controlled by MIGRATED_LOCKED_TEC.
    TPFL and TFAUNA each have their own variable.

    A missing env var is treated as locked (safe default). To unlock,
    explicitly set the variable to False, 0, or no.
    """
    lock_key = SOURCE_LOCK_KEY_MAP.get(source, source)
    env_var = f"{MIGRATION_LOCK_ENV_PREFIX}{lock_key}"
    raw = os.environ.get(env_var, None)
    if raw is None:
        return True  # absent = locked by default
    return raw.strip().lower() not in ("false", "0", "no")


def get_locked_sources(sources: list | None = None) -> list:
    """Return the subset of *sources* (defaults to ALL_SOURCES) that are currently locked."""
    check = sources if sources is not None else ALL_SOURCES
    return [s for s in check if is_source_locked(s)]
