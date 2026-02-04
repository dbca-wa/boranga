from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.data_migration.registry import _result
from boranga.components.main.models import LegacyUsernameEmailuserMapping

DUMMY_TEC_USER = "boranga.tec@dbca.wa.gov.au"

# Cache for the dummy user ID and legacy user mappings
_DUMMY_TEC_USER_ID = None
_DUMMY_SEARCHED = False
_TEC_USER_MAPPING_CACHE = None  # Dict[username.lower(), emailuser_id]
_SITE_VISIT_TO_SID_CACHE = None  # Dict[site_visit_id, s_id]


def _ensure_caches_loaded():
    global _DUMMY_TEC_USER_ID, _TEC_USER_MAPPING_CACHE, _DUMMY_SEARCHED

    # Load Dummy User
    if not _DUMMY_SEARCHED:
        try:
            u = EmailUserRO.objects.get(email__iexact=DUMMY_TEC_USER)
            _DUMMY_TEC_USER_ID = u.id
        except EmailUserRO.DoesNotExist:
            pass  # Will handle as None in lookup
        finally:
            _DUMMY_SEARCHED = True

    # Load Mappings for TEC system
    if _TEC_USER_MAPPING_CACHE is None:
        _TEC_USER_MAPPING_CACHE = {}
        # Bulk load all user mappings for TEC to avoid N+1
        mappings = LegacyUsernameEmailuserMapping.objects.filter(legacy_system__iexact="TEC").values(
            "legacy_username", "emailuser_id"
        )

        for m in mappings:
            uname = m["legacy_username"].strip().lower() if m["legacy_username"] else ""
            if uname:
                _TEC_USER_MAPPING_CACHE[uname] = m["emailuser_id"]


def tec_user_lookup(value, ctx):
    """
    Resolve TEC user using in-memory caching.
    - If mapped from Survey USERNAME or CREATED_BY (and not empty): try lookup using cache.
    - If lookup returns None (or value was empty, like in Site Visit), fallback to Dummy user.
    """
    _ensure_caches_loaded()

    # 1. Try standard lookup if value is present
    if value:
        val_str = str(value).strip().lower()
        if val_str in _TEC_USER_MAPPING_CACHE:
            return _result(_TEC_USER_MAPPING_CACHE[val_str])

    # 2. Fallback to Dummy User
    return _result(_DUMMY_TEC_USER_ID)


TEC_USER_LOOKUP = tec_user_lookup


def load_site_visit_to_sid_mapping(site_visits_path: str) -> dict[str, str]:
    """
    Load SITE_VISIT_ID -> S_ID mapping from SITE_VISITS.csv with caching.

    Args:
        site_visits_path: Path to SITE_VISITS.csv file

    Returns:
        Dictionary mapping SITE_VISIT_ID (as string) to S_ID
    """
    global _SITE_VISIT_TO_SID_CACHE

    if _SITE_VISIT_TO_SID_CACHE is not None:
        return _SITE_VISIT_TO_SID_CACHE

    import logging
    import os

    import pandas as pd

    logger = logging.getLogger(__name__)
    _SITE_VISIT_TO_SID_CACHE = {}

    if not os.path.exists(site_visits_path):
        logger.warning(f"SITE_VISITS.csv not found at {site_visits_path}")
        return _SITE_VISIT_TO_SID_CACHE

    try:
        df = pd.read_csv(site_visits_path, dtype=str)
        df = df.fillna("")

        for _, row in df.iterrows():
            sv_id = row.get("SITE_VISIT_ID", "").strip()
            s_id = row.get("S_ID", "").strip()
            if sv_id and s_id:
                _SITE_VISIT_TO_SID_CACHE[sv_id] = s_id

        logger.info(f"Loaded {len(_SITE_VISIT_TO_SID_CACHE)} SITE_VISIT_ID -> S_ID mappings from {site_visits_path}")
    except Exception as e:
        logger.error(f"Failed to load SITE_VISITS.csv: {e}")

    return _SITE_VISIT_TO_SID_CACHE


_SID_TO_SITE_VISIT_ROWS_CACHE = None  # Dict[s_id, List[dict]]


def load_sid_to_site_visit_rows(site_visits_path: str):
    """
    Loads SITE_VISITS.csv and indexes rows by S_ID.
    Returns:
        Dict mapping S_ID (string) to list of raw row dicts
    """
    global _SID_TO_SITE_VISIT_ROWS_CACHE

    if _SID_TO_SITE_VISIT_ROWS_CACHE is not None:
        return _SID_TO_SITE_VISIT_ROWS_CACHE

    import logging
    import os

    import pandas as pd

    logger = logging.getLogger(__name__)
    _SID_TO_SITE_VISIT_ROWS_CACHE = {}

    if not os.path.exists(site_visits_path):
        logger.warning(f"SITE_VISITS.csv not found at {site_visits_path}")
        return _SID_TO_SITE_VISIT_ROWS_CACHE

    try:
        df = pd.read_csv(site_visits_path, dtype=str)
        df = df.fillna("")

        for _, row in df.iterrows():
            s_id = row.get("S_ID", "").strip()
            if s_id:
                if s_id not in _SID_TO_SITE_VISIT_ROWS_CACHE:
                    _SID_TO_SITE_VISIT_ROWS_CACHE[s_id] = []
                # Convert to plain dict
                _SID_TO_SITE_VISIT_ROWS_CACHE[s_id].append(row.to_dict())

        logger.info(f"Loaded visits for {len(_SID_TO_SITE_VISIT_ROWS_CACHE)} sites from {site_visits_path}")
    except Exception as e:
        logger.error(f"Failed to load SITE_VISITS.csv: {e}")

    return _SID_TO_SITE_VISIT_ROWS_CACHE
