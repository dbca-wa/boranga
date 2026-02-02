from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.data_migration.registry import _result
from boranga.components.main.models import LegacyUsernameEmailuserMapping

DUMMY_TEC_USER = "boranga.tec@dbca.wa.gov.au"

# Cache for the dummy user ID and legacy user mappings
_DUMMY_TEC_USER_ID = None
_DUMMY_SEARCHED = False
_TEC_USER_MAPPING_CACHE = None  # Dict[username.lower(), emailuser_id]


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
