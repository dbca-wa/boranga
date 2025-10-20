import logging
import os
import sys

import confy
from confy import env
from decouple import Csv, config

logger = logging.getLogger(__name__)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.exists(BASE_DIR + "/.env"):
    confy.read_environment_file(BASE_DIR + "/.env")
os.environ.setdefault("BASE_DIR", BASE_DIR)

from ledger_api_client.settings_base import *  # noqa: F403

WORKING_FROM_HOME = env("WORKING_FROM_HOME", False)

ROOT_URLCONF = "boranga.urls"
SITE_ID = 1
DEPT_DOMAINS = env("DEPT_DOMAINS", ["dpaw.wa.gov.au", "dbca.wa.gov.au"])
SYSTEM_MAINTENANCE_WARNING = env("SYSTEM_MAINTENANCE_WARNING", 24)  # hours
DISABLE_EMAIL = env("DISABLE_EMAIL", False)
SHOW_TESTS_URL = env("SHOW_TESTS_URL", False)
SHOW_DEBUG_TOOLBAR = env("SHOW_DEBUG_TOOLBAR", False)
TIME_ZONE = "Australia/Perth"

SILENCE_SYSTEM_CHECKS = env("SILENCE_SYSTEM_CHECKS", False)
if SILENCE_SYSTEM_CHECKS:
    SILENCED_SYSTEM_CHECKS = ["fields.W903", "fields.W904", "debug_toolbar.W004"]

TEMPLATE_TITLE = "Boranga System"
LEDGER_TEMPLATE = "bootstrap5"
EMAIL_DELIVERY = env("EMAIL_DELIVERY", "off")
EMAIL_INSTANCE = env("EMAIL_INSTANCE", "DEV")

# Use these two admin group names as they are referred to in dbca templates
DJANGO_ADMIN_GROUP = env("DJANGO_ADMIN_GROUP", "Django Admin")
# ----------------------------------------------

GROUP_NAME_CONSERVATION_STATUS_ASSESSOR = "Conservation Status Assessors"
GROUP_NAME_CONSERVATION_STATUS_APPROVER = "Conservation Status Approvers"

GROUP_NAME_EXTERNAL_CONTRIBUTOR = "External Contributors"
GROUP_NAME_INTERNAL_CONTRIBUTOR = "Internal Contributors"

GROUP_NAME_OCCURRENCE_APPROVER = "Occurrence Approvers"
GROUP_NAME_OCCURRENCE_ASSESSOR = "Occurrence Assessors"

GROUP_NAME_READONLY_USER = "Read Only Users"

GROUP_NAME_SPECIES_COMMUNITIES_APPROVER = "Species and Communities Approvers"

GROUP_NAME_CHOICES = [
    DJANGO_ADMIN_GROUP,
    GROUP_NAME_CONSERVATION_STATUS_ASSESSOR,
    GROUP_NAME_CONSERVATION_STATUS_APPROVER,
    GROUP_NAME_EXTERNAL_CONTRIBUTOR,
    GROUP_NAME_INTERNAL_CONTRIBUTOR,
    GROUP_NAME_OCCURRENCE_APPROVER,
    GROUP_NAME_OCCURRENCE_ASSESSOR,
    GROUP_NAME_READONLY_USER,
    GROUP_NAME_SPECIES_COMMUNITIES_APPROVER,
]

# This settings is used for security checks in which case an internal contributor is not considered 'internal'
INTERNAL_GROUPS = [
    g
    for g in GROUP_NAME_CHOICES
    if g != GROUP_NAME_EXTERNAL_CONTRIBUTOR and g != GROUP_NAME_INTERNAL_CONTRIBUTOR
]

# This settings is used in django admin to prevent internal users from being added to external groups and visa versa
# I.e. we don't want external users added to the internal contributor group
GROUPS_THAT_ALLOW_INTERNAL_MEMBERS_ONLY = [
    g for g in GROUP_NAME_CHOICES if g != GROUP_NAME_EXTERNAL_CONTRIBUTOR
]

if env("CONSOLE_EMAIL_BACKEND", False):
    EMAIL_BACKEND = "wagov_utils.components.utils.email_backend.EmailBackend"


if SHOW_DEBUG_TOOLBAR:

    def show_toolbar(request):
        return True

    MIDDLEWARE_CLASSES += [
        "debug_toolbar.middleware.DebugToolbarMiddleware",
    ]
    INSTALLED_APPS += ("debug_toolbar",)
    INTERNAL_IPS = ("127.0.0.1", "localhost")

    # this dict removes check to dtermine if toolbar should display --> works for rks docker container
    DEBUG_TOOLBAR_CONFIG = {
        "SHOW_TOOLBAR_CALLBACK": show_toolbar,
        "INTERCEPT_REDIRECTS": False,
    }

STATIC_URL = "/static/"

INSTALLED_APPS += [
    "webtemplate_dbca",
    "boranga",
    "boranga.components.main",
    "boranga.components.species_and_communities",
    "boranga.components.conservation_status",
    "boranga.components.users",
    "boranga.components.meetings",
    "boranga.components.occurrence",
    "boranga.components.spatial",
    "csvexport",
    "taggit",
    "rest_framework",
    "rest_framework_datatables",
    "rest_framework_gis",
    "reset_migrations",
    "multiselectfield",
    "import_export",
    "ledger_api_client",
    "appmonitor_client",
    "reversion",
    "reversion_compare",
    "nested_admin",
    "colorfield",
    "django_filters",
    "ordered_model",
    "django_vite",
]

ADD_REVERSION_ADMIN = True

WSGI_APPLICATION = "boranga.wsgi.application"

if DEBUG:
    rest_framework_renderer_classes = (
        "rest_framework.renderers.JSONRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
        "rest_framework_datatables.renderers.DatatablesRenderer",
    )
else:
    rest_framework_renderer_classes = (
        "rest_framework.renderers.JSONRenderer",
        "rest_framework_datatables.renderers.DatatablesRenderer",
    )

REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": rest_framework_renderer_classes,
    "EXCEPTION_HANDLER": "boranga.exceptions.custom_exception_handler",
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    "DEFAULT_PAGINATION_CLASS": "rest_framework_datatables.pagination.DatatablesPageNumberPagination",
    "PAGE_SIZE": 100,
}

MIDDLEWARE_CLASSES += [
    "boranga.middleware.FirstTimeNagScreenMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
]
MIDDLEWARE = MIDDLEWARE_CLASSES
MIDDLEWARE_CLASSES = None

TEMPLATES[0]["DIRS"].append(os.path.join(BASE_DIR, "boranga", "templates"))
TEMPLATES[0]["DIRS"].append(
    os.path.join(BASE_DIR, "boranga", "components", "organisations", "templates")
)
TEMPLATES[0]["DIRS"].append(
    os.path.join(BASE_DIR, "boranga", "components", "emails", "templates")
)

TEMPLATES[0]["OPTIONS"]["context_processors"].append(
    "boranga.context_processors.config"
)

del BOOTSTRAP3["css_url"]
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.filebased.FileBasedCache",
        "LOCATION": os.path.join(BASE_DIR, "boranga", "cache"),
    }
}
STATIC_ROOT = os.path.join(BASE_DIR, "staticfiles")
STATICFILES_DIRS.append(os.path.join(os.path.join(BASE_DIR, "boranga", "static")))
STATICFILES_DIRS.append(
    os.path.join(os.path.join(BASE_DIR, "boranga", "static", "boranga_vue"))
)
DATA_UPLOAD_MAX_NUMBER_FIELDS = None
DATA_UPLOAD_MAX_MEMORY_SIZE = 20 * 1024 * 1024

# Department details
SYSTEM_NAME = "Boranga System"
SYSTEM_NAME_SHORT = env("SYSTEM_NAME_SHORT", "BGA")
SITE_PREFIX = env("SITE_PREFIX")
SITE_DOMAIN = env("SITE_DOMAIN")

# These are used for converting between internal and external urls
# This is useful when generating urls for emails when the current request user
# is external and the link needs to be internal or vis versa
# To get email links to work locally, set SITE_SUBDOMAIN_INTERNAL_SUFFIX=None in .env
SITE_RANCHER_CLUSTER = env("SITE_RANCHER_CLUSTER", None)
SITE_SUBDOMAIN_INTERNAL_SUFFIX = env("SITE_SUBDOMAIN_INTERNAL_SUFFIX", "-internal")
if SITE_RANCHER_CLUSTER:
    SITE_SUBDOMAIN_INTERNAL_SUFFIX += "-" + SITE_RANCHER_CLUSTER


SUPPORT_EMAIL = env("SUPPORT_EMAIL", "bio@" + SITE_DOMAIN).lower()
DEP_URL = env("DEP_URL", "www." + SITE_DOMAIN)
DEP_PHONE = env("DEP_PHONE", "(08) 9219 9978")
DEP_PHONE_SUPPORT = env("DEP_PHONE_SUPPORT", "(08) 9219 9000")
DEP_FAX = env("DEP_FAX", "(08) 9423 8242")
DEP_POSTAL = env(
    "DEP_POSTAL", "Locked Bag 104, Bentley Delivery Centre, Western Australia 6983"
)
DEP_NAME = env("DEP_NAME", "Department of Biodiversity, Conservation and Attractions")
DEP_NAME_SHORT = env("DEP_NAME_SHORT", "DBCA")
BRANCH_NAME = env("BRANCH_NAME", "Tourism and Concessions Branch")
DEP_ADDRESS = env("DEP_ADDRESS", "17 Dick Perry Avenue, Kensington WA 6151")
SITE_URL = env("SITE_URL", "https://" + SITE_PREFIX + "." + SITE_DOMAIN)
PUBLIC_URL = env("PUBLIC_URL", SITE_URL)
DEFAULT_FROM_EMAIL = env("DEFAULT_FROM_EMAIL", "no-reply@" + SITE_DOMAIN).lower()
MEDIA_APP_DIR = env("MEDIA_APP_DIR", "boranga")
CRON_RUN_AT_TIMES = env("CRON_RUN_AT_TIMES", "04:05")
CRON_EMAIL = env("CRON_EMAIL", "cron@" + SITE_DOMAIN).lower()
EMAIL_FROM = DEFAULT_FROM_EMAIL

CRON_CLASSES = [
    "appmonitor_client.cron.CronJobAppMonitorClient",
    "boranga.cron.CronJobFetchNomosTaxonDataDaily",
    "boranga.cron.CronJobOCRPreProcessBulkImportTasks",
    "boranga.cron.CronJobOCRProcessBulkImportQueue",
    "boranga.cron.CronJobAutoLockConservationStatusRecords",
    "boranga.cron.CronJobAutoLockUnlockedOccurrenceRecords",
]


BASE_URL = env("BASE_URL")

# Additional logging for boranga
LOGGING["loggers"]["boranga"] = {"handlers": ["file"], "level": "INFO"}
if DEBUG:
    LOGGING["formatters"] = {
        "verbose": {
            "format": "%(levelname)s %(asctime)s %(name)s [Line:%(lineno)s][%(funcName)s] %(message)s"
        },
        "simple": {
            "format": "[Line:%(lineno)s][%(funcName)s] %(levelname)s %(message)s"
        },
    }
    LOGGING["loggers"]["boranga"] = {
        "handlers": ["console"],
        "level": "DEBUG",
        "formatter": "verbose",
        "propagate": False,
    }

    # Get rid of the annoying asyncio info log message
    LOGGING["loggers"]["asyncio"] = {
        "level": "WARNING",
    }

DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

# Use git commit hash for purging cache in browser for deployment changes
GIT_COMMIT_HASH = os.environ.get("GIT_COMMIT", os.environ.get("COMMIT", "unknown"))

APPLICATION_VERSION = env("APPLICATION_VERSION", "1.0.0") + "-" + GIT_COMMIT_HASH[:7]

RUNNING_DEVSERVER = len(sys.argv) > 1 and sys.argv[1] == "runserver"

# Sentry settings
SENTRY_DSN = env("SENTRY_DSN", default=None)
SENTRY_SAMPLE_RATE = env("SENTRY_SAMPLE_RATE", default=1.0)  # Error sampling rate
SENTRY_TRANSACTION_SAMPLE_RATE = env(
    "SENTRY_TRANSACTION_SAMPLE_RATE", default=0.0
)  # Transaction sampling
if not RUNNING_DEVSERVER and SENTRY_DSN and EMAIL_INSTANCE:
    import sentry_sdk

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        sample_rate=SENTRY_SAMPLE_RATE,
        traces_sample_rate=SENTRY_TRANSACTION_SAMPLE_RATE,
        environment=EMAIL_INSTANCE.lower(),
        release=APPLICATION_VERSION,
    )

LEDGER_UI_ACCOUNTS_MANAGEMENT = [
    {"first_name": {"options": {"view": True, "edit": True}}},
    {"last_name": {"options": {"view": True, "edit": True}}},
    {"phone_number": {"options": {"view": True, "edit": True}}},
    {"mobile_number": {"options": {"view": True, "edit": True}}},
]

LEDGER_UI_ACCOUNTS_MANAGEMENT_KEYS = []
for am in LEDGER_UI_ACCOUNTS_MANAGEMENT:
    LEDGER_UI_ACCOUNTS_MANAGEMENT_KEYS.append(list(am.keys())[0])

RECENT_REFERRAL_COUNT = env("RECENT_REFERRAL_COUNT", 5)

#  ------------- NOMOS JSON file url --------------
NOMOS_BLOB_URL = env("NOMOS_BLOB_URL")

GIS_SERVER_URL = env(
    "GIS_SERVER_URL", "https://kaartdijin-boodja-geoserver.dbca.wa.gov.au/geoserver/ows"
)

# Proxy prefix for basic authentication
BASIC_AUTH_PROXY_PREFIX = env("BASIC_AUTH_PROXY_PREFIX", "kb-proxy/")


# Set USE_X_FORWARDED_HOST env to True to ensure that if the request is https
# then urls generated for file fields are also https
USE_X_FORWARDED_HOST = env("USE_X_FORWARDED_HOST", False)
if USE_X_FORWARDED_HOST:
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

SESSION_COOKIE_SECURE = env("SESSION_COOKIE_SECURE", True)
CSRF_COOKIE_SECURE = env("CSRF_COOKIE_SECURE", True)
CSRF_TRUSTED_ORIGINS = config("CSRF_TRUSTED_ORIGINS", default="", cast=Csv())
SESSION_COOKIE_AGE = env("SESSION_COOKIE_AGE", 60 * 60 * 24)  # 24 hours

SECURE_CROSS_ORIGIN_OPENER_POLICY = env(
    "SECURE_CROSS_ORIGIN_OPENER_POLICY",
    "same-origin",
)


# Make sure this returns true when in local development
# so you can use the vite dev server with hot module reloading
DJANGO_VITE_DEV_MODE = RUNNING_DEVSERVER and EMAIL_INSTANCE == "DEV" and DEBUG is True

STATIC_URL_PREFIX = "/static/boranga_vue/" if DJANGO_VITE_DEV_MODE else "boranga_vue/"

DJANGO_VITE = {
    "default": {
        "dev_mode": DJANGO_VITE_DEV_MODE,
        "manifest_path": os.path.join(
            BASE_DIR, "boranga", "static", "boranga_vue", "manifest.json"
        ),
        "dev_server_host": "localhost",  # Default host for vite (can change if needed)
        "dev_server_port": 5173,  # Default port for vite (can change if needed)
        "static_url_prefix": STATIC_URL_PREFIX,
    }
}
VUE3_ENTRY_SCRIPT = env(
    "VUE3_ENTRY_SCRIPT",
    default="src/main.js",  # This path will be auto prefixed with the static_url_prefix from DJANGO_VITE above
)  # Path of the vue3 entry point script served by vite


# ---------- Identifier fields for logging ----------

""" Fields that the logging functions will check for on the instance
    and use to identify the instance in the logs. """
ACTION_LOGGING_IDENTIFIER_FIELDS = [
    "lodgement_number",
    "occurrence_number",
    "occurrence_report_number",
    "id",
]

# ---------- User Log Actions ----------

ACTION_VIEW = "View {} {}"
ACTION_CREATE = "Create {} {}"
ACTION_UPDATE = "Update {} {}"
ACTION_DESTROY = "Destroy {} {}"

# ---------- Cache keys ----------
CACHE_TIMEOUT_5_SECONDS = 5
CACHE_TIMEOUT_10_SECONDS = 10
CACHE_TIMEOUT_1_MINUTE = 60
CACHE_TIMEOUT_5_MINUTES = 60 * 5
CACHE_TIMEOUT_2_HOURS = 60 * 60 * 2
CACHE_TIMEOUT_24_HOURS = 60 * 60 * 24
CACHE_TIMEOUT_NEVER = None

CACHE_KEY_EPSG_CODES = "epsg-codes-{auth_name}-{pj_type}"
CACHE_KEY_PROXY_LAYER_DATA = "proxy-layer-data-{app_label}-{model_name}"
CACHE_KEY_PROXY_NODE_DATA = "proxy-node-data-{request_path}"
CACHE_KEY_MAP_OCCURRENCES = "map-occurrences"
CACHE_KEY_MAP_OCCURRENCE_REPORTS = "map-occurrence-reports"
CACHE_KEY_USER_BELONGS_TO_GROUP = "user-{user_id}-belongs-to-{group_name}"
CACHE_KEY_USER_IS_REFEREE = "user-{user_id}-is-referee-{model}-{pk}"
CACHE_KEY_SUPERUSER_IDS = "superuser-ids"
CACHE_KEY_FILE_EXTENSION_WHITELIST = "file-extension-whitelist"
CACHE_KEY_PLAUSIBILITY_GEOMETRY = "plausibility-geometry-{geometry_model}"

# ---------- Conservation Change Codes ----------

CONSERVATION_CHANGE_CODE_NEW = "New"
CONSERVATION_CHANGE_CODE_UPGRADE = "Upgrade"
CONSERVATION_CHANGE_CODE_DOWNGRADE = "Downgrade"
CONSERVATION_CHANGE_CODE_NAME_CHANGE = "Name Change"
CONSERVATION_CHANGE_CODE_RANK_CHANGE = "Rank Change"
CONSERVATION_CHANGE_CODE_CATEGORY_CHANGE = "Category Change"
CONSERVATION_CHANGE_CODE_CRITERIA_CHANGE = "Criteria Change"
CONSERVATION_CHANGE_CODE_DELIST = "DeList"
CONSERVATION_CHANGE_CODE_CLOSE = "Close"
CONSERVATION_CHANGE_CODE_OTHER = "Other"

CONSERVATION_CHANGE_CODES = [
    {"code": CONSERVATION_CHANGE_CODE_NEW, "label": "New"},
    {"code": CONSERVATION_CHANGE_CODE_UPGRADE, "label": "Upgrade"},
    {"code": CONSERVATION_CHANGE_CODE_DOWNGRADE, "label": "Downgrade"},
    {"code": CONSERVATION_CHANGE_CODE_NAME_CHANGE, "label": "Name Change"},
    {"code": CONSERVATION_CHANGE_CODE_RANK_CHANGE, "label": "Rank Change"},
    {"code": CONSERVATION_CHANGE_CODE_CATEGORY_CHANGE, "label": "Category Change"},
    {"code": CONSERVATION_CHANGE_CODE_CRITERIA_CHANGE, "label": "Criteria Change"},
    {"code": CONSERVATION_CHANGE_CODE_DELIST, "label": "DeList"},
    {"code": CONSERVATION_CHANGE_CODE_CLOSE, "label": "Close"},
    {"code": CONSERVATION_CHANGE_CODE_OTHER, "label": "Other"},
]

# ---------- Conservation Lists / Categories ----------

WA_LEGISLATIVE_LIST_WCA = {"code": "WCA", "label": "Wildlife Conservation Act 1950"}
WA_LEGISLATIVE_LIST_BCA = {
    "code": "BCA",
    "label": "Biodiversity Conservation Act 2016 + BCA",
}

WA_LEGISLATIVE_LISTS = [WA_LEGISLATIVE_LIST_WCA, WA_LEGISLATIVE_LIST_BCA]

WA_LEGISLATIVE_CATEGORY_CR = {"code": "CR", "label": "Critically Endangered"}
WA_LEGISLATIVE_CATEGORY_EN = {"code": "EN", "label": "Endangered"}
WA_LEGISLATIVE_CATEGORY_VU = {"code": "VU", "label": "Vulnerable"}
WA_LEGISLATIVE_CATEGORY_EX = {"code": "EX", "label": "Extinct"}
WA_LEGISLATIVE_CATEGORY_EW = {"code": "EW", "label": "Extinct in the Wild"}
WA_LEGISLATIVE_CATEGORY_CO = {"code": "CO", "label": "Collapsed"}
WA_LEGISLATIVE_CATEGORY_RD = {"code": "RD", "label": "Rediscovered"}
WA_LEGISLATIVE_CATEGORY_SP_SI = {
    "code": "SP_SI",
    "label": "Specially Protected - special conservation interest",
}
WA_LEGISLATIVE_CATEGORY_SP_MS = {
    "code": "SP_MS",
    "label": "Specially Protected - migratory species",
}
WA_LEGISLATIVE_CATEGORY_SP_C = {
    "code": "SP_C",
    "label": "Specially Protected - cetaceans",
}
WA_LEGISLATIVE_CATEGORY_SP_IA = {
    "code": "SP_IA",
    "label": "Specially Protected - international agreement",
}

WA_LEGISLATIVE_CATEGORIES = [
    WA_LEGISLATIVE_CATEGORY_CR,
    WA_LEGISLATIVE_CATEGORY_EN,
    WA_LEGISLATIVE_CATEGORY_VU,
    WA_LEGISLATIVE_CATEGORY_EX,
    WA_LEGISLATIVE_CATEGORY_EW,
    WA_LEGISLATIVE_CATEGORY_CO,
    WA_LEGISLATIVE_CATEGORY_RD,
    WA_LEGISLATIVE_CATEGORY_SP_SI,
    WA_LEGISLATIVE_CATEGORY_SP_MS,
    WA_LEGISLATIVE_CATEGORY_SP_C,
    WA_LEGISLATIVE_CATEGORY_SP_IA,
]

WA_PRIORITY_LIST_FLORA = {"code": "FLORA", "label": "Flora"}
WA_PRIORITY_LIST_FAUNA = {"code": "FAUNA", "label": "Fauna"}
WA_PRIORITY_LIST_COMMUNITIES = {"code": "COMMUNITIES", "label": "Communities"}

WA_PRIORITY_LISTS = [
    WA_PRIORITY_LIST_FLORA,
    WA_PRIORITY_LIST_FAUNA,
    WA_PRIORITY_LIST_COMMUNITIES,
]

WA_PRIORITY_CATEGORY_P1 = {"code": "P1", "label": "Priority 1"}
WA_PRIORITY_CATEGORY_P2 = {"code": "P2", "label": "Priority 2"}
WA_PRIORITY_CATEGORY_P3 = {"code": "P3", "label": "Priority 3"}
WA_PRIORITY_CATEGORY_P4 = {"code": "P4", "label": "Priority 4"}

WA_PRIORITY_CATEGORIES = [
    WA_PRIORITY_CATEGORY_P1,
    WA_PRIORITY_CATEGORY_P2,
    WA_PRIORITY_CATEGORY_P3,
    WA_PRIORITY_CATEGORY_P4,
]

COMMONWEALTH_CONSERVATION_LIST_EPBC = {
    "code": "EPBC",
    "label": "Environment Protection and Biodiversity Conservation Act 1999",
}

COMMONWEALTH_CONSERVATION_LISTS = [COMMONWEALTH_CONSERVATION_LIST_EPBC]

# This is needed so that the chmod is not called in django/core/files/storage.py
# (_save method of FileSystemStorage class)
# As it causes a permission exception when using azure network drives
FILE_UPLOAD_PERMISSIONS = None

OCR_BULK_IMPORT_TASK_TIMEOUT_SECONDS = env(
    "OCR_BULK_IMPORT_TASK_TIMEOUT_SECONDS", 60 * 5
)  # Default = 5 minutes

OCR_BULK_IMPORT_PROCESS_TASKS_IMMEDIATELY = env(
    "OCR_BULK_IMPORT_PROCESS_TASKS_IMMEDIATELY", False
)  # used to make testing easier (i.e. set to True in local env) NEVER set to True in production!

OCR_BULK_IMPORT_LOOKUP_TABLE_DISPLAY_FIELDS = [
    "name",
    "label",
    "display_name",
    "code",
]

OCR_BULK_IMPORT_LOOKUP_TABLE_RECORD_LIMIT = env(
    "OCR_BULK_IMPORT_LOOKUP_TABLE_RECORD_LIMIT", 30
)

OCR_BULK_IMPORT_M2M_DELIMITER = env("OCR_BULK_IMPORT_M2M_DELIMITER", "||")

COUNT_STATUS_NOT_COUNTED = "not_counted"
COUNT_STATUS_COUNTED = "detailed_count"
COUNT_STATUS_SIMPLE_COUNT = "simple_count"
COUNT_STATUS_CHOICES = [
    (COUNT_STATUS_NOT_COUNTED, "Not Counted"),
    (COUNT_STATUS_COUNTED, "Detailed Count"),
    (COUNT_STATUS_SIMPLE_COUNT, "Simple Count"),
]

# Django admin csv export settings
CSV_EXPORT_REFERENCE_DEPTH = config("CSV_EXPORT_REFERENCE_DEPTH", default=3, cast=int)
CSV_EXPORT_EMPTY_VALUE = config("CSV_EXPORT_EMPTY_VALUE", default="")

CSV_EXPORT_DELIMITER = config("CSV_EXPORT_DELIMITER", default=",")
CSV_EXPORT_ESCAPECHAR = config("CSV_EXPORT_ESCAPECHAR", default="\\")
CSV_EXPORT_QUOTECHAR = config("CSV_EXPORT_ESCAPECHAR", default='"')
CSV_EXPORT_DOUBLEQUOTE = config("CSV_EXPORT_DOUBLEQUOTE", default=True, cast=bool)
CSV_EXPORT_LINETERMINATOR = config("CSV_EXPORT_LINETERMINATOR", default=r"\n")
CSV_EXPORT_QUOTING = config("CSV_EXPORT_QUOTING", default="QUOTE_ALL")

DEFAULT_SELECT2_RECORDS_LIMIT = config(
    "DEFAULT_SELECT2_RECORDS_LIMIT", default=20, cast=int
)

GIS_EXTENT = config(
    "GIS_EXTENT",
    default="96.0, -35.0, 129.0, -10.0",
    cast=Csv(float, post_process=tuple),
)

DEFAULT_UNLOCKED_EDITING_WINDOW_MINUTES = config(
    "DEFAULT_UNLOCKED_EDITING_WINDOW_MINUTES", default=30, cast=int
)
UNLOCKED_CONSERVATION_STATUS_EDITING_WINDOW_MINUTES = config(
    "UNLOCKED_CONSERVATION_STATUS_EDITING_WINDOW_MINUTES", default=30, cast=int
)
UNLOCKED_OCCURRENCE_EDITING_WINDOW_MINUTES = config(
    "UNLOCKED_OCCURRENCE_EDITING_WINDOW_MINUTES", default=30, cast=int
)

FETCH_NOMOS_DATA_TIME_OF_DAY = config(
    "FETCH_NOMOS_DATA_TIME_OF_DAY", default="03:00"
)  # 24 hour format HH:MM

INCLUDE_ROOT_VIEW = env("INCLUDE_ROOT_VIEW", False)
