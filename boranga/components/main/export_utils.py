"""
Utilities for exporting dashboard model data to CSV/Excel files.

Used by the ``email_exports`` management command (invoked via the job queue)
to generate email-attached reports for internal users.
"""

import csv
import logging
import os
from datetime import datetime
from io import BytesIO

from django.conf import settings
from django.db.models import Count
from openpyxl import Workbook
from openpyxl.styles import Font

logger = logging.getLogger(__name__)

MAX_NUM_ROWS_MODEL_EXPORT = getattr(settings, "MAX_NUM_ROWS_MODEL_EXPORT", 500000)

# ── helpers ──────────────────────────────────────────────────────────────────


def _ensure_tmp_dir():
    tmp_dir = os.path.join(settings.BASE_DIR, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def _build_display_name(model_label, group_type_label, ext):
    """Build a display filename like 'Boranga - Occurrence - Flora - Report Export.xlsx'."""
    if group_type_label:
        return f"Boranga - {model_label} - {group_type_label} - Report Export.{ext}"
    return f"Boranga - {model_label} - Report Export.{ext}"


def _csv_file(model_label, header, rows, group_type_label=""):
    """Write *rows* to a CSV and return ``(display_name, bytes, mimetype)``."""
    import io

    text_buf = io.StringIO()
    writer = csv.writer(text_buf)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    data = text_buf.getvalue().encode("utf-8")
    display = _build_display_name(model_label, group_type_label, "csv")
    return (display, data, "text/csv")


def _excel_file(model_label, header, rows, group_type_label=""):
    """Write *rows* to an XLSX workbook and return ``(display_name, bytes, mimetype)``."""
    wb = Workbook()
    ws = wb.active
    ws.title = f"{model_label} Report"[:31]
    bold = Font(bold=True)
    ws.append(header)
    for cell in ws[1]:
        cell.font = bold
    for row in rows:
        ws.append([str(v) if v is not None else "" for v in row])
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    display = _build_display_name(model_label, group_type_label, "xlsx")
    return (display, buf.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def _user_name(user_id, cache):
    """Look up a user's full name by ID, with caching."""
    if not user_id:
        return ""
    if user_id not in cache:
        from boranga.ledger_api_utils import retrieve_email_user

        user = retrieve_email_user(user_id)
        cache[user_id] = user.get_full_name() if user else ""
    return cache[user_id]


def _safe(val, default=""):
    """Return *val* if not None, else *default*."""
    return val if val is not None else default


def _fmt_date(val, fmt="%Y-%m-%d"):
    if val is None:
        return ""
    if hasattr(val, "strftime"):
        return val.strftime(fmt)
    return str(val)


def _approved_cs(obj):
    """Return the approved conservation status for a Species/Community, or None."""
    try:
        return obj.conservation_status.get(processing_status="approved")
    except Exception:
        return None


def _parse_date(val):
    """Parse a YYYY-MM-DD string to a ``date`` object, or None on failure."""
    if not val:
        return None
    try:
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _id_list(val):
    """Convert a comma-separated ID string to a list of non-empty stripped strings."""
    if not val:
        return []
    return [x.strip() for x in str(val).split(",") if x.strip()]


def _user_id_from_email(email):
    """Return a user ID from either a numeric ID or an email address. Returns None if not found."""
    if not email and email != 0:
        return None
    val = str(email).strip()
    if not val:
        return None
    # If the value is numeric, use it directly as the user ID
    if val.isdigit():
        return int(val)
    try:
        from ledger_api_client.ledger_models import EmailUserRO

        return EmailUserRO.objects.get(email__iexact=val).id
    except Exception:
        return None


def _approved_cs_field(cs, field_path):
    """Safely traverse dotted *field_path* on a conservation status."""
    if cs is None:
        return ""
    try:
        val = cs
        for part in field_path.split("."):
            val = getattr(val, part, None)
            if val is None:
                return ""
        return val
    except Exception:
        return ""


# ── per-model export functions ───────────────────────────────────────────────


# ── Species ──────────────────────────────────────────────────────────────────

SPECIES_HEADER = [
    "Number",
    "Scientific Name",
    "Common Name",
    "Family",
    "Genus",
    "Informal Group(s)",
    "Fauna Group",
    "Fauna Subgroup",
    "Region(s)",
    "District(s)",
    "WA Legislative List",
    "WA Legislative Category",
    "WA Priority Category",
    "Commonwealth Conservation Category",
    "Other Conservation Assessment",
    "Conservation Criteria",
    "Processing Status",
    "Publishing Status",
]


def get_species_export(filters, limit):
    from boranga.components.species_and_communities.models import Species

    qs = Species.objects.select_related(
        "taxonomy",
        "group_type",
        "species_publishing_status",
        "fauna_group",
        "fauna_sub_group",
    ).prefetch_related(
        "taxonomy__vernaculars",
        "taxonomy__informal_groups__classification_system_fk",
        "regions",
        "districts",
        "conservation_status",
    )
    if filters.get("group_type") and filters["group_type"] != "all":
        qs = qs.filter(group_type__name__iexact=filters["group_type"])
    ps = filters.get("filter_status") or filters.get("processing_status")
    if ps and ps not in ("all", ""):
        qs = qs.filter(processing_status=ps)
    if filters.get("filter_scientific_name"):
        qs = qs.filter(taxonomy__scientific_name__icontains=filters["filter_scientific_name"])
    region_ids = _id_list(filters.get("filter_region"))
    if region_ids:
        qs = qs.filter(regions__id__in=region_ids).distinct()
    district_ids = _id_list(filters.get("filter_district"))
    if district_ids:
        qs = qs.filter(districts__id__in=district_ids).distinct()
    val = filters.get("filter_wa_legislative_list")
    if val and val not in ("all", ""):
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_list_id=val,
        ).distinct()
    val = filters.get("filter_wa_legislative_category")
    if val and val not in ("all", ""):
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_category_id=val,
        ).distinct()
    ids = _id_list(filters.get("filter_wa_priority_category"))
    if ids:
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_priority_category_id__in=ids,
        ).distinct()
    ns = filters.get("filter_name_status")
    if ns and ns not in ("all", ""):
        qs = qs.filter(taxonomy__is_current=(ns.lower() == "true"))
    pub = filters.get("filter_publication_status")
    if pub and pub not in ("all", ""):
        qs = qs.filter(species_publishing_status__species_public=(pub.lower() == "true"))
    if filters.get("filter_commonwealth_relevance") == "true":
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__commonwealth_conservation_category__isnull=False,
        ).distinct()
    if filters.get("filter_international_relevance") == "true":
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__other_conservation_assessment__isnull=False,
        ).distinct()
    cc = filters.get("filter_conservation_criteria")
    if cc:
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__conservation_criteria__icontains=cc,
        ).distinct()
    fg = filters.get("filter_fauna_group")
    if fg and fg not in ("all", ""):
        qs = qs.filter(fauna_group_id=fg)
    fsg = filters.get("filter_fauna_sub_group")
    if fsg and fsg not in ("all", ""):
        qs = qs.filter(fauna_sub_group_id=fsg)
    if filters.get("filter_common_name"):
        qs = qs.filter(taxonomy__vernaculars__vernacular_name__icontains=filters["filter_common_name"]).distinct()
    if filters.get("filter_family"):
        qs = qs.filter(taxonomy__family_name__icontains=filters["filter_family"])
    if filters.get("filter_genus"):
        qs = qs.filter(taxonomy__genera_name__icontains=filters["filter_genus"])
    ig = filters.get("filter_informal_group")
    if ig and ig not in ("all", ""):
        qs = qs.filter(taxonomy__informal_groups__classification_system_fk_id=ig).distinct()
    return list(qs[:limit])


def get_species_export_fields(data, include_group_type=False):
    rows = []
    for obj in data:
        try:
            t = obj.taxonomy
        except Exception:
            t = None
        common_names = ", ".join(t.vernaculars.all().values_list("vernacular_name", flat=True)) if t else ""
        informal = ", ".join(
            ig.classification_system_fk.class_desc
            for ig in (t.informal_groups.all() if t else [])
            if ig.classification_system_fk
        )
        regions = ", ".join(obj.regions.all().values_list("name", flat=True))
        districts = ", ".join(obj.districts.all().values_list("name", flat=True))
        cs = _approved_cs(obj)
        pub = ""
        try:
            pub = "Public" if obj.species_publishing_status.species_public else "Private"
        except Exception:
            pass
        row = [
            _safe(obj.species_number),
            _safe(getattr(t, "scientific_name", "")),
            common_names,
            _safe(getattr(t, "family_name", "")),
            _safe(getattr(t, "genera_name", "")),
            informal,
            _safe(obj.fauna_group.name if obj.fauna_group else ""),
            _safe(obj.fauna_sub_group.name if obj.fauna_sub_group else ""),
            regions,
            districts,
            _approved_cs_field(cs, "wa_legislative_list.code"),
            _approved_cs_field(cs, "wa_legislative_category.code"),
            _approved_cs_field(cs, "wa_priority_category.code"),
            _approved_cs_field(cs, "commonwealth_conservation_category.code"),
            _approved_cs_field(cs, "other_conservation_assessment.code"),
            _approved_cs_field(cs, "conservation_criteria"),
            _safe(obj.get_processing_status_display()),
            pub,
        ]
        if include_group_type:
            row.insert(1, _safe(obj.group_type.name if obj.group_type else ""))
        rows.append(row)
    header = list(SPECIES_HEADER)
    if include_group_type:
        header.insert(1, "Group Type")
    return header, rows


# ── Communities ──────────────────────────────────────────────────────────────

COMMUNITY_HEADER = [
    "Number",
    "Community ID",
    "Community Name",
    "Region(s)",
    "District(s)",
    "WA Legislative List",
    "WA Legislative Category",
    "WA Priority Category",
    "Commonwealth Conservation Category",
    "Other Conservation Assessment",
    "Conservation Criteria",
    "Processing Status",
    "Publishing Status",
]


def get_community_export(filters, limit):
    from boranga.components.species_and_communities.models import Community

    qs = Community.objects.select_related(
        "taxonomy",
        "group_type",
        "community_publishing_status",
    ).prefetch_related(
        "regions",
        "districts",
        "conservation_status",
    )
    ps = filters.get("filter_status") or filters.get("processing_status")
    if ps and ps not in ("all", ""):
        qs = qs.filter(processing_status=ps)
    if filters.get("filter_community_name"):
        qs = qs.filter(taxonomy__community_name__icontains=filters["filter_community_name"])
    region_ids = _id_list(filters.get("filter_region"))
    if region_ids:
        qs = qs.filter(regions__id__in=region_ids).distinct()
    district_ids = _id_list(filters.get("filter_district"))
    if district_ids:
        qs = qs.filter(districts__id__in=district_ids).distinct()
    val = filters.get("filter_wa_legislative_list")
    if val and val not in ("all", ""):
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_list_id=val,
        ).distinct()
    val = filters.get("filter_wa_legislative_category")
    if val and val not in ("all", ""):
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_category_id=val,
        ).distinct()
    ids = _id_list(filters.get("filter_wa_priority_category"))
    if ids:
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_priority_category_id__in=ids,
        ).distinct()
    pub = filters.get("filter_publication_status")
    if pub and pub not in ("all", ""):
        qs = qs.filter(community_publishing_status__community_public=(pub.lower() == "true"))
    if filters.get("filter_commonwealth_relevance") == "true":
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__commonwealth_conservation_category__isnull=False,
        ).distinct()
    if filters.get("filter_international_relevance") == "true":
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__other_conservation_assessment__isnull=False,
        ).distinct()
    cc = filters.get("filter_conservation_criteria")
    if cc:
        qs = qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__conservation_criteria__icontains=cc,
        ).distinct()
    if filters.get("filter_community_common_id"):
        qs = qs.filter(taxonomy__community_common_id__icontains=filters["filter_community_common_id"])
    return list(qs[:limit])


def get_community_export_fields(data, include_group_type=False):
    rows = []
    for obj in data:
        try:
            t = obj.taxonomy
        except Exception:
            t = None
        regions = ", ".join(obj.regions.all().values_list("name", flat=True))
        districts = ", ".join(obj.districts.all().values_list("name", flat=True))
        cs = _approved_cs(obj)
        pub = ""
        try:
            pub = "Public" if obj.community_publishing_status.community_public else "Private"
        except Exception:
            pass
        rows.append(
            [
                _safe(obj.community_number),
                _safe(getattr(t, "community_common_id", "")),
                _safe(getattr(t, "community_name", "")),
                regions,
                districts,
                _approved_cs_field(cs, "wa_legislative_list.code"),
                _approved_cs_field(cs, "wa_legislative_category.code"),
                _approved_cs_field(cs, "wa_priority_category.code"),
                _approved_cs_field(cs, "commonwealth_conservation_category.code"),
                _approved_cs_field(cs, "other_conservation_assessment.code"),
                _approved_cs_field(cs, "conservation_criteria"),
                _safe(obj.get_processing_status_display()),
                pub,
            ]
        )
    return list(COMMUNITY_HEADER), rows


# ── Species & Communities (combined "all" view) ───────────────────────────────

SPECIES_AND_COMMUNITIES_HEADER = [
    "Number",
    "Group Type",
    "Scientific Name",
    "Common Name",
    "Community ID",
    "Community Name",
    "Family",
    "Genus",
    "Informal Group(s)",
    "Fauna Group",
    "Fauna Subgroup",
    "Region(s)",
    "District(s)",
    "WA Legislative List",
    "WA Legislative Category",
    "WA Priority Category",
    "Commonwealth Conservation Category",
    "Other Conservation Assessment",
    "Conservation Criteria",
    "Processing Status",
    "Publishing Status",
]


def get_species_and_communities_export(filters, limit):
    from boranga.components.species_and_communities.models import Community, Species

    species_limit = limit // 2
    community_limit = limit - species_limit
    species_qs = Species.objects.select_related(
        "taxonomy",
        "group_type",
        "species_publishing_status",
        "fauna_group",
        "fauna_sub_group",
    ).prefetch_related(
        "taxonomy__vernaculars",
        "taxonomy__informal_groups__classification_system_fk",
        "regions",
        "districts",
        "conservation_status",
    )
    community_qs = Community.objects.select_related(
        "taxonomy",
        "group_type",
        "community_publishing_status",
    ).prefetch_related(
        "regions",
        "districts",
        "conservation_status",
    )
    ps = filters.get("filter_status") or filters.get("processing_status")
    if ps and ps not in ("all", ""):
        species_qs = species_qs.filter(processing_status=ps)
        community_qs = community_qs.filter(processing_status=ps)
    if filters.get("filter_scientific_name"):
        species_qs = species_qs.filter(taxonomy__scientific_name__icontains=filters["filter_scientific_name"])
    if filters.get("filter_community_name"):
        community_qs = community_qs.filter(taxonomy__community_name__icontains=filters["filter_community_name"])
    region_ids = _id_list(filters.get("filter_region"))
    if region_ids:
        species_qs = species_qs.filter(regions__id__in=region_ids).distinct()
        community_qs = community_qs.filter(regions__id__in=region_ids).distinct()
    district_ids = _id_list(filters.get("filter_district"))
    if district_ids:
        species_qs = species_qs.filter(districts__id__in=district_ids).distinct()
        community_qs = community_qs.filter(districts__id__in=district_ids).distinct()
    val = filters.get("filter_wa_legislative_list")
    if val and val not in ("all", ""):
        species_qs = species_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_list_id=val,
        ).distinct()
        community_qs = community_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_list_id=val,
        ).distinct()
    val = filters.get("filter_wa_legislative_category")
    if val and val not in ("all", ""):
        species_qs = species_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_category_id=val,
        ).distinct()
        community_qs = community_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_legislative_category_id=val,
        ).distinct()
    ids = _id_list(filters.get("filter_wa_priority_category"))
    if ids:
        species_qs = species_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_priority_category_id__in=ids,
        ).distinct()
        community_qs = community_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__wa_priority_category_id__in=ids,
        ).distinct()
    ns = filters.get("filter_name_status")
    if ns and ns not in ("all", ""):
        species_qs = species_qs.filter(taxonomy__is_current=(ns.lower() == "true"))
    pub = filters.get("filter_publication_status")
    if pub and pub not in ("all", ""):
        species_qs = species_qs.filter(species_publishing_status__species_public=(pub.lower() == "true"))
        community_qs = community_qs.filter(community_publishing_status__community_public=(pub.lower() == "true"))
    if filters.get("filter_commonwealth_relevance") == "true":
        species_qs = species_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__commonwealth_conservation_category__isnull=False,
        ).distinct()
        community_qs = community_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__commonwealth_conservation_category__isnull=False,
        ).distinct()
    if filters.get("filter_international_relevance") == "true":
        species_qs = species_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__other_conservation_assessment__isnull=False,
        ).distinct()
        community_qs = community_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__other_conservation_assessment__isnull=False,
        ).distinct()
    cc = filters.get("filter_conservation_criteria")
    if cc:
        species_qs = species_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__conservation_criteria__icontains=cc,
        ).distinct()
        community_qs = community_qs.filter(
            conservation_status__processing_status="approved",
            conservation_status__conservation_criteria__icontains=cc,
        ).distinct()
    fg = filters.get("filter_fauna_group")
    if fg and fg not in ("all", ""):
        species_qs = species_qs.filter(fauna_group_id=fg)
    fsg = filters.get("filter_fauna_sub_group")
    if fsg and fsg not in ("all", ""):
        species_qs = species_qs.filter(fauna_sub_group_id=fsg)
    if filters.get("filter_common_name"):
        species_qs = species_qs.filter(
            taxonomy__vernaculars__vernacular_name__icontains=filters["filter_common_name"]
        ).distinct()
    if filters.get("filter_family"):
        species_qs = species_qs.filter(taxonomy__family_name__icontains=filters["filter_family"])
    if filters.get("filter_genus"):
        species_qs = species_qs.filter(taxonomy__genera_name__icontains=filters["filter_genus"])
    ig = filters.get("filter_informal_group")
    if ig and ig not in ("all", ""):
        species_qs = species_qs.filter(taxonomy__informal_groups__classification_system_fk_id=ig).distinct()
    if filters.get("filter_community_common_id"):
        community_qs = community_qs.filter(
            taxonomy__community_common_id__icontains=filters["filter_community_common_id"]
        )
    return list(species_qs[:species_limit]) + list(community_qs[:community_limit])


def get_species_and_communities_export_fields(data, include_group_type=False):
    from boranga.components.species_and_communities.models import Community

    rows = []
    for obj in data:
        is_community = isinstance(obj, Community)
        try:
            t = obj.taxonomy
        except Exception:
            t = None
        cs = _approved_cs(obj)
        if is_community:
            group_type_name = "Community"
            scientific_name = ""
            common_name = ""
            community_id = _safe(getattr(t, "community_common_id", ""))
            community_name = _safe(getattr(t, "community_name", ""))
            family = ""
            genus = ""
            informal = ""
            fauna_group = ""
            fauna_subgroup = ""
            pub = ""
            try:
                pub = "Public" if obj.community_publishing_status.community_public else "Private"
            except Exception:
                pass
            number = _safe(obj.community_number)
        else:
            group_type_name = _safe(obj.group_type.name if obj.group_type else "")
            common_name = ", ".join(t.vernaculars.all().values_list("vernacular_name", flat=True)) if t else ""
            informal = ", ".join(
                ig.classification_system_fk.class_desc
                for ig in (t.informal_groups.all() if t else [])
                if ig.classification_system_fk
            )
            scientific_name = _safe(getattr(t, "scientific_name", ""))
            community_id = ""
            community_name = ""
            family = _safe(getattr(t, "family_name", ""))
            genus = _safe(getattr(t, "genera_name", ""))
            fauna_group = _safe(obj.fauna_group.name if obj.fauna_group else "")
            fauna_subgroup = _safe(obj.fauna_sub_group.name if obj.fauna_sub_group else "")
            pub = ""
            try:
                pub = "Public" if obj.species_publishing_status.species_public else "Private"
            except Exception:
                pass
            number = _safe(obj.species_number)
        regions = ", ".join(obj.regions.all().values_list("name", flat=True))
        districts = ", ".join(obj.districts.all().values_list("name", flat=True))
        rows.append(
            [
                number,
                group_type_name,
                scientific_name,
                common_name,
                community_id,
                community_name,
                family,
                genus,
                informal,
                fauna_group,
                fauna_subgroup,
                regions,
                districts,
                _approved_cs_field(cs, "wa_legislative_list.code"),
                _approved_cs_field(cs, "wa_legislative_category.code"),
                _approved_cs_field(cs, "wa_priority_category.code"),
                _approved_cs_field(cs, "commonwealth_conservation_category.code"),
                _approved_cs_field(cs, "other_conservation_assessment.code"),
                _approved_cs_field(cs, "conservation_criteria"),
                _safe(obj.get_processing_status_display()),
                pub,
            ]
        )
    return list(SPECIES_AND_COMMUNITIES_HEADER), rows


# ── Conservation Status (Species) ────────────────────────────────────────────

CS_SPECIES_HEADER = [
    "Number",
    "Species Number",
    "Scientific Name",
    "Common Name",
    "Family",
    "Genus",
    "Informal Group(s)",
    "Fauna Group",
    "Fauna Subgroup",
    "Change Type",
    "WA Priority List",
    "WA Priority Category",
    "WA Legislative List",
    "WA Legislative Category",
    "Commonwealth Conservation Category",
    "Other Conservation Assessment",
    "Conservation Criteria",
    "Submitter Name",
    "Submitter Category",
    "Submitter Organisation",
    "Assessor Name",
    "Processing Status",
    "Effective From",
    "Effective To",
    "Review Due Date",
]


def get_conservation_status_species_export(filters, limit):
    from boranga.components.conservation_status.models import ConservationStatus

    qs = ConservationStatus.objects.select_related(
        "species__taxonomy",
        "species__group_type",
        "species__fauna_group",
        "species__fauna_sub_group",
        "wa_legislative_list",
        "wa_legislative_category",
        "wa_priority_list",
        "wa_priority_category",
        "commonwealth_conservation_category",
        "other_conservation_assessment",
        "change_code",
        "submitter_information__submitter_category",
    ).prefetch_related(
        "species__taxonomy__vernaculars",
        "species__taxonomy__informal_groups__classification_system_fk",
    )
    # Ensure only species-type CS records are included (community CS have species=None)
    qs = qs.filter(species__isnull=False)
    ps = filters.get("filter_status") or filters.get("processing_status")
    if ps and ps not in ("all", ""):
        qs = qs.filter(processing_status=ps)
    if filters.get("group_type") and filters["group_type"] != "all":
        qs = qs.filter(species__group_type__name__iexact=filters["group_type"])
    if filters.get("filter_scientific_name"):
        qs = qs.filter(species__taxonomy__scientific_name__icontains=filters["filter_scientific_name"])
    val = filters.get("filter_wa_legislative_list")
    if val and val not in ("all", ""):
        qs = qs.filter(wa_legislative_list=val)
    val = filters.get("filter_wa_legislative_category")
    if val and val not in ("all", ""):
        qs = qs.filter(wa_legislative_category=val)
    ids = _id_list(filters.get("filter_wa_priority_category"))
    if ids:
        qs = qs.filter(wa_priority_category__in=ids)
    d = _parse_date(filters.get("filter_from_effective_from_date"))
    if d:
        qs = qs.filter(effective_from__gte=d)
    d = _parse_date(filters.get("filter_to_effective_from_date"))
    if d:
        qs = qs.filter(effective_from__lte=d)
    d = _parse_date(filters.get("filter_from_effective_to_date"))
    if d:
        qs = qs.filter(effective_to__gte=d)
    d = _parse_date(filters.get("filter_to_effective_to_date"))
    if d:
        qs = qs.filter(effective_to__lte=d)
    d = _parse_date(filters.get("filter_from_review_due_date"))
    if d:
        qs = qs.filter(review_due_date__gte=d)
    d = _parse_date(filters.get("filter_to_review_due_date"))
    if d:
        qs = qs.filter(review_due_date__lte=d)
    if filters.get("filter_commonwealth_relevance") == "true":
        qs = qs.exclude(commonwealth_conservation_category__isnull=True)
    if filters.get("filter_international_relevance") == "true":
        qs = qs.exclude(other_conservation_assessment__isnull=True)
    cc = filters.get("filter_conservation_criteria")
    if cc:
        qs = qs.filter(conservation_criteria__icontains=cc)
    fg = filters.get("filter_fauna_group")
    if fg and fg not in ("all", ""):
        qs = qs.filter(species__fauna_group_id=fg)
    ccode = filters.get("filter_change_code")
    if ccode and ccode not in ("all", ""):
        qs = qs.filter(change_code_id=ccode)
    sc = filters.get("filter_submitter_category")
    if sc and sc not in ("all", ""):
        qs = qs.filter(submitter_information__submitter_category_id=sc)
    lock = filters.get("filter_locked")
    if lock and lock not in ("all", ""):
        qs = qs.filter(locked=(lock.lower() == "true"))
    if filters.get("filter_common_name"):
        qs = qs.filter(
            species__taxonomy__vernaculars__vernacular_name__icontains=filters["filter_common_name"]
        ).distinct()
    if filters.get("filter_family"):
        qs = qs.filter(species__taxonomy__family_name__icontains=filters["filter_family"])
    if filters.get("filter_genus"):
        qs = qs.filter(species__taxonomy__genera_name__icontains=filters["filter_genus"])
    uid = _user_id_from_email(filters.get("filter_assessor"))
    if uid:
        qs = qs.filter(assigned_officer=uid)
    uid = _user_id_from_email(filters.get("filter_submitter"))
    if uid:
        qs = qs.filter(submitter=uid)
    return list(qs[:limit])


def get_conservation_status_species_export_fields(data, include_group_type=False):
    rows = []
    user_cache = {}
    for obj in data:
        sp = obj.species
        t = sp.taxonomy if sp else None
        common_names = ", ".join(t.vernaculars.all().values_list("vernacular_name", flat=True)) if t else ""
        informal = ", ".join(
            ig.classification_system_fk.class_desc
            for ig in (t.informal_groups.all() if t else [])
            if ig.classification_system_fk
        )
        si = getattr(obj, "submitter_information", None)
        row = [
            _safe(obj.conservation_status_number),
            _safe(getattr(sp, "species_number", "")),
            _safe(getattr(t, "scientific_name", "")),
            common_names,
            _safe(getattr(t, "family_name", "")),
            _safe(getattr(t, "genera_name", "")),
            informal,
            _safe(sp.fauna_group.name if sp and sp.fauna_group else ""),
            _safe(sp.fauna_sub_group.name if sp and sp.fauna_sub_group else ""),
            _safe(getattr(obj.change_code, "code", "") if obj.change_code else ""),
            _safe(getattr(obj.wa_priority_list, "code", "") if obj.wa_priority_list else ""),
            _safe(getattr(obj.wa_priority_category, "code", "") if obj.wa_priority_category else ""),
            _safe(getattr(obj.wa_legislative_list, "code", "") if obj.wa_legislative_list else ""),
            _safe(getattr(obj.wa_legislative_category, "code", "") if obj.wa_legislative_category else ""),
            _safe(
                getattr(obj.commonwealth_conservation_category, "code", "")
                if obj.commonwealth_conservation_category
                else ""
            ),
            _safe(getattr(obj.other_conservation_assessment, "code", "") if obj.other_conservation_assessment else ""),
            _safe(obj.conservation_criteria),
            _user_name(obj.submitter, user_cache),
            _safe(getattr(getattr(si, "submitter_category", None), "name", "")) if si else "",
            _safe(getattr(si, "organisation", "")) if si else "",
            _user_name(obj.assigned_officer, user_cache),
            _safe(obj.get_processing_status_display()),
            _fmt_date(obj.effective_from),
            _fmt_date(obj.effective_to),
            _fmt_date(obj.review_due_date),
        ]
        if include_group_type:
            row.insert(1, _safe(sp.group_type.name if sp and sp.group_type else ""))
        rows.append(row)
    header = list(CS_SPECIES_HEADER)
    if include_group_type:
        header.insert(1, "Group Type")
    return header, rows


# ── Conservation Status (Community) ──────────────────────────────────────────

CS_COMMUNITY_HEADER = [
    "Number",
    "Community Number",
    "Community ID",
    "Community Name",
    "Region(s)",
    "District(s)",
    "Change Type",
    "WA Priority List",
    "WA Priority Category",
    "WA Legislative List",
    "WA Legislative Category",
    "Commonwealth Conservation Category",
    "Other Conservation Assessment",
    "Conservation Criteria",
    "Submitter Name",
    "Submitter Category",
    "Submitter Organisation",
    "Assessor Name",
    "Processing Status",
    "Effective From",
    "Effective To",
    "Review Due Date",
]


def get_conservation_status_community_export(filters, limit):
    from boranga.components.conservation_status.models import ConservationStatus

    qs = ConservationStatus.objects.select_related(
        "community__taxonomy",
        "community__group_type",
        "wa_legislative_list",
        "wa_legislative_category",
        "wa_priority_list",
        "wa_priority_category",
        "commonwealth_conservation_category",
        "other_conservation_assessment",
        "change_code",
        "submitter_information__submitter_category",
    ).prefetch_related(
        "community__regions",
        "community__districts",
    )
    # Ensure only community-type CS records are included (species CS have community=None)
    qs = qs.filter(community__isnull=False)
    ps = filters.get("filter_status") or filters.get("processing_status")
    if ps and ps not in ("all", ""):
        qs = qs.filter(processing_status=ps)
    if filters.get("filter_community_name"):
        qs = qs.filter(community__taxonomy__community_name__icontains=filters["filter_community_name"])
    val = filters.get("filter_wa_legislative_list")
    if val and val not in ("all", ""):
        qs = qs.filter(wa_legislative_list=val)
    val = filters.get("filter_wa_legislative_category")
    if val and val not in ("all", ""):
        qs = qs.filter(wa_legislative_category=val)
    ids = _id_list(filters.get("filter_wa_priority_category"))
    if ids:
        qs = qs.filter(wa_priority_category__in=ids)
    d = _parse_date(filters.get("filter_from_effective_from_date"))
    if d:
        qs = qs.filter(effective_from__gte=d)
    d = _parse_date(filters.get("filter_to_effective_from_date"))
    if d:
        qs = qs.filter(effective_from__lte=d)
    d = _parse_date(filters.get("filter_from_effective_to_date"))
    if d:
        qs = qs.filter(effective_to__gte=d)
    d = _parse_date(filters.get("filter_to_effective_to_date"))
    if d:
        qs = qs.filter(effective_to__lte=d)
    d = _parse_date(filters.get("filter_from_review_due_date"))
    if d:
        qs = qs.filter(review_due_date__gte=d)
    d = _parse_date(filters.get("filter_to_review_due_date"))
    if d:
        qs = qs.filter(review_due_date__lte=d)
    if filters.get("filter_commonwealth_relevance") == "true":
        qs = qs.exclude(commonwealth_conservation_category__isnull=True)
    if filters.get("filter_international_relevance") == "true":
        qs = qs.exclude(other_conservation_assessment__isnull=True)
    cc = filters.get("filter_conservation_criteria")
    if cc:
        qs = qs.filter(conservation_criteria__icontains=cc)
    ccode = filters.get("filter_change_code")
    if ccode and ccode not in ("all", ""):
        qs = qs.filter(change_code_id=ccode)
    sc = filters.get("filter_submitter_category")
    if sc and sc not in ("all", ""):
        qs = qs.filter(submitter_information__submitter_category_id=sc)
    lock = filters.get("filter_locked")
    if lock and lock not in ("all", ""):
        qs = qs.filter(locked=(lock.lower() == "true"))
    if filters.get("filter_community_common_id"):
        qs = qs.filter(community__taxonomy__community_common_id__icontains=filters["filter_community_common_id"])
    uid = _user_id_from_email(filters.get("filter_assessor"))
    if uid:
        qs = qs.filter(assigned_officer=uid)
    uid = _user_id_from_email(filters.get("filter_submitter"))
    if uid:
        qs = qs.filter(submitter=uid)
    return list(qs[:limit])


def get_conservation_status_community_export_fields(data, include_group_type=False):
    rows = []
    user_cache = {}
    for obj in data:
        comm = obj.community
        t = comm.taxonomy if comm else None
        regions = ", ".join(comm.regions.all().values_list("name", flat=True)) if comm else ""
        districts = ", ".join(comm.districts.all().values_list("name", flat=True)) if comm else ""
        si = getattr(obj, "submitter_information", None)
        rows.append(
            [
                _safe(obj.conservation_status_number),
                _safe(getattr(comm, "community_number", "")),
                _safe(getattr(t, "community_common_id", "")),
                _safe(getattr(t, "community_name", "")),
                regions,
                districts,
                _safe(getattr(obj.change_code, "code", "") if obj.change_code else ""),
                _safe(getattr(obj.wa_priority_list, "code", "") if obj.wa_priority_list else ""),
                _safe(getattr(obj.wa_priority_category, "code", "") if obj.wa_priority_category else ""),
                _safe(getattr(obj.wa_legislative_list, "code", "") if obj.wa_legislative_list else ""),
                _safe(getattr(obj.wa_legislative_category, "code", "") if obj.wa_legislative_category else ""),
                _safe(
                    getattr(obj.commonwealth_conservation_category, "code", "")
                    if obj.commonwealth_conservation_category
                    else ""
                ),
                _safe(
                    getattr(obj.other_conservation_assessment, "code", "") if obj.other_conservation_assessment else ""
                ),
                _safe(obj.conservation_criteria),
                _user_name(obj.submitter, user_cache),
                _safe(getattr(getattr(si, "submitter_category", None), "name", "")) if si else "",
                _safe(getattr(si, "organisation", "")) if si else "",
                _user_name(obj.assigned_officer, user_cache),
                _safe(obj.get_processing_status_display()),
                _fmt_date(obj.effective_from),
                _fmt_date(obj.effective_to),
                _fmt_date(obj.review_due_date),
            ]
        )
    return list(CS_COMMUNITY_HEADER), rows


# ── Occurrence ───────────────────────────────────────────────────────────────

OCC_HEADER = [
    "Number",
    "Occurrence Name",
    "Scientific Name",
    "Common Name",
    "Community Name",
    "Community ID",
    "Wild Status",
    "Number of Reports",
    "Migrated From ID",
    "Region",
    "District",
    "Review Due Date",
    "Last Modified By",
    "Last Modified Date",
    "Activated Date",
    "Created Date",
    "Family",
    "Informal Group(s)",
    "Fauna Group",
    "Fauna Subgroup",
    "Processing Status",
]


def get_occurrence_export(filters, limit):
    from boranga.components.occurrence.models import Occurrence

    qs = (
        Occurrence.objects.select_related(
            "species__taxonomy",
            "species__fauna_group",
            "species__fauna_sub_group",
            "community__taxonomy",
            "group_type",
            "wild_status",
            "location__region",
            "location__district",
        )
        .prefetch_related(
            "occurrence_reports",
            "species__taxonomy__informal_groups__classification_system_fk",
            "species__taxonomy__vernaculars",
        )
        .annotate(num_reports=Count("occurrence_reports"))
    )
    if filters.get("group_type") and filters["group_type"] != "all":
        qs = qs.filter(group_type__name__iexact=filters["group_type"])
    ps = filters.get("filter_status") or filters.get("processing_status")
    if ps and ps not in ("all", ""):
        qs = qs.filter(processing_status=ps)
    if filters.get("filter_scientific_name"):
        qs = qs.filter(species__taxonomy__scientific_name__icontains=filters["filter_scientific_name"])
    if filters.get("filter_community_name"):
        qs = qs.filter(community__taxonomy__community_name__icontains=filters["filter_community_name"])
    if filters.get("filter_occurrence_name"):
        qs = qs.filter(occurrence_name__icontains=filters["filter_occurrence_name"])
    region_ids = _id_list(filters.get("filter_region"))
    if region_ids:
        qs = qs.filter(location__region__id__in=region_ids)
    district_ids = _id_list(filters.get("filter_district"))
    if district_ids:
        qs = qs.filter(location__district__id__in=district_ids)
    d = _parse_date(filters.get("filter_from_due_date"))
    if d:
        qs = qs.filter(review_due_date__gte=d)
    d = _parse_date(filters.get("filter_to_due_date"))
    if d:
        qs = qs.filter(review_due_date__lte=d)
    d = _parse_date(filters.get("filter_created_from_date"))
    if d:
        qs = qs.filter(datetime_created__date__gte=d)
    d = _parse_date(filters.get("filter_created_to_date"))
    if d:
        qs = qs.filter(datetime_created__date__lte=d)
    d = _parse_date(filters.get("filter_activated_from_date"))
    if d:
        qs = qs.filter(lodgement_date__date__gte=d)
    d = _parse_date(filters.get("filter_activated_to_date"))
    if d:
        qs = qs.filter(lodgement_date__date__lte=d)
    d = _parse_date(filters.get("filter_last_modified_from_date"))
    if d:
        qs = qs.filter(datetime_updated__date__gte=d)
    d = _parse_date(filters.get("filter_last_modified_to_date"))
    if d:
        qs = qs.filter(datetime_updated__date__lte=d)
    if filters.get("filter_common_name"):
        qs = qs.filter(
            species__taxonomy__vernaculars__vernacular_name__icontains=filters["filter_common_name"]
        ).distinct()
    uid = _user_id_from_email(filters.get("filter_last_modified_by"))
    if uid:
        qs = qs.filter(last_modified_by=uid)
    return list(qs[:limit])


def get_occurrence_export_fields(data, include_group_type=False):
    rows = []
    user_cache = {}
    for obj in data:
        sp = obj.species
        t = sp.taxonomy if sp else None
        comm = obj.community
        ct = comm.taxonomy if comm else None
        loc = getattr(obj, "location", None)
        informal = ", ".join(
            ig.classification_system_fk.class_desc
            for ig in (t.informal_groups.all() if t else [])
            if ig.classification_system_fk
        )
        common_name = ""
        if t:
            v = t.vernaculars.all().first()
            if v:
                common_name = v.vernacular_name
        row = [
            _safe(obj.occurrence_number),
            _safe(obj.occurrence_name),
            _safe(getattr(t, "scientific_name", "")),
            common_name,
            _safe(getattr(ct, "community_name", "")),
            _safe(getattr(ct, "community_common_id", "")),
            _safe(getattr(obj.wild_status, "name", "") if obj.wild_status else ""),
            obj.num_reports,
            _safe(obj.migrated_from_id),
            _safe(getattr(getattr(loc, "region", None), "name", "")),
            _safe(getattr(getattr(loc, "district", None), "name", "")),
            _fmt_date(obj.review_due_date),
            _user_name(obj.last_modified_by, user_cache),
            _fmt_date(obj.datetime_updated, "%d/%m/%Y"),
            _fmt_date(obj.lodgement_date, "%d/%m/%Y"),
            _fmt_date(obj.datetime_created, "%d/%m/%Y"),
            _safe(getattr(t, "family_name", "")),
            informal,
            _safe(sp.fauna_group.name if sp and sp.fauna_group else ""),
            _safe(sp.fauna_sub_group.name if sp and sp.fauna_sub_group else ""),
            _safe(obj.get_processing_status_display()),
        ]
        if include_group_type:
            row.insert(1, _safe(obj.group_type.name if obj.group_type else ""))
        rows.append(row)
    header = list(OCC_HEADER)
    if include_group_type:
        header.insert(1, "Group Type")
    return header, rows


# ── Occurrence Report ────────────────────────────────────────────────────────

OCR_HEADER = [
    "Number",
    "Occurrence",
    "Occurrence Name",
    "Scientific Name",
    "Common Name",
    "Community Name",
    "Community ID",
    "Observation Date",
    "Main Observer",
    "Migrated From ID",
    "Region",
    "District",
    "Submitted On",
    "Submitter",
    "Approved Date",
    "Assessor",
    "Last Modified By",
    "Last Modified Date",
    "Family",
    "Fauna Group",
    "Fauna Subgroup",
    "Processing Status",
]


def get_occurrence_report_export(filters, limit):
    from boranga.components.occurrence.models import OccurrenceReport

    qs = OccurrenceReport.objects.select_related(
        "species__taxonomy",
        "species__fauna_group",
        "species__fauna_sub_group",
        "community__taxonomy",
        "group_type",
        "occurrence",
        "location__region",
        "location__district",
    ).prefetch_related(
        "observer_detail",
        "species__taxonomy__vernaculars",
    )
    if filters.get("group_type") and filters["group_type"] != "all":
        qs = qs.filter(group_type__name__iexact=filters["group_type"])
    ps = filters.get("filter_status") or filters.get("processing_status")
    if ps and ps not in ("all", ""):
        qs = qs.filter(processing_status=ps)
    if filters.get("filter_scientific_name"):
        qs = qs.filter(species__taxonomy__scientific_name__icontains=filters["filter_scientific_name"])
    if filters.get("filter_community_name"):
        qs = qs.filter(community__taxonomy__community_name__icontains=filters["filter_community_name"])
    if filters.get("filter_occurrence_name"):
        qs = qs.filter(occurrence__occurrence_name__icontains=filters["filter_occurrence_name"])
    if filters.get("filter_occurrence") and filters["filter_occurrence"] not in ("all", ""):
        qs = qs.filter(occurrence__occurrence_number__icontains=filters["filter_occurrence"])
    region_ids = _id_list(filters.get("filter_region"))
    if region_ids:
        qs = qs.filter(location__region__id__in=region_ids)
    district_ids = _id_list(filters.get("filter_district"))
    if district_ids:
        qs = qs.filter(location__district__id__in=district_ids)
    d = _parse_date(filters.get("filter_observation_from_date"))
    if d:
        qs = qs.filter(observation_date__gte=d)
    d = _parse_date(filters.get("filter_observation_to_date"))
    if d:
        qs = qs.filter(observation_date__lte=d)
    d = _parse_date(filters.get("filter_submitted_from_date"))
    if d:
        qs = qs.filter(lodgement_date__gte=d)
    d = _parse_date(filters.get("filter_submitted_to_date"))
    if d:
        qs = qs.filter(lodgement_date__date__lte=d)
    d = _parse_date(filters.get("filter_approved_from_date"))
    if d:
        qs = qs.filter(datetime_approved__date__gte=d)
    d = _parse_date(filters.get("filter_approved_to_date"))
    if d:
        qs = qs.filter(datetime_approved__date__lte=d)
    d = _parse_date(filters.get("filter_last_modified_from_date"))
    if d:
        qs = qs.filter(datetime_updated__date__gte=d)
    d = _parse_date(filters.get("filter_last_modified_to_date"))
    if d:
        qs = qs.filter(datetime_updated__date__lte=d)
    if filters.get("filter_common_name"):
        qs = qs.filter(
            species__taxonomy__vernaculars__vernacular_name__icontains=filters["filter_common_name"]
        ).distinct()
    uid = _user_id_from_email(filters.get("filter_assessor"))
    if uid:
        qs = qs.filter(assigned_officer=uid)
    uid = _user_id_from_email(filters.get("filter_submitter"))
    if uid:
        qs = qs.filter(submitter=uid)
    uid = _user_id_from_email(filters.get("filter_last_modified_by"))
    if uid:
        qs = qs.filter(last_modified_by=uid)
    return list(qs[:limit])


def get_occurrence_report_export_fields(data, include_group_type=False):
    rows = []
    user_cache = {}
    for obj in data:
        sp = obj.species
        t = sp.taxonomy if sp else None
        comm = obj.community
        ct = comm.taxonomy if comm else None
        occ = obj.occurrence
        loc = getattr(obj, "location", None)
        main_obs = ""
        try:
            obs = obj.observer_detail.filter(main_observer=True).first()
            if obs:
                main_obs = obs.observer_name
        except Exception:
            pass
        common_name = ""
        if t:
            v = t.vernaculars.all().first()
            if v:
                common_name = v.vernacular_name
        row = [
            _safe(obj.occurrence_report_number),
            _safe(getattr(occ, "occurrence_number", "")),
            _safe(getattr(occ, "occurrence_name", "")),
            _safe(getattr(t, "scientific_name", "")),
            common_name,
            _safe(getattr(ct, "community_name", "")),
            _safe(getattr(ct, "community_common_id", "")),
            _fmt_date(obj.observation_date, "%d/%m/%Y"),
            main_obs,
            _safe(obj.migrated_from_id),
            _safe(getattr(getattr(loc, "region", None), "name", "")),
            _safe(getattr(getattr(loc, "district", None), "name", "")),
            _fmt_date(obj.lodgement_date, "%Y-%m-%d %H:%M:%S"),
            _user_name(obj.submitter, user_cache),
            _fmt_date(obj.datetime_approved, "%d/%m/%Y"),
            _user_name(obj.assigned_officer, user_cache),
            _user_name(obj.last_modified_by, user_cache),
            _fmt_date(obj.datetime_updated, "%d/%m/%Y"),
            _safe(getattr(t, "family_name", "")),
            _safe(sp.fauna_group.name if sp and sp.fauna_group else ""),
            _safe(sp.fauna_sub_group.name if sp and sp.fauna_sub_group else ""),
            _safe(obj.get_processing_status_display()),
        ]
        if include_group_type:
            row.insert(1, _safe(obj.group_type.name if obj.group_type else ""))
        rows.append(row)
    header = list(OCR_HEADER)
    if include_group_type:
        header.insert(1, "Group Type")
    return header, rows


# ── Dispatch ─────────────────────────────────────────────────────────────────

EXPORT_MODELS = {
    "species": {
        "label": "Species",
        "get_data": get_species_export,
        "get_fields": get_species_export_fields,
    },
    "species_and_communities": {
        "label": "Species & Communities",
        "get_data": get_species_and_communities_export,
        "get_fields": get_species_and_communities_export_fields,
    },
    "community": {
        "label": "Community",
        "get_data": get_community_export,
        "get_fields": get_community_export_fields,
    },
    "conservation_status_species": {
        "label": "Conservation Status (Species)",
        "get_data": get_conservation_status_species_export,
        "get_fields": get_conservation_status_species_export_fields,
    },
    "conservation_status_community": {
        "label": "Conservation Status (Community)",
        "get_data": get_conservation_status_community_export,
        "get_fields": get_conservation_status_community_export_fields,
    },
    "occurrence": {
        "label": "Occurrence",
        "get_data": get_occurrence_export,
        "get_fields": get_occurrence_export_fields,
    },
    "occurrence_report": {
        "label": "Occurrence Report Form",
        "get_data": get_occurrence_report_export,
        "get_fields": get_occurrence_report_export_fields,
    },
}

# ── Report categories (user-facing dropdown) ────────────────────────────────
# Maps each category to which EXPORT_MODELS key to use depending on group_type.

REPORT_CATEGORIES = [
    {"key": "species", "label": "Species & Communities"},
    {"key": "conservation_status", "label": "Conservation Status"},
    {"key": "occurrence", "label": "Occurrence"},
    {"key": "occurrence_report", "label": "Occurrence Report Form"},
]

GROUP_TYPES = [
    {"key": "all", "label": "All"},
    {"key": "flora", "label": "Flora"},
    {"key": "fauna", "label": "Fauna"},
    {"key": "community", "label": "Communities"},
]


def resolve_export_key(category, group_type):
    """Return the EXPORT_MODELS key and filters dict for a category + group_type."""
    filters = {}
    if category == "species":
        if group_type == "community":
            return "community", filters
        if group_type not in ("all", ""):
            filters["group_type"] = group_type
            return "species", filters
        # "all" — return both species and communities combined
        return "species_and_communities", filters
    if category == "conservation_status":
        if group_type == "community":
            return "conservation_status_community", filters
        if group_type not in ("all", ""):
            filters["group_type"] = group_type
        return "conservation_status_species", filters
    if category in ("occurrence", "occurrence_report"):
        if group_type not in ("all", ""):
            filters["group_type"] = group_type
        return category, filters
    return None, filters


def export_model_data(model_key, filters, num_records):
    """Fetch export data for the given *model_key*."""
    if model_key not in EXPORT_MODELS:
        return None
    limit = min(num_records or MAX_NUM_ROWS_MODEL_EXPORT, MAX_NUM_ROWS_MODEL_EXPORT)
    return EXPORT_MODELS[model_key]["get_data"](filters, limit)


def format_export_data(model_key, data, fmt="csv", group_type_label="", include_group_type=False):
    """Format previously-fetched *data* as a file attachment tuple ``(name, bytes, mime)``."""
    if model_key not in EXPORT_MODELS:
        return None
    conf = EXPORT_MODELS[model_key]
    header, rows = conf["get_fields"](data, include_group_type=include_group_type)
    if fmt == "excel":
        return _excel_file(conf["label"], header, rows, group_type_label)
    return _csv_file(conf["label"], header, rows, group_type_label)
