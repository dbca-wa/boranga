from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any

import requests
from django.conf import settings
from django.db import transaction
from django.db.models import Exists, OuterRef
from django.utils import timezone

from boranga.components.data_migration.adapters.occurrence import (  # shared canonical schema
    schema,
)
from boranga.components.data_migration.adapters.occurrence.tec import (
    OccurrenceTecAdapter,
    tec_site_geometry_transform,
)
from boranga.components.data_migration.adapters.occurrence.tec_boundaries import (
    OccurrenceTecBoundariesAdapter,
)
from boranga.components.data_migration.adapters.occurrence.tpfl import (
    OccurrenceTpflAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.handlers.helpers import (
    apply_model_defaults,
    apply_value_to_instance,
    normalize_create_kwargs,
)
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.occurrence.models import (
    AssociatedSpeciesTaxonomy,
    OCCAssociatedSpecies,
    OCCContactDetail,
    OCCFireHistory,
    OCCHabitatComposition,
    OCCHabitatCondition,
    OCCIdentification,
    OCCLocation,
    OCCObservationDetail,
    OCCPlantCount,
    Occurrence,
    OccurrenceDocument,
    OccurrenceGeometry,
    OccurrenceReport,
    OccurrenceSite,
    OccurrenceUserAction,
    OCCVegetationStructure,
    OCRAssociatedSpecies,
    SpeciesRole,
)
from boranga.components.species_and_communities.models import District, Taxonomy
from boranga.components.users.models import SubmitterInformation

logger = logging.getLogger(__name__)

# WFS layer used to resolve occurrence district by spatial intersection.
_DISTRICT_WFS_LAYER = "kaartdijin-boodja-public:CPT_DBCA_DISTRICTS"
_DISTRICT_NAME_CANDIDATES = [
    "DDT_DISTRICT_NAME",
    "DISTRICT_NAME",
    "NAME",
    "DISTRICTNAM",
    "DIST_NAME",
    "district_name",
    "name",
]


def _pick_district_name(props, candidates):
    """Return the first non-empty string from *candidates* found in *props*."""
    for f in candidates:
        v = props.get(f)
        if v:
            return str(v)
    for k, v in props.items():
        if v and isinstance(v, str) and ("name" in k.lower() or "district" in k.lower()):
            return v
    return None


def _parse_geojson_features(fc, invert_xy, name_field, source_label):
    """Parse a GeoJSON FeatureCollection dict into ``(name, fallback_name, shapely_geom)`` tuples."""
    from shapely.affinity import affine_transform as shapely_affine_transform
    from shapely.geometry import shape as shapely_shape_from_geojson

    features = fc.get("features") or []
    if not features:
        logger.warning("GIS district: no features in %s", source_label)
        return []

    sample_props = features[0].get("properties") or {}
    logger.info(
        "GIS district: %d features from %s; property keys: %s",
        len(features),
        source_label,
        list(sample_props.keys()),
    )

    results = []
    for feat in features:
        props = feat.get("properties") or {}
        raw_geom = feat.get("geometry")
        if not raw_geom:
            continue
        try:
            shp = shapely_shape_from_geojson(raw_geom)
            if invert_xy:
                shp = shapely_affine_transform(shp, [0, 1, 1, 0, 0, 0])
            if not shp.is_valid:
                shp = shp.buffer(0)
        except Exception as exc:
            logger.warning("Could not parse district geometry from %s: %s", source_label, exc)
            continue
        name = _pick_district_name(props, [name_field] if name_field else _DISTRICT_NAME_CANDIDATES)
        # ADMIN_ZONE is a secondary field used as a fallback when the primary name doesn't match
        fallback_name = props.get("ADMIN_ZONE") or None
        results.append((name, fallback_name, shp))

    logger.info("GIS district: loaded %d valid geometries from %s", len(results), source_label)
    return results


def _load_district_shapes_from_file(file_path, invert_xy=True, name_field=None):
    """Load district shapes from a local GeoJSON file."""
    import json

    try:
        with open(file_path, encoding="utf-8") as fh:
            fc = json.load(fh)
    except Exception as exc:
        logger.error("GIS district: could not read local file %s: %s", file_path, exc)
        return []
    return _parse_geojson_features(fc, invert_xy, name_field, file_path)


def _load_district_shapes_from_wfs(url, invert_xy=True, name_field=None):
    """Download the CPT_DBCA_DISTRICTS WFS layer and return parsed shapes.

    Returns a list of ``(district_name_str, shapely_geom)`` tuples.  Only
    features with a valid geometry are included.  ``invert_xy=True`` (the
    default) swaps x/y because the layer is served with lat/lon axes
    inverted relative to the GeoJSON spec.
    """
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("GIS district WFS fetch failed (%s): %s", url, exc)
        return []

    try:
        fc = resp.json()
    except Exception as exc:
        logger.error("GIS district WFS response is not valid JSON: %s", exc)
        return []

    return _parse_geojson_features(fc, invert_xy, name_field, url)


def _build_district_geo_lookup(wfs_districts):
    """Match WFS district names to DB ``District`` PKs.

    Returns a list of ``(district_pk, region_pk, shapely_geom)`` tuples for
    districts that could be matched by (case-insensitive) name.  Unmatched
    WFS features are logged as warnings.
    """
    db_districts = {
        d.name.strip().lower(): d for d in District.objects.select_related("region").only("pk", "name", "region_id")
    }
    results = []
    for entry in wfs_districts:
        # Support both old (name, shp) and new (name, fallback_name, shp) tuples
        if len(entry) == 3:
            name, fallback_name, shp = entry
        else:
            name, shp = entry
            fallback_name = None

        if name is None and fallback_name is None:
            logger.warning("WFS district feature has no recognisable name field — skipped")
            continue

        db_dist = None
        for candidate in filter(None, [name, fallback_name]):
            key = candidate.strip().lower()
            db_dist = db_districts.get(key)
            if db_dist is None:
                # Try partial / substring match as a fallback
                for db_key, db_obj in db_districts.items():
                    if key in db_key or db_key in key:
                        db_dist = db_obj
                        break
            if db_dist is not None:
                break

        if db_dist is None:
            logger.warning(
                "WFS district '%s' (ADMIN_ZONE: '%s') could not be matched to a DB District — skipped",
                name,
                fallback_name,
            )
            continue
        results.append((db_dist.pk, db_dist.region_id, shp))
    logger.info("GIS district lookup: matched %d / %d WFS districts to DB records", len(results), len(wfs_districts))
    return results


def _find_primary_district(shp_geom, district_lookup):
    """Return ``(district_pk, region_pk)`` for the district with the largest
    intersection area.  Returns ``(None, None)`` when no district intersects.
    """
    best_pk = None
    best_region_pk = None
    best_area = 0.0
    for dist_pk, region_pk, dist_shp in district_lookup:
        try:
            if not shp_geom.intersects(dist_shp):
                continue
            area = shp_geom.intersection(dist_shp).area
            if area > best_area:
                best_area = area
                best_pk = dist_pk
                best_region_pk = region_pk
        except Exception:
            pass
    return best_pk, best_region_pk


SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceTpflAdapter(),
    Source.TEC.value: OccurrenceTecAdapter(),
    Source.TEC_BOUNDARIES.value: OccurrenceTecBoundariesAdapter(),
}


@register
class OccurrenceImporter(BaseSheetImporter):
    """
    Example import commands for different data sources:
        ./manage.py migrate_data run occurrence_legacy \
            private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL \
            --limit 10 --dry-run

        ./manage.py migrate_data run occurrence_legacy \
            private-media/legacy_data/TEC/ --sources TEC \
            --wipe-targets

    IMPLEMENTATION NOTES / TODOs:
    - Task 12177/12180 (OCCLocation.district/region): Uses DISTRICTS.csv + CALM_DISTRICTS.csv +
      CALM_REGIONS.csv chain. Requires LegacyValueMap pre-populated. Verify with S&C team.
    - Task 12225 (OCCIdentification.identification_certainty): Maps OCC_STATUS_CODE
      "Identified"→"High", "Believed"→"Medium". Confirm complete mapping with S&C team.
    - Task 12287 (OccurrenceDocument.uploaded_by): USERNAME column renamed to ADD_USERNAME
      in ADDITIONAL_DATA to avoid collision with SITES USERNAME. Review if better approach exists.
    """

    slug = "occurrence_legacy"
    integrity_tables = ["boranga_occurrence"]
    description = "Import occurrence data from legacy TEC / TFAUNA / TPFL sources"

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """Delete Occurrence target data and obvious children. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            logger.info("OccurrenceImporter.clear_targets: dry-run, skipping delete")
            return

        from boranga.components.data_migration.adapters.sources import (
            SOURCE_GROUP_TYPE_MAP,
        )

        sources = options.get("sources")
        target_group_types = set()
        if sources:
            for s in sources:
                if s in SOURCE_GROUP_TYPE_MAP:
                    target_group_types.add(SOURCE_GROUP_TYPE_MAP[s])

        # If specific sources are requested, filter deletions by group_type.
        is_filtered = bool(sources)

        occ_filter = {}
        report_filter = {}
        rel_filter = {}

        if is_filtered:
            if not target_group_types:
                logger.warning(
                    "clear_targets: sources %s provided but no associated group_types found in map. Skipping delete.",
                    sources,
                )
                return

            if len(sources) == 1 and sources[0] == Source.TEC_BOUNDARIES.value:
                logger.warning(
                    "OccurrenceImporter: TEC_BOUNDARIES with --wipe-targets: skipping delete to preserve "
                    "existing buffer_radius and other OccurrenceGeometry fields. Will use update path instead."
                )
                return

            logger.warning(
                "OccurrenceImporter: deleting Occurrence and related data for group_types: %s ...",
                target_group_types,
            )
            occ_filter = {"group_type__name__in": target_group_types}
            report_filter = {"group_type__name__in": target_group_types}
            rel_filter = {"occurrence__group_type__name__in": target_group_types}
        else:
            logger.warning("OccurrenceImporter: deleting ALL Occurrence and related data...")

        # Delete reversion history first (more efficient than waiting for cascade)
        from boranga.components.data_migration.history_cleanup.reversion_cleanup import ReversionHistoryCleaner

        cleaner = ReversionHistoryCleaner(batch_size=2000)
        cleaner.clear_occurrence_and_related(occ_filter if is_filtered else {})
        logger.info("Reversion cleanup completed. Stats: %s", cleaner.get_stats())

        # Perform deletes in an autocommit block so they are committed
        # immediately. This avoids the case where clear_targets runs inside a
        # larger transaction that later rolls back leaving the wipe undone.
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)
        try:
            try:
                # Delete SubmitterInformation first — it is not cascade-deleted when the OCR is deleted
                # (the FK sits on OccurrenceReport with on_delete=SET_NULL, so deleting the OCR orphans
                # the SubmitterInformation row without this explicit step).
                if is_filtered:
                    SubmitterInformation.objects.filter(
                        occurrence_report__group_type__name__in=target_group_types
                    ).delete()
                else:
                    SubmitterInformation.objects.filter(occurrence_report__isnull=False).delete()
                # Delete OccurrenceReport objects first as they depend on Occurrences
                OccurrenceReport.objects.filter(**report_filter).delete()

                # Nullify self-reference that blocks deletion of Occurrence
                Occurrence.objects.filter(**occ_filter).update(combined_occurrence=None)

                # Delete OccurrenceTenure before OccurrenceGeometry: deleting OccurrenceGeometry
                # triggers SET_NULL_AND_HISTORICAL which severs the occurrence_geometry FK on
                # OccurrenceTenure rows, making any subsequent source-scoped lookup return 0.
                # We must also catch already-historical tenures (occurrence_geometry=NULL) from
                # prior runs — those can't be found via the FK path, but their historical_occurrence
                # integer field still references the occurrence ID.
                from django.db.models import Q

                from boranga.components.occurrence.models import OccurrenceTenure

                if is_filtered:
                    occ_ids = list(
                        Occurrence.objects.filter(group_type__name__in=target_group_types).values_list("id", flat=True)
                    )
                    OccurrenceTenure.objects.filter(
                        Q(occurrence_geometry__occurrence__group_type__name__in=target_group_types)
                        | Q(historical_occurrence__in=occ_ids)
                    ).delete()
                else:
                    OccurrenceTenure.objects.all().delete()

                relations = [
                    OCCContactDetail,
                    OCCLocation,
                    OccurrenceSite,
                    OCCObservationDetail,
                    OCCHabitatComposition,
                    OCCFireHistory,
                    OCCAssociatedSpecies,
                    OccurrenceDocument,
                    OCCIdentification,
                    OCCHabitatCondition,
                    OCCPlantCount,
                    OCCVegetationStructure,
                    OccurrenceGeometry,
                ]
                for model in relations:
                    model.objects.filter(**rel_filter).delete()

                if not is_filtered:
                    AssociatedSpeciesTaxonomy.objects.all().delete()

            except Exception:
                logger.exception("Failed to delete related Occurrence data")
            try:
                Occurrence.objects.filter(**occ_filter).delete()
            except Exception:
                logger.exception("Failed to delete Occurrence")

        finally:
            if not was_autocommit:
                conn.set_autocommit(False)

        # Reset the primary key sequence for Occurrence, OccurrenceReport and OccurrenceTenure
        # when using PostgreSQL. OccurrenceTenure is included here because it is deleted above
        # (before OccurrenceGeometry) so its sequence must also be reset at this point.
        try:
            from boranga.components.occurrence.models import OccurrenceTenure

            if getattr(conn, "vendor", None) == "postgresql":
                for model in [Occurrence, OccurrenceReport, OccurrenceTenure]:
                    table = model._meta.db_table
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT MAX(id) FROM {table}")
                        row = cur.fetchone()
                        max_id = row[0] if row else None

                        if max_id is not None:
                            cur.execute(
                                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                                [table, "id", max_id, True],
                            )
                        else:
                            cur.execute(
                                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                                [table, "id", 1, False],
                            )
                    logger.info("Reset primary key sequence for table %s to %s", table, max_id)
        except Exception:
            logger.exception("Failed to reset primary key sequences")

    def add_arguments(self, parser):
        parser.add_argument(
            "--path-map",
            nargs="+",
            metavar="SRC=PATH",
            help="Per-source path overrides (e.g. TPFL=/tmp/tpfl.xlsx). If omitted, --path is reused.",
        )
        try:
            parser.add_argument(
                "--sources",
                nargs="+",
                choices=list(SOURCE_ADAPTERS.keys()),
                help="Subset of sources (default: all implemented)",
            )
        except argparse.ArgumentError:
            # Already added by management command
            pass

        # GIS district arguments
        try:
            parser.add_argument(
                "--no-gis-district",
                action="store_true",
                default=False,
                help=(
                    "Skip GIS district assignment entirely. "
                    "By default the handler uses --districts-file (if provided) or "
                    "the WFS endpoint to set OCCLocation.district / region."
                ),
            )
            parser.add_argument(
                "--districts-file",
                default=None,
                help=(
                    "Path to a local GeoJSON file containing district boundaries. "
                    "Use this instead of the WFS endpoint when the server is not "
                    "reachable (e.g. dev environments)."
                ),
            )
            parser.add_argument(
                "--districts-url",
                default=None,
                help=(
                    "Override the WFS GetFeature URL used to download district boundaries. "
                    "Defaults to GIS_SERVER_URL with the CPT_DBCA_DISTRICTS layer. "
                    "Ignored when --districts-file is provided."
                ),
            )
            parser.add_argument(
                "--district-name-field",
                default=None,
                help=(
                    "Property name in the GeoJSON/WFS response that holds the district name. "
                    "If not set, a list of common field names is tried."
                ),
            )
            parser.add_argument(
                "--invert-xy",
                action="store_true",
                default=False,
                help=(
                    "Swap x/y of district geometries from WFS. Off by default because "
                    "GeoJSON outputFormat already returns coordinates in lon/lat order."
                ),
            )
            parser.add_argument(
                "--no-invert-xy",
                dest="invert_xy",
                action="store_false",
                help="Do not swap x/y of district geometries from WFS.",
            )
        except argparse.ArgumentError:
            pass

    def _parse_path_map(self, pairs):
        out = {}
        if not pairs:
            return out
        for p in pairs:
            if "=" not in p:
                raise ValueError(f"Invalid path-map entry: {p}")
            k, v = p.split("=", 1)
            out[k] = v
        return out

    def run(self, path: str, ctx: ImportContext, **options):
        start_time = timezone.now()
        logger.info(
            "OccurrenceImporter (%s) started at %s (dry_run=%s)",
            self.slug,
            start_time.isoformat(),
            ctx.dry_run,
        )
        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []
        errors_details = []
        warnings_details = []

        # 1. Extract
        for src in sources:
            adapter = SOURCE_ADAPTERS[src]
            src_path = path_map.get(src, path)
            result = adapter.extract(src_path, **options)
            for w in result.warnings:
                warnings.append(f"{src}: {w.message}")
            for r in result.rows:
                r["_source"] = src
            all_rows.extend(result.rows)

        # Apply optional global per-importer limit (ctx.limit) after extraction
        limit = getattr(ctx, "limit", None)
        if limit:
            try:
                all_rows = all_rows[: int(limit)]
            except Exception:
                pass

        # 2. Build pipelines per-source by merging base schema pipelines with
        # any adapter-provided `PIPELINES`. This allows adapters to own
        # source-specific transform bindings while the importer runs them
        # uniformly.
        from boranga.components.data_migration.registry import (
            registry as transform_registry,
        )

        base_column_names = schema.COLUMN_PIPELINES or {}
        pipelines_by_source: dict[str, dict] = {}
        for src_key, adapter in SOURCE_ADAPTERS.items():
            src_column_names = dict(base_column_names)
            adapter_pipes = getattr(adapter, "PIPELINES", None)
            if adapter_pipes:
                src_column_names.update(adapter_pipes)

            built: dict[str, list] = {}
            for col, names in src_column_names.items():
                built[col] = transform_registry.build_pipeline(names)
            pipelines_by_source[src_key] = built

        # Build a `pipelines` mapping (keys only) used by merge_group to know
        # which canonical columns to consider when merging entries. Use the
        # union of columns across all source-specific pipelines.
        all_columns = set()
        for built in pipelines_by_source.values():
            all_columns.update(built.keys())
        if not all_columns and schema.COLUMN_PIPELINES:
            all_columns.update(schema.COLUMN_PIPELINES.keys())
        pipelines = {col: None for col in sorted(all_columns)}

        processed = 0
        errors = 0
        created = 0
        updated = 0
        skipped = 0
        warn_count = 0

        # 3. Transform every row into canonical form, collect per-key groups
        groups: dict[str, list[tuple[dict, str, list[tuple[str, Any]]]]] = defaultdict(list)
        # groups[migrated_from_id] -> list of (transformed_dict, source, issues_list)

        for row in all_rows:
            processed += 1
            # progress output every 500 rows
            if processed % 500 == 0:
                logger.info(
                    "OccurrenceImporter %s: processed %d rows so far",
                    self.slug,
                    processed,
                )
            # Adapter provides canonical row (already mapped from raw CSV)
            canonical_row = row

            tcx = TransformContext(row=canonical_row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            # Choose pipeline map based on the row source (fallback to base)
            src = row.get("_source")
            pipeline_map = pipelines_by_source.get(src, pipelines_by_source.get(None, {}))
            for col, pipeline in pipeline_map.items():
                raw_val = canonical_row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)
                transformed[col] = res.value
                for issue in res.issues:
                    issues.append((col, issue))
                    level = getattr(issue, "level", "error")
                    record = {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": col,
                        "level": level,
                        "message": getattr(issue, "message", str(issue)),
                        "raw_value": raw_val,
                    }
                    if level == "error":
                        has_error = True
                        errors += 1
                        errors_details.append(record)
                    else:
                        warn_count += 1
                        warnings_details.append(record)
            if has_error:
                skipped += 1
                continue

            # copy adapter-added keys (e.g. group_type_id) from the source row into
            # the transformed dict so they survive the merge. Also preserve _nested_*
            # keys which hold related model data, and _source for pipeline selection.
            for k, v in row.items():
                if k.startswith("_"):
                    # Preserve _nested_* keys and _source, skip other internals
                    if not (k.startswith("_nested_") or k == "_source"):
                        continue
                if k in transformed:
                    continue
                transformed[k] = v

            # Prepend source prefix to migrated_from_id if present
            _src = row.get("_source")
            _mid = transformed.get("migrated_from_id")
            if _src and _mid:
                if _src == "TEC_BOUNDARIES":
                    # Special case: TEC_BOUNDARIES links to TEC records, so use 'tec-' prefix
                    prefix = "tec"
                else:
                    prefix = _src.lower().replace("_", "-")

                if not str(_mid).startswith(f"{prefix}-"):
                    transformed["migrated_from_id"] = f"{prefix}-{_mid}"

            key = transformed.get("migrated_from_id")
            if not key:
                # missing key — cannot merge/persist
                skipped += 1
                errors += 1
                errors_details.append({"reason": "missing_migrated_from_id", "row": transformed})
                continue
            groups[key].append((transformed, row.get("_source"), issues))

        # 4. Merge groups and persist one object per key
        def merge_group(entries, source_priority):
            """
            Merge canonical columns (from schema pipelines) and also preserve any
            adapter-added keys found in transformed dicts (first non-empty wins).
            """
            entries_sorted = sorted(
                entries,
                key=lambda e: source_priority.index(e[1]) if e[1] in source_priority else len(source_priority),
            )
            merged = {}
            combined_issues = []
            # first merge the canonical columns defined by the schema/pipelines
            for col in pipelines.keys():
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(col)
                    # Accept empty string as a valid value (e.g., for text fields with defaults)
                    if v not in (None,):
                        val = v
                        break
                merged[col] = val
            # also merge any adapter-added keys that are present in transformed dicts
            extra_keys = set().union(*(set(trans.keys()) for trans, _, _ in entries_sorted))
            for extra in sorted(extra_keys):
                if extra in pipelines:
                    continue
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(extra)
                    # Accept empty string as a valid value
                    if v not in (None,):
                        val = v
                        break
                merged[extra] = val
            for _, _, iss in entries_sorted:
                combined_issues.extend(iss)
            return merged, combined_issues

        # Persist merged rows in bulk where possible (prepare ops then create/update)

        ops = []
        for migrated_from_id, entries in groups.items():
            merged, combined_issues = merge_group(entries, sources)
            # if any error in combined_issues => skip
            if any(i.level == "error" for _, i in combined_issues):
                skipped += 1
                continue

            involved_sources = sorted({src for _, src, _ in entries})
            occ_row = schema.OccurrenceRow.from_dict(merged)
            source_for_validation = involved_sources[0] if len(involved_sources) == 1 else None
            validation_issues = occ_row.validate(source=source_for_validation)
            if validation_issues:
                for level, msg in validation_issues:
                    rec = {
                        "migrated_from_id": merged.get("migrated_from_id"),
                        "reason": "validation",
                        "level": level,
                        "message": msg,
                        "row": merged,
                    }
                    if level == "error":
                        errors_details.append(rec)
                    else:
                        warnings_details.append(rec)
                if any(level == "error" for level, _ in validation_issues):
                    skipped += 1
                    errors += sum(1 for level, _ in validation_issues if level == "error")
                    continue

            # QA check: occurrence_name must be unique for the same species
            if occ_row.occurrence_name and occ_row.species_id:
                dup_exists = (
                    Occurrence.objects.filter(
                        species_id=occ_row.species_id,
                        occurrence_name=occ_row.occurrence_name,
                    )
                    .exclude(migrated_from_id=migrated_from_id)
                    .exists()
                )
                if dup_exists:
                    rec = {
                        "migrated_from_id": merged.get("migrated_from_id"),
                        "reason": "duplicate_occurrence_name",
                        "level": "error",
                        "message": (
                            f"occurrence_name '{occ_row.occurrence_name}' "
                            f"already exists for species {occ_row.species_id}"
                        ),
                        "row": merged,
                    }
                    errors_details.append(rec)
                    skipped += 1
                    errors += 1
                    continue

            defaults = occ_row.to_model_defaults()

            # If MODIFIED_DATE was blank, fall back to datetime_created so we
            # don't store the migration run time as the last-modified date.
            # This must happen before apply_model_defaults, which would otherwise
            # replace the None with timezone.now() (the field's Python default).
            if defaults.get("datetime_updated") is None and defaults.get("datetime_created") is not None:
                defaults["datetime_updated"] = defaults["datetime_created"]

            # If last_modified_by is not set, fall back to submitter (from CREATED_BY).
            if defaults.get("last_modified_by") is None and defaults.get("submitter") is not None:
                defaults["last_modified_by"] = defaults["submitter"]

            # Apply model defaults (handles None -> "" for non-nullable text fields, etc.)
            apply_model_defaults(Occurrence, defaults)

            # If dry-run, log planned defaults and skip adding to ops so no DB work
            if ctx.dry_run:
                pretty = json.dumps(defaults, default=str, indent=2, sort_keys=True)
                logger.debug(
                    "OccurrenceImporter %s dry-run: would persist migrated_from_id=%s defaults:\n%s",
                    self.slug,
                    migrated_from_id,
                    pretty,
                )
                continue

            ops.append(
                {
                    "migrated_from_id": migrated_from_id,
                    "defaults": defaults,
                    "merged": merged,
                    "sources": list(involved_sources),
                }
            )

        # Free large intermediate structures — all_rows and groups are no longer
        # needed once ops is built; releasing them reduces peak memory usage.
        del all_rows
        del groups

        # Note: do not exit early on dry-run here — continue so error CSV is generated

        # Geometry-only sources (e.g. TEC_BOUNDARIES) only patch OccurrenceGeometry —
        # there is no Occurrence-level data to create or update.
        _geometry_only_sources = {Source.TEC_BOUNDARIES.value}
        _is_geometry_only_run = bool(sources) and all(s in _geometry_only_sources for s in sources)

        # Determine existing occurrences and plan create vs update
        migrated_keys = [o["migrated_from_id"] for o in ops]
        existing_by_migrated = {
            s.migrated_from_id: s for s in Occurrence.objects.filter(migrated_from_id__in=migrated_keys)
        }

        to_create = []
        create_meta = []
        to_update = []
        BATCH = 1000

        for op in ops:
            migrated_from_id = op["migrated_from_id"]
            defaults = op["defaults"]
            merged = op.get("merged") or {}

            obj = existing_by_migrated.get(migrated_from_id)
            if obj:
                # For geometry-only sources (TEC_BOUNDARIES) the Occurrence record
                # itself needs no update — only OccurrenceGeometry is patched.
                if not _is_geometry_only_run:
                    for k, v in defaults.items():
                        apply_value_to_instance(obj, k, v)
                    to_update.append(obj)
                continue

            # Check if we should skip creation for boundary-only sources
            involved_sources = op.get("sources", [])
            if len(involved_sources) == 1 and Source.TEC_BOUNDARIES.value in involved_sources:
                logger.warning(
                    "Skipping creation of Occurrence %s (found in TEC_BOUNDARIES but not in primary TEC source)",
                    migrated_from_id,
                )
                errors_details.append(
                    {
                        "migrated_from_id": migrated_from_id,
                        "reason": "missing_primary_record",
                        "level": "error",
                        "message": "Occurrence found in TEC_BOUNDARIES but not in primary TEC source",
                        "row": merged,
                    }
                )
                skipped += 1
                errors += 1
                continue

            create_kwargs = dict(defaults)
            create_kwargs["migrated_from_id"] = migrated_from_id
            if getattr(ctx, "migration_run", None) is not None:
                create_kwargs["migration_run"] = ctx.migration_run
            inst = Occurrence(**normalize_create_kwargs(Occurrence, create_kwargs))
            to_create.append(inst)
            create_meta.append(
                (
                    migrated_from_id,
                    merged.get("OCCContactDetail__contact"),
                    merged.get("OCCContactDetail__contact_name"),
                    merged.get("OCCContactDetail__notes"),
                    merged.get("modified_by"),
                    merged.get("datetime_updated"),
                )
            )

        # Bulk create new Occurrences
        created_map = {}
        if to_create:
            logger.info(
                "OccurrenceImporter: bulk-creating %d new Occurrences",
                len(to_create),
            )
            for i in range(0, len(to_create), BATCH):
                chunk = to_create[i : i + BATCH]
                with transaction.atomic():
                    Occurrence.objects.bulk_create(chunk, batch_size=BATCH)

        # Refresh created objects to get PKs
        if create_meta:
            created_keys = [m[0] for m in create_meta]
            for s in Occurrence.objects.filter(migrated_from_id__in=created_keys):
                created_map[s.migrated_from_id] = s

        # If migration_run present, ensure it's attached to created objects
        if created_map and getattr(ctx, "migration_run", None) is not None:
            try:
                Occurrence.objects.filter(migrated_from_id__in=list(created_map.keys())).update(
                    migration_run=ctx.migration_run
                )
            except Exception:
                logger.exception("Failed to attach migration_run to some created Occurrence(s)")

        # Ensure occurrence_number is populated for newly-created Occurrence objects.
        # The model normally sets this in save() as 'OCC' + pk, but bulk_create
        # bypasses save(), so we must set it here using the assigned PKs.
        if created_map:
            occs_to_update = []
            for occ in created_map.values():
                try:
                    if not getattr(occ, "occurrence_number", None):
                        occ.occurrence_number = f"OCC{occ.pk}"
                        occs_to_update.append(occ)
                except Exception:
                    logger.exception(
                        "Error preparing occurrence_number for Occurrence %s",
                        getattr(occ, "pk", None),
                    )
            if occs_to_update:
                try:
                    Occurrence.objects.bulk_update(occs_to_update, ["occurrence_number"], batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_update occurrence_number; falling back to individual saves")
                    for occ in occs_to_update:
                        try:
                            occ.save(update_fields=["occurrence_number"])
                        except Exception:
                            logger.exception(
                                "Failed to save occurrence_number for Occurrence %s",
                                getattr(occ, "pk", None),
                            )

        # Bulk update existing objects
        if to_update:
            logger.info(
                "OccurrenceImporter: bulk-updating %d existing Occurrences",
                len(to_update),
            )
            update_instances = to_update
            # determine fields to update: include only fields that are
            # non-None on every instance. Using the union (fields present on
            # some instances) can cause bulk_update to write NULL into rows
            # for instances where the attribute is None, which violates NOT
            # NULL constraints. Restricting to fields present on all instances
            # avoids that. Also exclude id and migrated_from_id which cannot be updated.
            fields = []
            if update_instances:
                all_fields = [f for f in update_instances[0]._meta.fields]
                for f in all_fields:
                    if f.name in ("id", "migrated_from_id"):
                        continue
                    # include field only if every instance has a non-None value
                    try:
                        if all(getattr(inst, f.name, None) is not None for inst in update_instances):
                            fields.append(f.name)
                    except Exception:
                        # Be conservative: skip fields that raise on getattr
                        continue
            # perform bulk_update only if we have safe fields to update
            try:
                if fields:
                    Occurrence.objects.bulk_update(update_instances, fields, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update Occurrence; falling back to individual saves")
                for inst in update_instances:
                    try:
                        # Build a conservative per-instance update_fields list:
                        # include only model fields that currently have a non-None
                        # value on the instance. This avoids attempting to write
                        # NULL into non-nullable DB columns when the instance
                        # attribute is None. Also exclude id and migrated_from_id.
                        update_fields = [
                            f.name
                            for f in inst._meta.fields
                            if getattr(inst, f.name, None) is not None and f.name not in ("id", "migrated_from_id")
                        ]
                        if update_fields:
                            inst.save(update_fields=update_fields, override_datetime_updated=True)
                        else:
                            # Nothing to update (all values are None or only PK), skip
                            logger.debug(
                                "Skipping save for Occurrence %s: no updatable fields",
                                getattr(inst, "pk", None),
                            )
                    except Exception:
                        logger.exception(
                            "Failed to save Occurrence %s",
                            getattr(inst, "pk", None),
                        )

        # Process related objects in chunks to avoid massive SQL queries.
        # For geometry-only sources (TEC_BOUNDARIES) a larger chunk amortises the
        # fixed per-chunk DB overhead (prefetches, Occurrence fetch) over more rows.
        RELATED_BATCH_SIZE = 5000 if _is_geometry_only_run else 1000
        total_ops = len(ops)

        # Load DocumentCategory "ORF Document" once
        from boranga.components.species_and_communities.models import DocumentCategory

        try:
            orf_document_category = DocumentCategory.objects.get(document_category_name="ORF Document")
        except DocumentCategory.DoesNotExist:
            logger.warning(
                "DocumentCategory 'ORF Document' not found. OccurrenceDocuments will be created without category."
            )
            orf_document_category = None

        # Load ContentType for Occurrence (used by OccurrenceGeometry.content_type)
        from django.contrib.contenttypes.models import ContentType

        try:
            occ_content_type = ContentType.objects.get_for_model(Occurrence)
        except Exception:
            logger.warning("Could not resolve ContentType for Occurrence model")
            occ_content_type = None

        # Preload district_id → region_id for deriving OCCLocation.region from district (task 14938)
        district_to_region_id: dict[int, int] = {
            d.pk: d.region_id for d in District.objects.filter(region_id__isnull=False).only("pk", "region_id")
        }

        # GIS district lookup: download district boundaries ONCE from WFS and build an
        # in-memory Shapely index.  Only enabled when TEC (or TEC_BOUNDARIES) is in the
        # active source set and --no-gis-district has not been passed.
        _tec_sources = {Source.TEC.value, Source.TEC_BOUNDARIES.value}
        _run_gis_district = (
            not options.get("no_gis_district") and any(s in _tec_sources for s in sources) and not ctx.dry_run
        )
        district_geo_lookup: list = []  # [(district_pk, region_pk, shapely_geom), ...]
        if _run_gis_district:
            _name_field = options.get("district_name_field")
            _invert_xy = options.get("invert_xy", False)
            _districts_file = options.get("districts_file")
            _raw_shapes = []

            if _districts_file:
                # Local file takes priority over WFS
                logger.info("GIS district: using local file %s", _districts_file)
                _raw_shapes = _load_district_shapes_from_file(_districts_file, invert_xy=False, name_field=_name_field)
            else:
                _districts_url = options.get("districts_url")
                if not _districts_url:
                    _gis_base = getattr(settings, "GIS_SERVER_URL", None)
                    if _gis_base:
                        _gis_base = _gis_base.rstrip("/")
                        _sep = "&" if "?" in _gis_base else "?"
                        _districts_url = (
                            f"{_gis_base}{_sep}service=WFS&version=2.0.0&request=GetFeature"
                            f"&typeName={_DISTRICT_WFS_LAYER}&outputFormat=application%2Fjson"
                            "&srsName=EPSG%3A4326"
                        )
                if _districts_url:
                    logger.info("GIS district WFS URL: %s", _districts_url)
                    _raw_shapes = _load_district_shapes_from_wfs(
                        _districts_url, invert_xy=_invert_xy, name_field=_name_field
                    )
                else:
                    logger.warning(
                        "GIS district: GIS_SERVER_URL not configured and neither "
                        "--districts-file nor --districts-url was provided"
                    )

            if _raw_shapes:
                district_geo_lookup = _build_district_geo_lookup(_raw_shapes)
            else:
                logger.warning(
                    "GIS district: no district shapes loaded — district/region will NOT be set. "
                    "Provide --districts-file <path/to/districts.geojson> or ensure the WFS endpoint is reachable."
                )

        logger.info(
            "OccurrenceImporter: processing related objects in chunks (total %d ops)...",
            total_ops,
        )

        for i in range(0, total_ops, RELATED_BATCH_SIZE):
            chunk_ops = ops[i : i + RELATED_BATCH_SIZE]

            logger.info("Processing related objects: %d / %d ...", i + len(chunk_ops), total_ops)

            chunk_mig_ids = [o["migrated_from_id"] for o in chunk_ops]

            # Fetch occurrences for this chunk
            chunk_occs = list(Occurrence.objects.filter(migrated_from_id__in=chunk_mig_ids))
            chunk_occ_map = {o.migrated_from_id: o for o in chunk_occs}
            chunk_occ_ids = [o.pk for o in chunk_occs]

            # Helper
            def get_chunk_occ(mig_id):
                return chunk_occ_map.get(mig_id)

            # --- OCCContactDetail ---
            # Skip for geometry-only sources (TEC_BOUNDARIES) — no contact data in source.
            existing_contacts = set()
            if not _is_geometry_only_run and not getattr(ctx, "wipe_targets", False):
                existing_contacts = set(
                    OCCContactDetail.objects.filter(occurrence_id__in=chunk_occ_ids).values_list(
                        "occurrence_id", flat=True
                    )
                )

            want_contact_create = []

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                if (
                    merged.get("OCCContactDetail__contact")
                    or merged.get("OCCContactDetail__contact_name")
                    or merged.get("OCCContactDetail__notes")
                ):
                    if occ.pk not in existing_contacts:
                        want_contact_create.append(
                            OCCContactDetail(
                                occurrence=occ,
                                contact=merged.get("OCCContactDetail__contact") or "",
                                contact_name=merged.get("OCCContactDetail__contact_name") or "",
                                notes=merged.get("OCCContactDetail__notes") or "",
                                visible=True,
                            )
                        )
                        existing_contacts.add(occ.pk)

            if want_contact_create:
                try:
                    OCCContactDetail.objects.bulk_create(want_contact_create, batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_create OCCContactDetail; falling back to individual creates")
                    for obj in want_contact_create:
                        try:
                            obj.save()
                        except Exception:
                            logger.exception("Failed to create OCCContactDetail")

            # --- OccurrenceUserAction ---
            # Skip for geometry-only sources (TEC_BOUNDARIES) — no action data in source.
            existing_actions = set()
            if not _is_geometry_only_run and not getattr(ctx, "wipe_targets", False):
                existing_actions = set(
                    OccurrenceUserAction.objects.filter(occurrence_id__in=chunk_occ_ids).values_list(
                        "occurrence_id", flat=True
                    )
                )

            want_action_create = []

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                if occ.pk in existing_actions:
                    continue

                modified_by = merged.get("modified_by")
                datetime_updated = merged.get("datetime_updated")
                if modified_by and datetime_updated:
                    want_action_create.append(
                        OccurrenceUserAction(
                            occurrence=occ,
                            what="Edited in TPFL",
                            when=datetime_updated,
                            who=modified_by,
                        )
                    )

            if want_action_create:
                try:
                    OccurrenceUserAction.objects.bulk_create(want_action_create, batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_create OccurrenceUserAction; falling back to individual creates")
                    for obj in want_action_create:
                        try:
                            obj.save()
                        except Exception:
                            logger.exception("Failed to create OccurrenceUserAction")

            # --- Related Models (OneToOne) ---
            loc_create, loc_update = [], []
            obs_create, obs_update = [], []
            hab_create, hab_update = [], []
            fire_create, fire_update = [], []
            assoc_create, assoc_update = [], []
            doc_create, doc_update = [], []
            geo_create, geo_update = [], []
            ident_create, ident_update = [], []
            veg_create, veg_update = [], []
            hcond_create, hcond_update = [], []
            plant_create, plant_update = [], []

            existing_locs = {}
            existing_obs = {}
            existing_hab = {}
            existing_fire = {}
            existing_assoc = {}
            existing_docs = {}
            existing_geo = {}
            existing_ident = {}
            existing_veg = {}
            existing_hcond = {}
            existing_plant = {}

            if not getattr(ctx, "wipe_targets", False):
                # For geometry-only sources only fetch OccurrenceGeometry — the other
                # 10 models carry no data for these rows and the queries would be wasted.
                if not _is_geometry_only_run:
                    existing_locs = {
                        loc.occurrence_id: loc for loc in OCCLocation.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_obs = {
                        o.occurrence_id: o for o in OCCObservationDetail.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_hab = {
                        h.occurrence_id: h
                        for h in OCCHabitatComposition.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_fire = {
                        f.occurrence_id: f for f in OCCFireHistory.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_assoc = {
                        a.occurrence_id: a for a in OCCAssociatedSpecies.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_docs = {
                        d.occurrence_id: d for d in OccurrenceDocument.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                existing_geo = {
                    g.occurrence_id: g for g in OccurrenceGeometry.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                if not _is_geometry_only_run:
                    existing_ident = {
                        i.occurrence_id: i for i in OCCIdentification.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_veg = {
                        v.occurrence_id: v
                        for v in OCCVegetationStructure.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_hcond = {
                        hc.occurrence_id: hc
                        for hc in OCCHabitatCondition.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }
                    existing_plant = {
                        p.occurrence_id: p for p in OCCPlantCount.objects.filter(occurrence_id__in=chunk_occ_ids)
                    }

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                # OCCLocation
                if not _is_geometry_only_run and any(k.startswith("OCCLocation__") for k in merged):
                    # Concatenate location_description with LGA code if present
                    loc_desc = merged.get("OCCLocation__location_description")
                    lga_code = merged.get("OCCLocation__lga_code")
                    if loc_desc and lga_code:
                        loc_desc = f"{loc_desc} LGA: {lga_code}"
                    elif lga_code:
                        loc_desc = f"LGA: {lga_code}"
                    district_id = merged.get("OCCLocation__district_id")
                    # Task 14938: derive region from district (no separate REGION column in source)
                    region_id = district_to_region_id.get(district_id) if district_id else None
                    defaults = {
                        "coordinate_source_id": merged.get("OCCLocation__coordinate_source_id"),
                        "boundary_description": merged.get("OCCLocation__boundary_description"),
                        "locality": merged.get("OCCLocation__locality"),
                        "location_description": loc_desc,
                        "district_id": district_id,
                        "region_id": region_id,
                        "location_accuracy_id": merged.get("OCCLocation__location_accuracy_id"),
                    }
                    apply_model_defaults(OCCLocation, defaults)
                    if occ.pk in existing_locs:
                        obj = existing_locs[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        loc_update.append(obj)
                    else:
                        loc_create.append(OCCLocation(occurrence=occ, **defaults))

                # OCCObservationDetail
                if not _is_geometry_only_run and any(k.startswith("OCCObservationDetail__") for k in merged):
                    defaults = {
                        "comments": merged.get("OCCObservationDetail__comments"),
                        "area_assessment_id": merged.get("OCCObservationDetail__area_assessment_id"),
                        "area_surveyed": merged.get("OCCObservationDetail__area_surveyed"),
                        "survey_duration": merged.get("OCCObservationDetail__survey_duration"),
                    }
                    apply_model_defaults(OCCObservationDetail, defaults)
                    if occ.pk in existing_obs:
                        obj = existing_obs[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        obs_update.append(obj)
                    else:
                        obs_create.append(OCCObservationDetail(occurrence=occ, **defaults))

                # OCCHabitatComposition
                if not _is_geometry_only_run and any(k.startswith("OCCHabitatComposition__") for k in merged):
                    # land_form and soil_type are MultiSelectField — wrap in list
                    _lf = merged.get("OCCHabitatComposition__land_form")
                    _st = merged.get("OCCHabitatComposition__soil_type")
                    defaults = {
                        "water_quality": merged.get("OCCHabitatComposition__water_quality"),
                        "habitat_notes": merged.get("OCCHabitatComposition__habitat_notes"),
                        "drainage_id": merged.get("OCCHabitatComposition__drainage_id"),
                        "land_form": [str(_lf)] if _lf else [],
                        "loose_rock_percent": merged.get("OCCHabitatComposition__loose_rock_percent"),
                        "rock_type_id": merged.get("OCCHabitatComposition__rock_type_id"),
                        "soil_colour_id": merged.get("OCCHabitatComposition__soil_colour_id"),
                        "soil_condition_id": merged.get("OCCHabitatComposition__soil_condition_id"),
                        "soil_type": [str(_st)] if _st else [],
                    }
                    apply_model_defaults(OCCHabitatComposition, defaults)
                    if occ.pk in existing_hab:
                        obj = existing_hab[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        hab_update.append(obj)
                    else:
                        hab_create.append(OCCHabitatComposition(occurrence=occ, **defaults))

                # OCCFireHistory
                if not _is_geometry_only_run and any(k.startswith("OCCFireHistory__") for k in merged):
                    # Build comment from fire_season (transformed) + fire_year
                    fire_comment = merged.get("OCCFireHistory__comment")
                    if not fire_comment:
                        fire_season = merged.get("OCCFireHistory__fire_season")
                        fire_year = merged.get("OCCFireHistory__fire_year")
                        fc_parts = [p for p in (fire_season, fire_year) if p]
                        fire_comment = " ".join(fc_parts) if fc_parts else None
                    defaults = {
                        "comment": fire_comment,
                        "intensity_id": merged.get("OCCFireHistory__intensity_id"),
                    }
                    apply_model_defaults(OCCFireHistory, defaults)
                    if occ.pk in existing_fire:
                        obj = existing_fire[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        fire_update.append(obj)
                    else:
                        fire_create.append(OCCFireHistory(occurrence=occ, **defaults))

                # OCCAssociatedSpecies
                if not _is_geometry_only_run and (
                    any(k.startswith("OCCAssociatedSpecies__") for k in merged) or merged.get("_nested_species")
                ):
                    defaults = {"comment": merged.get("OCCAssociatedSpecies__comment") or ""}
                    if occ.pk in existing_assoc:
                        obj = existing_assoc[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        assoc_update.append(obj)
                    else:
                        new_obj = OCCAssociatedSpecies(occurrence=occ, **defaults)
                        assoc_create.append(new_obj)

                # OccurrenceGeometry
                geo_src = merged.get("_source")
                if geo_src == Source.TPFL.value:
                    # TPFL: build geometry from lat/long as 1m circle polygon
                    tpfl_lat = merged.get("OccurrenceGeometry__latitude")
                    tpfl_lon = merged.get("OccurrenceGeometry__longitude")
                    if tpfl_lat and tpfl_lon:
                        try:
                            from django.contrib.gis.geos import GEOSGeometry
                            from pyproj import Transformer
                            from shapely.geometry import Point as ShapelyPoint
                            from shapely.ops import transform as shapely_transform

                            tpfl_lat = float(tpfl_lat)
                            tpfl_lon = float(tpfl_lon)
                            # Project to Australian Albers (EPSG:3577, metric), buffer 1m,
                            # then project back — consistent with point_to_circle_factory.
                            target_crs = f"EPSG:{settings.DEFAULT_SRID}"
                            to_albers = Transformer.from_crs("EPSG:4283", "EPSG:3577", always_xy=True)
                            from_albers = Transformer.from_crs("EPSG:3577", target_crs, always_xy=True)
                            pt_sh = ShapelyPoint(tpfl_lon, tpfl_lat)
                            pt_alb = shapely_transform(lambda x, y: to_albers.transform(x, y), pt_sh)
                            circ_alb = pt_alb.buffer(1)
                            circ_sh = shapely_transform(lambda x, y: from_albers.transform(x, y), circ_alb)
                            circle = GEOSGeometry(circ_sh.wkt, srid=settings.DEFAULT_SRID)
                            from django.contrib.gis.geos import Point as GEOSPoint

                            original_point = GEOSPoint(tpfl_lon, tpfl_lat, srid=4283)
                            defaults = {
                                "geometry": circle,
                                "original_geometry_ewkb": original_point.ewkb,
                                "locked": True,
                            }
                            if occ_content_type:
                                defaults["content_type"] = occ_content_type
                            apply_model_defaults(OccurrenceGeometry, defaults)
                            if occ.pk in existing_geo:
                                obj = existing_geo[occ.pk]
                                for k, v in defaults.items():
                                    setattr(obj, k, v)
                                geo_update.append(obj)
                            else:
                                geo_create.append(OccurrenceGeometry(occurrence=occ, **defaults))
                        except Exception:
                            logger.exception(
                                "Failed to create geometry from lat/long for migrated_from_id=%s (lat=%s, lon=%s)",
                                mig,
                                tpfl_lat,
                                tpfl_lon,
                            )
                            errors_details.append(
                                {
                                    "migrated_from_id": mig,
                                    "reason": "geometry_creation_failed",
                                    "level": "error",
                                    "message": (
                                        f"Failed to create 1m circle geometry from lat={tpfl_lat}, lon={tpfl_lon}"
                                    ),
                                    "row": merged,
                                }
                            )
                            errors += 1
                elif any(k.startswith("OccurrenceGeometry__") for k in merged):
                    defaults = {
                        "geometry": merged.get("OccurrenceGeometry__geometry"),
                        "locked": merged.get("OccurrenceGeometry__locked"),
                        "buffer_radius": merged.get("OccurrenceGeometry__buffer_radius"),
                    }
                    if occ_content_type:
                        defaults["content_type"] = occ_content_type
                    # Populate original_geometry_ewkb for TEC_BOUNDARIES.
                    # The pipeline has already converted the WKT string to a GEOSGeometry
                    # and stored it in defaults["geometry"], so read .ewkb directly — no
                    # re-parse needed.
                    if geo_src == Source.TEC_BOUNDARIES.value:
                        geom = defaults.get("geometry")
                        if geom:
                            try:
                                defaults["original_geometry_ewkb"] = geom.ewkb
                            except Exception:
                                logger.exception(
                                    "Failed to compute ewkb for migrated_from_id=%s; skipping original_geometry_ewkb",
                                    mig,
                                )
                                errors_details.append(
                                    {
                                        "migrated_from_id": mig,
                                        "reason": "invalid_wkt_geometry",
                                        "level": "error",
                                        "message": "Failed to compute original_geometry_ewkb for OccurrenceGeometry",
                                        "row": merged,
                                    }
                                )
                                errors += 1
                    apply_model_defaults(OccurrenceGeometry, defaults)
                    if occ.pk in existing_geo:
                        obj = existing_geo[occ.pk]
                        # For updates, only set fields that are actually provided (non-None after defaults applied)
                        # This preserves existing values for fields not in the source (e.g., buffer_radius from TEC when updating via TEC_BOUNDARIES)
                        for k, v in defaults.items():
                            # Skip buffer_radius if TEC_BOUNDARIES didn't provide it (preserve existing value)
                            if k == "buffer_radius" and v in (None, 0) and geo_src == Source.TEC_BOUNDARIES.value:
                                continue
                            setattr(obj, k, v)
                        geo_update.append(obj)
                    else:
                        geo_create.append(OccurrenceGeometry(occurrence=occ, **defaults))

                # OccurrenceDocument
                # Check if we have actual data before creating/updating a document on the main row
                doc_sub = merged.get("OccurrenceDocument__document_sub_category_id")
                doc_desc = merged.get("OccurrenceDocument__description")

                if doc_sub is not None or doc_desc:
                    doc_uploaded_by = merged.get("OccurrenceDocument__uploaded_by")
                    defaults = {
                        "document_sub_category_id": doc_sub,
                        "description": doc_desc or "",
                        "document_category": orf_document_category,
                        "uploaded_by": doc_uploaded_by,
                    }
                    if occ.pk in existing_docs:
                        obj = existing_docs[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        doc_update.append(obj)
                    else:
                        doc_create.append(OccurrenceDocument(occurrence=occ, **defaults))

                # OCCIdentification
                if not _is_geometry_only_run and any(k.startswith("OCCIdentification__") for k in merged):
                    # Build identification_comment from vchr_status_code + dupvouch_location
                    vchr = merged.get("OCCIdentification__vchr_status_code")
                    dupv = merged.get("OCCIdentification__dupvouch_location")
                    id_comment_parts: list[str] = []
                    if vchr:
                        id_comment_parts.append(f"Specimen Status: {vchr}")
                    if dupv:
                        id_comment_parts.append(f"Duplicate Voucher Location: {dupv}")
                    id_comment = "; ".join(id_comment_parts) if id_comment_parts else None
                    defaults = {
                        "identification_certainty_id": merged.get("OCCIdentification__identification_certainty_id"),
                        "barcode_number": merged.get("OCCIdentification__barcode_number"),
                        "collector_number": merged.get("OCCIdentification__collector_number"),
                        "identification_comment": id_comment,
                        "permit_id": merged.get("OCCIdentification__permit_id"),
                        "sample_destination_id": merged.get("OCCIdentification__sample_destination_id"),
                    }
                    apply_model_defaults(OCCIdentification, defaults)
                    if occ.pk in existing_ident:
                        obj = existing_ident[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        ident_update.append(obj)
                    else:
                        ident_create.append(OCCIdentification(occurrence=occ, **defaults))

                # OCCVegetationStructure
                veg_layer_one = merged.get("OCCVegetationStructure__vegetation_structure_layer_one")
                if veg_layer_one:
                    defaults = {"vegetation_structure_layer_one": veg_layer_one}
                    apply_model_defaults(OCCVegetationStructure, defaults)
                    if occ.pk in existing_veg:
                        obj = existing_veg[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        veg_update.append(obj)
                    else:
                        veg_create.append(OCCVegetationStructure(occurrence=occ, **defaults))

                # OCCHabitatCondition
                if any(k.startswith("OCCHabitatCondition__") for k in merged):
                    defaults = {
                        "pristine": merged.get("OCCHabitatCondition__pristine"),
                        "excellent": merged.get("OCCHabitatCondition__excellent"),
                        "very_good": merged.get("OCCHabitatCondition__very_good"),
                        "good": merged.get("OCCHabitatCondition__good"),
                        "degraded": merged.get("OCCHabitatCondition__degraded"),
                        "completely_degraded": merged.get("OCCHabitatCondition__completely_degraded"),
                    }
                    # Only create/update if at least one value is non-None
                    if any(v is not None for v in defaults.values()):
                        apply_model_defaults(OCCHabitatCondition, defaults)
                        if occ.pk in existing_hcond:
                            obj = existing_hcond[occ.pk]
                            for k, v in defaults.items():
                                setattr(obj, k, v)
                            hcond_update.append(obj)
                        else:
                            hcond_create.append(OCCHabitatCondition(occurrence=occ, **defaults))

                # OCCPlantCount
                if not _is_geometry_only_run and any(k.startswith("OCCPlantCount__") for k in merged):
                    # Build comment from multiple fields.
                    # POPULATION_NOTES goes first (no prefix), then a <LINE BREAK>,
                    # then the remaining structured fields joined with "; ".
                    pc_pop_notes = merged.get("OCCPlantCount__population_notes")
                    pc_rest_parts: list[str] = []
                    pc_pollinator = merged.get("OCCPlantCount__pollinator_observation")
                    if pc_pollinator:
                        pc_rest_parts.append(f"Pollinator Observation: {pc_pollinator}")
                    pc_area_method = merged.get("OCCPlantCount__area_occupied_method")
                    if pc_area_method:
                        pc_rest_parts.append(f"Area Occupied Method: {pc_area_method}")
                    pc_quad_size = merged.get("OCCPlantCount__quad_size")
                    if pc_quad_size:
                        pc_rest_parts.append(f"Quadrat Size: {pc_quad_size}")
                    pc_quad_total = merged.get("OCCPlantCount__quad_num_total")
                    if pc_quad_total:
                        pc_rest_parts.append(f"Quadrat Num Total: {pc_quad_total}")
                    pc_quad_mat = merged.get("OCCPlantCount__quad_num_mature")
                    if pc_quad_mat:
                        pc_rest_parts.append(f"Quadrat Num Mature: {pc_quad_mat}")
                    pc_quad_juv = merged.get("OCCPlantCount__quad_num_juvenile")
                    if pc_quad_juv:
                        pc_rest_parts.append(f"Quadrat Num Juvenile: {pc_quad_juv}")
                    pc_quad_seed = merged.get("OCCPlantCount__quad_num_seedlings")
                    if pc_quad_seed:
                        pc_rest_parts.append(f"Quadrat Num Seedlings: {pc_quad_seed}")
                    pc_sections = [p for p in [pc_pop_notes, "; ".join(pc_rest_parts)] if p]
                    pc_comment = "\n".join(pc_sections) if pc_sections else None

                    # Derive count_status from detailed/simple data
                    has_detailed = any(
                        merged.get(f"OCCPlantCount__{f}") is not None
                        for f in (
                            "detailed_alive_mature",
                            "detailed_dead_mature",
                            "detailed_alive_juvenile",
                            "detailed_dead_juvenile",
                            "detailed_alive_seedling",
                            "detailed_dead_seedling",
                        )
                    )
                    has_simple = any(
                        merged.get(f"OCCPlantCount__{f}") is not None for f in ("simple_alive", "simple_dead")
                    )
                    if has_detailed:
                        count_status = "detailed_count"
                    elif has_simple:
                        count_status = "simple_count"
                    else:
                        count_status = "not_counted"

                    defaults = {
                        "counted_subject_id": merged.get("OCCPlantCount__counted_subject_id"),
                        "plant_condition_id": merged.get("OCCPlantCount__plant_condition_id"),
                        "plant_count_method_id": merged.get("OCCPlantCount__plant_count_method_id"),
                        "count_status": count_status,
                        "clonal_reproduction_present": merged.get("OCCPlantCount__clonal_reproduction_present"),
                        "vegetative_state_present": merged.get("OCCPlantCount__vegetative_state_present"),
                        "flower_bud_present": merged.get("OCCPlantCount__flower_bud_present"),
                        "flower_present": merged.get("OCCPlantCount__flower_present"),
                        "immature_fruit_present": merged.get("OCCPlantCount__immature_fruit_present"),
                        "ripe_fruit_present": merged.get("OCCPlantCount__ripe_fruit_present"),
                        "dehisced_fruit_present": merged.get("OCCPlantCount__dehisced_fruit_present"),
                        "detailed_alive_mature": merged.get("OCCPlantCount__detailed_alive_mature"),
                        "detailed_dead_mature": merged.get("OCCPlantCount__detailed_dead_mature"),
                        "detailed_alive_juvenile": merged.get("OCCPlantCount__detailed_alive_juvenile"),
                        "detailed_dead_juvenile": merged.get("OCCPlantCount__detailed_dead_juvenile"),
                        "detailed_alive_seedling": merged.get("OCCPlantCount__detailed_alive_seedling"),
                        "detailed_dead_seedling": merged.get("OCCPlantCount__detailed_dead_seedling"),
                        "simple_alive": merged.get("OCCPlantCount__simple_alive"),
                        "simple_dead": merged.get("OCCPlantCount__simple_dead"),
                        "quadrats_surveyed": merged.get("OCCPlantCount__quadrats_surveyed"),
                        "estimated_population_area": merged.get("OCCPlantCount__estimated_population_area"),
                        "flowering_plants_per": merged.get("OCCPlantCount__flowering_plants_per"),
                        "total_quadrat_area": merged.get("OCCPlantCount__total_quadrat_area"),
                        "comment": pc_comment,
                        "obs_date": merged.get("OCCPlantCount__obs_date"),
                    }
                    apply_model_defaults(OCCPlantCount, defaults)
                    if occ.pk in existing_plant:
                        obj = existing_plant[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        plant_update.append(obj)
                    else:
                        plant_create.append(OCCPlantCount(occurrence=occ, **defaults))

            # Execution
            if loc_create:
                OCCLocation.objects.bulk_create(loc_create, batch_size=BATCH)
            if loc_update:
                OCCLocation.objects.bulk_update(
                    loc_update,
                    [
                        "coordinate_source_id",
                        "boundary_description",
                        "locality",
                        "location_description",
                        "district_id",
                        "region_id",
                        "location_accuracy_id",
                    ],
                    batch_size=BATCH,
                )

            if obs_create:
                OCCObservationDetail.objects.bulk_create(obs_create, batch_size=BATCH)
            if obs_update:
                OCCObservationDetail.objects.bulk_update(
                    obs_update,
                    ["comments", "area_assessment_id", "area_surveyed", "survey_duration"],
                    batch_size=BATCH,
                )

            if hab_create:
                OCCHabitatComposition.objects.bulk_create(hab_create, batch_size=BATCH)
            if hab_update:
                OCCHabitatComposition.objects.bulk_update(
                    hab_update,
                    [
                        "water_quality",
                        "habitat_notes",
                        "drainage_id",
                        "land_form",
                        "loose_rock_percent",
                        "rock_type_id",
                        "soil_colour_id",
                        "soil_condition_id",
                        "soil_type",
                    ],
                    batch_size=BATCH,
                )

            if fire_create:
                OCCFireHistory.objects.bulk_create(fire_create, batch_size=BATCH)
            if fire_update:
                OCCFireHistory.objects.bulk_update(fire_update, ["comment", "intensity_id"], batch_size=BATCH)

            if assoc_create:
                OCCAssociatedSpecies.objects.bulk_create(assoc_create, batch_size=BATCH)
            if assoc_update:
                OCCAssociatedSpecies.objects.bulk_update(assoc_update, ["comment"], batch_size=BATCH)

            if doc_create:
                OccurrenceDocument.objects.bulk_create(doc_create, batch_size=BATCH)
            if doc_update:
                OccurrenceDocument.objects.bulk_update(
                    doc_update,
                    [
                        "document_sub_category_id",
                        "description",
                        "document_category",
                        "uploaded_by",
                    ],
                    batch_size=BATCH,
                )

            if geo_create:
                OccurrenceGeometry.objects.bulk_create(geo_create, batch_size=BATCH)
            if geo_update:
                OccurrenceGeometry.objects.bulk_update(
                    geo_update, ["geometry", "locked", "content_type", "original_geometry_ewkb"], batch_size=BATCH
                )

            # GIS district assignment: after geometries are persisted, intersect each
            # touched geometry against the pre-loaded district polygons and bulk-update
            # OCCLocation.district_id / region_id.  One bulk DB query fetches all
            # affected locations; no individual per-occurrence queries needed.
            #
            # For runs that produce no geometry (e.g. TEC-only, no TEC_BOUNDARIES),
            # fall back to querying existing OccurrenceGeometry for occurrences that
            # had their OCCLocation created/updated in this chunk.
            if district_geo_lookup and (geo_create or geo_update or loc_create or loc_update):
                from shapely.wkt import loads as shapely_wkt_loads

                geo_touched = {g.occurrence_id: g for g in geo_create + geo_update}

                # If this run didn't touch any geometries, query existing ones for
                # occurrences whose OCCLocation was just created/updated.
                if not geo_touched and (loc_create or loc_update):
                    loc_occ_ids = [loc.occurrence_id for loc in loc_create + loc_update]
                    for g in OccurrenceGeometry.objects.filter(occurrence_id__in=loc_occ_ids):
                        geo_touched[g.occurrence_id] = g

                # Compute district assignments in-memory first
                gis_district_assignments: dict[int, tuple[int, int | None]] = {}
                for occ_id, geo_obj in geo_touched.items():
                    geom = getattr(geo_obj, "geometry", None)
                    if not geom:
                        continue
                    try:
                        shp = shapely_wkt_loads(geom.wkt)
                        if not shp.is_valid:
                            shp = shp.buffer(0)
                    except Exception:
                        continue
                    dist_pk, region_pk = _find_primary_district(shp, district_geo_lookup)
                    if dist_pk is not None:
                        gis_district_assignments[occ_id] = (dist_pk, region_pk)

                if gis_district_assignments:
                    # Bulk-fetch all relevant OCCLocations in one query
                    affected_ids = list(gis_district_assignments.keys())
                    loc_by_occ = {
                        loc.occurrence_id: loc for loc in OCCLocation.objects.filter(occurrence_id__in=affected_ids)
                    }
                    loc_to_update = []
                    for occ_id, (dist_pk, region_pk) in gis_district_assignments.items():
                        loc = loc_by_occ.get(occ_id)
                        if loc is None:
                            continue
                        loc.district_id = dist_pk
                        loc.region_id = region_pk
                        loc_to_update.append(loc)
                    if loc_to_update:
                        OCCLocation.objects.bulk_update(loc_to_update, ["district_id", "region_id"], batch_size=BATCH)
                        logger.info(
                            "GIS district: updated district/region for %d OCCLocation(s) in this chunk",
                            len(loc_to_update),
                        )

            if ident_create:
                OCCIdentification.objects.bulk_create(ident_create, batch_size=BATCH)
            if ident_update:
                OCCIdentification.objects.bulk_update(
                    ident_update,
                    [
                        "identification_certainty_id",
                        "barcode_number",
                        "collector_number",
                        "identification_comment",
                        "permit_id",
                        "sample_destination_id",
                    ],
                    batch_size=BATCH,
                )

            if veg_create:
                OCCVegetationStructure.objects.bulk_create(veg_create, batch_size=BATCH)
            if veg_update:
                OCCVegetationStructure.objects.bulk_update(
                    veg_update, ["vegetation_structure_layer_one"], batch_size=BATCH
                )

            if hcond_create:
                OCCHabitatCondition.objects.bulk_create(hcond_create, batch_size=BATCH)
            if hcond_update:
                OCCHabitatCondition.objects.bulk_update(
                    hcond_update,
                    [
                        "pristine",
                        "excellent",
                        "very_good",
                        "good",
                        "degraded",
                        "completely_degraded",
                    ],
                    batch_size=BATCH,
                )

            if plant_create:
                OCCPlantCount.objects.bulk_create(plant_create, batch_size=BATCH)
            if plant_update:
                OCCPlantCount.objects.bulk_update(
                    plant_update,
                    [
                        "counted_subject_id",
                        "plant_condition_id",
                        "plant_count_method_id",
                        "count_status",
                        "clonal_reproduction_present",
                        "vegetative_state_present",
                        "flower_bud_present",
                        "flower_present",
                        "immature_fruit_present",
                        "ripe_fruit_present",
                        "dehisced_fruit_present",
                        "detailed_alive_mature",
                        "detailed_dead_mature",
                        "detailed_alive_juvenile",
                        "detailed_dead_juvenile",
                        "detailed_alive_seedling",
                        "detailed_dead_seedling",
                        "simple_alive",
                        "simple_dead",
                        "quadrats_surveyed",
                        "estimated_population_area",
                        "flowering_plants_per",
                        "total_quadrat_area",
                        "comment",
                        "obs_date",
                    ],
                    batch_size=BATCH,
                )

            # Re-fetch OCCAssociatedSpecies for current chunk to get PKs
            if assoc_create:
                existing_assoc = {
                    a.occurrence_id: a for a in OCCAssociatedSpecies.objects.filter(occurrence_id__in=chunk_occ_ids)
                }

            # --- OccurrenceSite ---
            site_create = []
            site_update = []
            existing_sites = defaultdict(dict)
            if not _is_geometry_only_run and not getattr(ctx, "wipe_targets", False):
                for s in OccurrenceSite.objects.filter(occurrence_id__in=chunk_occ_ids):
                    existing_sites[s.occurrence_id][s.site_name] = s

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                src = merged.get("_source")
                pipeline_map = pipelines_by_source.get(src, pipelines_by_source.get(None, {}))

                sites_to_process = []
                if src != Source.TPFL.value:
                    if merged.get("_nested_sites"):
                        for raw_site in merged.get("_nested_sites"):
                            mapped_site = schema.SCHEMA.map_raw_row(raw_site)
                            tcx = TransformContext(row=mapped_site, model=None, user_id=ctx.user_id)
                            transformed_site = dict(mapped_site)
                            for col, pipeline in pipeline_map.items():
                                if col.startswith("OccurrenceSite__"):
                                    raw_val = mapped_site.get(col)
                                    res = run_pipeline(pipeline, raw_val, tcx)
                                    transformed_site[col] = res.value
                            sites_to_process.append(transformed_site)
                    elif any(k.startswith("OccurrenceSite__") for k in merged):
                        # Only create a site from the main row when it carries
                        # meaningful site-identifying data (name or coordinates).
                        # Prevents phantom null-site creation when sources like
                        # TEC_BOUNDARIES are run separately: they populate
                        # OccurrenceSite__ pipeline keys but leave all values None.
                        has_name = bool(merged.get("OccurrenceSite__site_name"))
                        has_coords = bool(merged.get("OccurrenceSite__latitude")) and bool(
                            merged.get("OccurrenceSite__longitude")
                        )
                        has_geo = bool(merged.get("OccurrenceSite__geometry"))
                        if has_name or has_coords or has_geo:
                            sites_to_process.append(merged)

                for mapped_site in sites_to_process:
                    site_name = mapped_site.get("OccurrenceSite__site_name")
                    defaults = {
                        "comments": mapped_site.get("OccurrenceSite__comments"),
                        "geometry": mapped_site.get("OccurrenceSite__geometry")
                        or tec_site_geometry_transform(mapped_site, None),
                        "updated_date": mapped_site.get("OccurrenceSite__updated_date")
                        or datetime(1900, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                        "drawn_by": mapped_site.get("OccurrenceSite__drawn_by"),
                        "last_updated_by": mapped_site.get("OccurrenceSite__drawn_by"),
                    }

                    if site_name in existing_sites[occ.pk]:
                        s = existing_sites[occ.pk][site_name]
                        for k, v in defaults.items():
                            if k != "updated_date":
                                setattr(s, k, v)
                        s.updated_date = defaults["updated_date"]
                        site_update.append(s)
                    else:
                        s = OccurrenceSite(
                            occurrence=occ,
                            site_name=site_name,
                            comments=defaults["comments"],
                            geometry=defaults["geometry"],
                            drawn_by=defaults["drawn_by"],
                            last_updated_by=defaults["last_updated_by"],
                        )
                        s.updated_date = defaults["updated_date"]
                        site_create.append(s)

            if site_create:
                created_sites = OccurrenceSite.objects.bulk_create(site_create, batch_size=BATCH)
                # bulk_create bypasses save(), so site_number is never set.
                # Assign it now using the same "ST{pk}" pattern as the model's save().
                to_number = [s for s in created_sites if s.pk and not s.site_number]
                if to_number:
                    for s in to_number:
                        s.site_number = f"ST{s.pk}"
                    OccurrenceSite.objects.bulk_update(to_number, ["site_number"], batch_size=BATCH)
            if site_update:
                OccurrenceSite.objects.bulk_update(
                    site_update,
                    [
                        "comments",
                        "geometry",
                        "updated_date",
                        "drawn_by",
                        "last_updated_by",
                    ],
                    batch_size=BATCH,
                )

            # --- Nested Documents (OccurrenceDocument) ---
            nested_doc_create = []

            # Pre-load existing documents to check for duplicates (avoid creating identical copies on re-runs)
            existing_docs_check = defaultdict(list)
            if not _is_geometry_only_run and not getattr(ctx, "wipe_targets", False):
                for d in OccurrenceDocument.objects.filter(occurrence_id__in=chunk_occ_ids):
                    existing_docs_check[d.occurrence_id].append(d)

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                nested = merged.get("_nested_additional_data")
                # Only process if we have nested data and it's from TEC (explicit check)
                if nested and merged.get("_source") == Source.TEC.value:
                    src = merged.get("_source")
                    pipeline_map = pipelines_by_source.get(src, pipelines_by_source.get(None, {}))

                    for raw_doc in nested:
                        mapped_doc = schema.SCHEMA.map_raw_row(raw_doc)
                        tcx = TransformContext(row=mapped_doc, model=None, user_id=ctx.user_id)
                        transformed_doc = dict(mapped_doc)
                        doc_has_error = False
                        doc_issues = []

                        # Apply pipelines for relevant columns
                        for col, pipeline in pipeline_map.items():
                            if col.startswith("OccurrenceDocument__"):
                                raw_val = mapped_doc.get(col)
                                res = run_pipeline(pipeline, raw_val, tcx)
                                transformed_doc[col] = res.value
                                for issue in res.issues:
                                    doc_issues.append((col, issue))
                                    level = getattr(issue, "level", "error")
                                    record = {
                                        "migrated_from_id": op["migrated_from_id"],
                                        "column": col,
                                        "level": level,
                                        "message": getattr(issue, "message", str(issue)),
                                        "raw_value": raw_val,
                                    }
                                    if level == "error":
                                        doc_has_error = True
                                        errors += 1
                                        errors_details.append(record)
                                    else:
                                        warn_count += 1
                                        warnings_details.append(record)

                        if doc_has_error:
                            continue  # Skip this document row if any error

                        sub_cat_id = transformed_doc.get("OccurrenceDocument__document_sub_category_id")
                        desc = transformed_doc.get("OccurrenceDocument__description") or ""

                        # Create if we have data and it's not a duplicate
                        if sub_cat_id is not None or desc:
                            # Check for duplicates
                            is_duplicate = False
                            if existing_docs_check.get(occ.pk):
                                for ex in existing_docs_check[occ.pk]:
                                    # Compare fields to determine if this exact document exists
                                    if (
                                        ex.document_sub_category_id == sub_cat_id
                                        and ex.description == desc
                                        and (
                                            orf_document_category
                                            and ex.document_category_id == orf_document_category.id
                                        )
                                    ):
                                        is_duplicate = True
                                        break

                            if not is_duplicate:
                                doc_uploaded = transformed_doc.get("OccurrenceDocument__uploaded_by")
                                nested_doc_create.append(
                                    OccurrenceDocument(
                                        occurrence=occ,
                                        document_category=orf_document_category,
                                        document_sub_category_id=sub_cat_id,
                                        description=desc,
                                        uploaded_by=doc_uploaded,
                                    )
                                )

            if nested_doc_create:
                try:
                    OccurrenceDocument.objects.bulk_create(nested_doc_create, batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_create nested OccurrenceDocument")

            # --- Nested Species ---
            needed_taxa = set()
            needed_roles = set()
            species_ops = []

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                nested_species = merged.get("_nested_species")
                if nested_species:
                    for sp in nested_species:
                        raw_taxon_id = sp.get("SPEC_TAXON_ID")
                        role_name = sp.get("_resolved_role")
                        voucher = sp.get("SPEC_VOUCHER_NO")
                        if raw_taxon_id:
                            try:
                                taxon_id = int(raw_taxon_id)
                                needed_taxa.add(taxon_id)
                                if role_name:
                                    needed_roles.add(role_name)
                                species_ops.append((occ.pk, taxon_id, role_name, voucher, mig))
                            except (ValueError, TypeError):
                                logger.warning(f"Skipping invalid SPEC_TAXON_ID: {raw_taxon_id}")

            if species_ops:
                tax_map = {t.taxon_name_id: t for t in Taxonomy.objects.filter(taxon_name_id__in=needed_taxa)}
                role_map = {r.name: r for r in SpeciesRole.objects.filter(name__in=needed_roles)}

                _ocr_through = OCRAssociatedSpecies.related_species.through
                _occ_through = OCCAssociatedSpecies.related_species.through

                if not ctx.dry_run:
                    # Phase 1: Global orphan sweep — delete ASTs not referenced by any OCR or OCC.
                    # This handles the --wipe-targets cascade where OCCs are deleted before this point.
                    AssociatedSpeciesTaxonomy.objects.filter(
                        ~Exists(_occ_through.objects.filter(associatedspeciestaxonomy_id=OuterRef("pk"))),
                        ~Exists(_ocr_through.objects.filter(associatedspeciestaxonomy_id=OuterRef("pk"))),
                    ).delete()

                    # Phase 2: Targeted wipe — for OCCs being re-processed without --wipe-targets,
                    # clear their existing through-table links and delete any newly-orphaned ASTs.
                    if not getattr(ctx, "wipe_targets", False) and existing_assoc:
                        _occ_assoc_ids_to_wipe = [a.id for a in existing_assoc.values()]
                        _ast_ids_to_check = set(
                            _occ_through.objects.filter(occassociatedspecies_id__in=_occ_assoc_ids_to_wipe).values_list(
                                "associatedspeciestaxonomy_id", flat=True
                            )
                        )
                        _occ_through.objects.filter(occassociatedspecies_id__in=_occ_assoc_ids_to_wipe).delete()
                        if _ast_ids_to_check:
                            AssociatedSpeciesTaxonomy.objects.filter(
                                pk__in=_ast_ids_to_check,
                            ).filter(
                                ~Exists(_occ_through.objects.filter(associatedspeciestaxonomy_id=OuterRef("pk"))),
                                ~Exists(_ocr_through.objects.filter(associatedspeciestaxonomy_id=OuterRef("pk"))),
                            ).delete()

                # Collect unique (taxonomy_id, role_id, voucher) combos and valid ops.
                needed_combos = set()
                valid_species_ops = []
                for occ_pk, taxon_id, role_name, voucher, mig in species_ops:
                    tax = tax_map.get(taxon_id)
                    if not tax:
                        errors_details.append(
                            {
                                "migrated_from_id": mig,
                                "column": "AssociatedSpecies",
                                "level": "error",
                                "message": f"Taxonomy not found for taxon_name_id {taxon_id}",
                                "raw_value": str(taxon_id),
                                "reason": "missing_taxonomy",
                            }
                        )
                        errors += 1
                        continue
                    role = role_map.get(role_name)
                    role_id = role.id if role else None
                    voucher_normalized = str(voucher).strip() if voucher else ""
                    needed_combos.add((tax.id, role_id, voucher_normalized))
                    valid_species_ops.append((occ_pk, tax.id, role_id, voucher_normalized))

                # Always create fresh AssociatedSpeciesTaxonomy rows — never reuse existing ones.
                # AST rows contain parent-specific data (role, voucher/comments) so sharing them
                # across different parent records causes cross-contamination.
                ast_lookup = {}  # (taxonomy_id, role_id, voucher) -> AST instance
                if not ctx.dry_run and needed_combos:
                    create_objs = [
                        AssociatedSpeciesTaxonomy(
                            taxonomy_id=tid,
                            species_role_id=rid,
                            comments=voucher,
                        )
                        for tid, rid, voucher in needed_combos
                    ]
                    try:
                        created_asts = AssociatedSpeciesTaxonomy.objects.bulk_create(create_objs, batch_size=BATCH)
                        # Django 4.1+ on Postgres sets PKs on returned instances directly.
                        for ast in created_asts:
                            if ast.pk:
                                key = (ast.taxonomy_id, ast.species_role_id, ast.comments or "")
                                if key not in ast_lookup:
                                    ast_lookup[key] = ast
                        # Fallback: fetch back any that didn't get their PK set.
                        unfetched = needed_combos - {
                            (ast.taxonomy_id, ast.species_role_id, ast.comments or "") for ast in ast_lookup.values()
                        }
                        if unfetched:
                            tax_ids_needed = {tid for tid, rid, v in unfetched}
                            for ast in AssociatedSpeciesTaxonomy.objects.filter(
                                taxonomy_id__in=list(tax_ids_needed)
                            ).order_by("-pk"):
                                key = (ast.taxonomy_id, ast.species_role_id, ast.comments or "")
                                if key in unfetched and key not in ast_lookup:
                                    ast_lookup[key] = ast
                    except Exception:
                        logger.exception("Bulk create failed for AssociatedSpeciesTaxonomy; trying individual creates")
                        for tid, rid, voucher in needed_combos:
                            try:
                                ast = AssociatedSpeciesTaxonomy.objects.create(
                                    taxonomy_id=tid,
                                    species_role_id=rid,
                                    comments=voucher,
                                )
                                ast_lookup[(ast.taxonomy_id, ast.species_role_id, ast.comments or "")] = ast
                            except Exception:
                                logger.exception(
                                    "Failed to create AssociatedSpeciesTaxonomy for "
                                    "(taxonomy_id=%s, role=%s, voucher=%s)",
                                    tid,
                                    rid,
                                    voucher,
                                )

                occ_assoc_ids = [a.id for a in existing_assoc.values()]
                if occ_assoc_ids:
                    through_model = OCCAssociatedSpecies.related_species.through
                    through_objs = []
                    seen_links = set()
                    for occ_pk, tax_id, role_id, voucher_normalized in valid_species_ops:
                        key = (tax_id, role_id, voucher_normalized)
                        ast = ast_lookup.get(key)
                        if not ast:
                            continue
                        occ_assoc = existing_assoc.get(occ_pk)
                        if occ_assoc:
                            link = (occ_assoc.id, ast.id)
                            if link not in seen_links:
                                through_objs.append(
                                    through_model(
                                        occassociatedspecies_id=occ_assoc.id,
                                        associatedspeciestaxonomy_id=ast.id,
                                    )
                                )
                                seen_links.add(link)

                    if through_objs:
                        through_model.objects.bulk_create(through_objs, batch_size=BATCH)

        # Update stats counts for created/updated based on performed ops
        created += len(created_map)
        updated += len(to_update)

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        # Attach lightweight error/warning details and write CSV if needed
        stats["error_count_details"] = len(errors_details)
        stats["warning_count_details"] = len(warnings_details)
        stats["error_details_csv"] = None

        # Merge extraction warnings and per-column transform warnings into errors_details
        # so they appear in the CSV rather than in the stats object.
        for w_msg in warnings:
            source_ref, msg_body = w_msg.split(": ", 1) if ": " in w_msg else ("", w_msg)
            errors_details.append(
                {
                    "migrated_from_id": "",
                    "column": "",
                    "level": "warning",
                    "message": msg_body,
                    "raw_value": "",
                    "reason": source_ref,
                }
            )
        errors_details.extend(warnings_details)

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)

        if errors_details:
            csv_path = options.get("error_csv")
            if csv_path:
                csv_path = os.path.abspath(csv_path)
            else:
                ts = timezone.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(
                    os.getcwd(),
                    "private-media/handler_output",
                    f"{self.slug}_errors_{ts}.csv",
                )
            try:
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                with open(csv_path, "w", newline="", encoding="utf-8") as fh:
                    fieldnames = [
                        "migrated_from_id",
                        "column",
                        "level",
                        "message",
                        "raw_value",
                        "reason",
                        "row_json",
                        "timestamp",
                    ]
                    import csv

                    writer = csv.DictWriter(fh, fieldnames=fieldnames)
                    writer.writeheader()
                    for rec in errors_details:
                        writer.writerow(
                            {
                                "migrated_from_id": rec.get("migrated_from_id"),
                                "column": rec.get("column"),
                                "level": rec.get("level"),
                                "message": rec.get("message"),
                                "raw_value": rec.get("raw_value"),
                                "reason": rec.get("reason"),
                                "row_json": json.dumps(rec.get("row", ""), default=str),
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                stats["error_details_csv"] = csv_path
                logger.info(
                    (
                        "OccurrenceImporter %s finished; processed=%d created=%d "
                        "updated=%d skipped=%d errors=%d warnings=%d time_taken=%s (details -> %s)"
                    ),
                    self.slug,
                    processed,
                    created,
                    updated,
                    skipped,
                    errors,
                    warn_count,
                    str(elapsed),
                    csv_path,
                )
            except Exception as e:
                logger.error("Failed to write error CSV for %s at %s: %s", self.slug, csv_path, e)
                logger.info(
                    (
                        "OccurrenceImporter %s finished; processed=%d created=%d "
                        "updated=%d skipped=%d errors=%d warnings=%d time_taken=%s"
                    ),
                    self.slug,
                    processed,
                    created,
                    updated,
                    skipped,
                    errors,
                    warn_count,
                    str(elapsed),
                )

        return stats
