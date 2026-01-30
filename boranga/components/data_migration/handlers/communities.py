from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

from django.utils import timezone

from boranga.components.data_migration.adapters.communities import schema
from boranga.components.data_migration.adapters.communities.tec import (
    CommunityTecAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.mappings import load_legacy_to_pk_map
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    register,
    run_pipeline,
)
from boranga.components.species_and_communities.models import (
    Community,
    CommunityDistribution,
    CommunityDocument,
    CommunityPublishingStatus,
    CommunityTaxonomy,
    ConservationThreat,
    CurrentImpact,
    DocumentCategory,
    GroupType,
    ThreatCategory,
)

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TEC.value: CommunityTecAdapter(),
}


def _load_publications_data(path: str) -> dict:
    """
    Load PUBLICATIONS.csv and return a dict: PUB_NO -> publication dict.
    """
    publications = {}
    pub_path = Path(path).parent / "PUBLICATIONS.csv"

    if not pub_path.exists():
        logger.warning(f"PUBLICATIONS.csv not found at {pub_path}")
        return publications

    try:
        with open(pub_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pub_no = row.get("PUB_NO", "").strip()
                if pub_no:
                    publications[pub_no] = row
    except Exception as e:
        logger.error(f"Error loading PUBLICATIONS.csv: {e}")

    logger.debug(f"Loaded {len(publications)} publications")
    return publications


def _load_community_publications_map(path: str) -> dict:
    """
    Load COMMUNITY_PUBLICATIONS.csv and return a dict: COM_NO -> list of PUB_NOs.
    """
    com_pub_map = {}
    comm_pub_path = Path(path).parent / "COMMUNITY_PUBLICATIONS.csv"

    if not comm_pub_path.exists():
        logger.warning(f"COMMUNITY_PUBLICATIONS.csv not found at {comm_pub_path}")
        return com_pub_map

    try:
        with open(comm_pub_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                com_no = row.get("COM_NO", "").strip()
                cp_pub_no = row.get("CP_PUB_NO", "").strip()
                if com_no and cp_pub_no:
                    if com_no not in com_pub_map:
                        com_pub_map[com_no] = []
                    com_pub_map[com_no].append(cp_pub_no)
    except Exception as e:
        logger.error(f"Error loading COMMUNITY_PUBLICATIONS.csv: {e}")

    logger.debug(
        f"Loaded community-publication mappings for {len(com_pub_map)} communities"
    )
    return com_pub_map


def _load_community_threats(path: str) -> dict:
    """
    Load COMMUNITY_THREATS.csv and return a dict: COM_NO -> list of threat dicts.
    """
    threats = {}
    threats_path = Path(path).parent / "COMMUNITY_THREATS.csv"

    if not threats_path.exists():
        logger.warning(f"COMMUNITY_THREATS.csv not found at {threats_path}")
        return threats

    try:
        with open(threats_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                com_no = row.get("COM_NO", "").strip()
                if com_no:
                    if com_no not in threats:
                        threats[com_no] = []
                    threats[com_no].append(row)
    except Exception as e:
        logger.error(f"Error loading COMMUNITY_THREATS.csv: {e}")

    logger.debug(f"Loaded threats for {len(threats)} communities")
    return threats


@register
class CommunityImporter(BaseSheetImporter):
    slug = "communities_legacy"
    description = "Import communities data from legacy TPFL sources"

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete community target data. Respects `ctx.dry_run` (no-op when True)."""
        if ctx.dry_run:
            logger.info("CommunityImporter.clear_targets: dry-run, skipping delete")
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

        is_filtered = bool(sources)

        if is_filtered:
            if not target_group_types:
                logger.warning(
                    "clear_targets: sources %s provided but no associated group_types found in map. Skipping delete.",
                    sources,
                )
                return
            logger.warning(
                "CommunityImporter: deleting Community and related data for group_types: %s ...",
                target_group_types,
            )
            comm_filter = {"group_type__name__in": target_group_types}
            occ_filter = {"group_type__name__in": target_group_types}
            report_filter = {"group_type__name__in": target_group_types}
        else:
            logger.warning(
                "CommunityImporter: deleting ALL Community and related data..."
            )
            comm_filter = {}
            occ_filter = {}
            report_filter = {}

        # Perform deletes in an autocommit block so they are committed immediately.
        from django.db import connections

        from boranga.components.occurrence.models import Occurrence, OccurrenceReport
        from boranga.components.species_and_communities.models import Community

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)
        try:
            if not is_filtered:
                vendor = getattr(conn, "vendor", None)
                if vendor == "postgresql":
                    logger.info(
                        "CommunityImporter: using TRUNCATE CASCADE for efficient bulk deletion"
                    )
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"TRUNCATE TABLE {Community._meta.db_table} CASCADE"
                            )
                            logger.info(
                                "CommunityImporter: successfully truncated %s with CASCADE",
                                Community._meta.db_table,
                            )
                        # Also truncate dependent tables
                        try:
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"TRUNCATE TABLE {OccurrenceReport._meta.db_table} CASCADE"
                                )
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"TRUNCATE TABLE {Occurrence._meta.db_table} CASCADE"
                                )
                        except Exception:
                            logger.warning(
                                "Failed to truncate dependent tables, skipping"
                            )

                    except Exception:
                        logger.exception(
                            "Failed to truncate tables with CASCADE; falling back to DELETE"
                        )

                        OccurrenceReport.objects.all().delete()
                        Occurrence.objects.all().update(combined_occurrence=None)
                        Occurrence.objects.all().delete()
                        Community.objects.all().delete()
                else:
                    # Non-Postgres
                    OccurrenceReport.objects.all().delete()
                    Occurrence.objects.all().update(combined_occurrence=None)
                    Occurrence.objects.all().delete()
                    Community.objects.all().delete()
            else:
                # Filtered delete
                try:
                    OccurrenceReport.objects.filter(**report_filter).delete()
                    Occurrence.objects.filter(**occ_filter).update(
                        combined_occurrence=None
                    )
                    Occurrence.objects.filter(**occ_filter).delete()
                    Community.objects.filter(**comm_filter).delete()
                except Exception:
                    logger.exception(
                        "CommunityImporter: Failed to delete filtered target data"
                    )

            # Reset PK sequence
            if not is_filtered:
                try:
                    if getattr(conn, "vendor", None) == "postgresql":
                        table = Community._meta.db_table
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                                [table, "id", 1, False],
                            )
                        logger.info("Reset primary key sequence for table %s", table)
                except Exception:
                    logger.exception("Failed to reset Community primary key sequence")

        finally:
            if not was_autocommit:
                conn.set_autocommit(False)

    def add_arguments(self, parser):
        parser.add_argument(
            "--sources",
            nargs="+",
            choices=list(SOURCE_ADAPTERS.keys()),
            help="Subset of sources (default: all implemented)",
        )
        parser.add_argument(
            "--path-map",
            nargs="+",
            metavar="SRC=PATH",
            help="Per-source path overrides (e.g. TPFL=/tmp/tpfl.xlsx). If omitted, --path is reused.",
        )

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
            "CommunityImporter (%s) started at %s (dry_run=%s)",
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

        # 1. Extract from each source
        for src in sources:
            adapter = SOURCE_ADAPTERS[src]
            src_path = path_map.get(src, path)
            result = adapter.extract(src_path, **options)
            for w in result.warnings:
                warnings.append(f"{src}: {w.message}")
            for r in result.rows:
                r["_source"] = src
            all_rows.extend(result.rows)

        # 1b. Load related data (publications, threats) from separate CSV files
        logger.info("Loading related publication and threat data...")
        base_path = path_map.get("TEC", path)  # Use TEC path as base
        publications_data = _load_publications_data(base_path)
        community_pub_map = _load_community_publications_map(base_path)
        community_threats_map = _load_community_threats(base_path)
        logger.info(
            f"Loaded: {len(publications_data)} publications, "
            f"{len(community_pub_map)} communities with pubs, "
            f"{len(community_threats_map)} communities with threats"
        )

        # Apply limit
        limit = getattr(ctx, "limit", None)
        if limit:
            try:
                all_rows = all_rows[: int(limit)]
            except Exception:
                pass

        # 2. Build pipelines
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

        # 3. Transform rows
        processed = 0
        errors = 0

        # Pre-fetch static values
        try:
            community_group_type = GroupType.objects.get(name="community")
        except GroupType.DoesNotExist:
            logger.error("GroupType 'community' not found. Aborting migration.")
            return

        # Load existing communities for update
        existing_communities = {
            c.migrated_from_id: c
            for c in Community.objects.filter(migrated_from_id__isnull=False)
        }

        communities_cache = existing_communities.copy()
        to_create = []
        dirty_communities = set()
        valid_rows = []  # Store canonical rows for subsequent processing

        # Fields to update if record exists
        update_fields = [
            "group_type",
            "comment",
            "lodgement_date",
            "processing_status",
        ]

        for row in all_rows:
            processed += 1
            src = row["_source"]
            pipeline_map = pipelines_by_source.get(src, {})

            canonical = {}
            row_errors = []

            # Apply pipelines
            for col, val in row.items():
                if col.startswith("_"):
                    continue

                pipeline = pipeline_map.get(col)
                if pipeline:
                    res = run_pipeline(pipeline, val, ctx)
                    if res.errors:
                        for e in res.errors:
                            row_errors.append(f"{col}: {e.message}")
                    canonical[col] = res.value
                else:
                    canonical[col] = val

            # Prepend source prefix to migrated_from_id if present
            if src and canonical.get("migrated_from_id"):
                prefix = src.lower().replace("_", "-")
                if not str(canonical["migrated_from_id"]).startswith(f"{prefix}-"):
                    canonical["migrated_from_id"] = (
                        f"{prefix}-{canonical['migrated_from_id']}"
                    )

            if row_errors:
                errors += 1
                errors_details.append(
                    {
                        "source": src,
                        "row_index": processed,
                        "migrated_from_id": canonical.get("migrated_from_id"),
                        "errors": "; ".join(row_errors),
                    }
                )
                continue

            migrated_id = canonical.get("migrated_from_id")
            if not migrated_id:
                # Should be caught by 'required' pipeline, but just in case
                continue

            valid_rows.append(canonical)

            # Prepare object data
            defaults = {
                "group_type": community_group_type,
                "comment": canonical.get("comment"),
                "lodgement_date": timezone.now(),
                "processing_status": "active",
            }

            if migrated_id in communities_cache:
                obj = communities_cache[migrated_id]
                changed = False
                for k, v in defaults.items():
                    if getattr(obj, k) != v:
                        setattr(obj, k, v)
                        changed = True
                if changed:
                    dirty_communities.add(obj)
            else:
                obj = Community(migrated_from_id=migrated_id, **defaults)
                to_create.append(obj)
                communities_cache[migrated_id] = obj

        # 4. Bulk operations
        if not ctx.dry_run:
            if to_create:
                logger.info(f"Creating {len(to_create)} new communities...")
                Community.objects.bulk_create(to_create, batch_size=1000)

                # Fetch newly created communities to set community_number
                # bulk_create bypasses model .save() so logic generating `community_number`
                # from PK won't have run.
                new_migrated_ids = [c.migrated_from_id for c in to_create]
                new_communities = Community.objects.filter(
                    migrated_from_id__in=new_migrated_ids
                )

                communities_to_update_number = []
                for c in new_communities:
                    if not c.community_number:
                        c.community_number = f"C{str(c.pk)}"
                        communities_to_update_number.append(c)

                if communities_to_update_number:
                    logger.info(
                        f"Updating community_number for {len(communities_to_update_number)} new communities..."
                    )
                    Community.objects.bulk_update(
                        communities_to_update_number,
                        ["community_number"],
                        batch_size=1000,
                    )

            if dirty_communities:
                logger.info(
                    f"Updating {len(dirty_communities)} existing communities..."
                )
                Community.objects.bulk_update(
                    list(dirty_communities), update_fields, batch_size=1000
                )

            # 4b. Create/Update CommunityTaxonomy
            logger.info("Updating CommunityTaxonomy records...")
            # Reload communities to get IDs for newly created ones
            all_communities = {
                c.migrated_from_id: c
                for c in Community.objects.filter(migrated_from_id__isnull=False)
            }

            # Load existing taxonomies
            existing_taxonomies = {
                t.community_id: t
                for t in CommunityTaxonomy.objects.filter(
                    community__in=all_communities.values()
                )
            }

            taxonomy_to_create = []
            taxonomy_to_update = []
            taxonomy_update_fields = [
                "community_common_id",
                "community_name",
                "community_description",
            ]

            # Track unique names we've seen to handle duplicates
            seen_names = set()
            for existing_tax in existing_taxonomies.values():
                if existing_tax.community_name:
                    seen_names.add(existing_tax.community_name.lower())

            for canonical in valid_rows:
                migrated_id = canonical.get("migrated_from_id")
                community = all_communities.get(migrated_id)
                if not community:
                    continue

                tax_defaults = {
                    "community_common_id": canonical.get("community_common_id"),
                    "community_name": canonical.get("community_name"),
                    "community_description": canonical.get("community_description"),
                }

                # Check for duplicate community names
                original_name = tax_defaults.get("community_name")
                if original_name:
                    name_lower = original_name.lower()
                    if name_lower in seen_names:
                        # Duplicate found - log as error and skip
                        error_msg = f"Duplicate community_name '{original_name}'; skipping taxonomy creation"
                        logger.error(f"{error_msg} for migrated_from_id={migrated_id}")
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": migrated_id,
                                "column": "community_name",
                                "level": "ERROR",
                                "message": error_msg,
                                "raw_value": original_name,
                                "reason": "Unique constraint violation",
                                "row": canonical,
                            }
                        )
                        continue
                    seen_names.add(name_lower)

                if community.id in existing_taxonomies:
                    tax_obj = existing_taxonomies[community.id]
                    changed = False
                    for k, v in tax_defaults.items():
                        if getattr(tax_obj, k) != v:
                            setattr(tax_obj, k, v)
                            changed = True
                    if changed:
                        taxonomy_to_update.append(tax_obj)
                else:
                    tax_obj = CommunityTaxonomy(community=community, **tax_defaults)
                    taxonomy_to_create.append(tax_obj)

            if taxonomy_to_create:
                logger.info(
                    f"Creating {len(taxonomy_to_create)} new CommunityTaxonomy records..."
                )
                CommunityTaxonomy.objects.bulk_create(
                    taxonomy_to_create, batch_size=1000
                )

            if taxonomy_to_update:
                logger.info(
                    f"Updating {len(taxonomy_to_update)} existing CommunityTaxonomy records..."
                )
                CommunityTaxonomy.objects.bulk_update(
                    taxonomy_to_update, taxonomy_update_fields, batch_size=1000
                )

            # 4c. Create/Update CommunityDistribution
            logger.info("Updating CommunityDistribution records...")

            # Load existing distributions
            existing_distributions = {
                d.community_id: d
                for d in CommunityDistribution.objects.filter(
                    community__in=all_communities.values()
                )
            }

            dist_to_create = []
            dist_to_update = []
            dist_update_fields = [
                "community_original_area",
                "community_original_area_accuracy",
                "community_original_area_reference",
                "distribution",
                "aoo_actual_auto",
                "eoo_auto",
                "noo_auto",
            ]

            for canonical in valid_rows:
                migrated_id = canonical.get("migrated_from_id")
                community = all_communities.get(migrated_id)
                if not community:
                    continue

                # Calculate conditional reference
                area_acc = canonical.get("community_original_area_accuracy")
                ref_val = None
                if area_acc is not None and area_acc >= 0:
                    ref_val = "TEC Database"

                dist_defaults = {
                    "community_original_area": canonical.get("community_original_area"),
                    "community_original_area_accuracy": area_acc,
                    "community_original_area_reference": ref_val,
                    "distribution": canonical.get("distribution"),
                    "aoo_actual_auto": True,
                    "eoo_auto": True,
                    "noo_auto": True,
                }

                if community.id in existing_distributions:
                    dist_obj = existing_distributions[community.id]
                    changed = False
                    for k, v in dist_defaults.items():
                        if getattr(dist_obj, k) != v:
                            setattr(dist_obj, k, v)
                            changed = True
                    if changed:
                        dist_to_update.append(dist_obj)
                else:
                    dist_obj = CommunityDistribution(
                        community=community, **dist_defaults
                    )
                    dist_to_create.append(dist_obj)

            if dist_to_create:
                logger.info(
                    f"Creating {len(dist_to_create)} new CommunityDistribution records..."
                )
                CommunityDistribution.objects.bulk_create(
                    dist_to_create, batch_size=1000
                )

            if dist_to_update:
                logger.info(
                    f"Updating {len(dist_to_update)} existing CommunityDistribution records..."
                )
                CommunityDistribution.objects.bulk_update(
                    dist_to_update, dist_update_fields, batch_size=1000
                )

            # 4d. Create/Update CommunityPublishingStatus
            logger.info("Updating CommunityPublishingStatus records...")

            existing_publishing_statuses = {
                ps.community_id: ps
                for ps in CommunityPublishingStatus.objects.filter(
                    community__in=all_communities.values()
                )
            }

            pub_to_create = []
            pub_to_update = []
            pub_update_fields = [
                "community_public",
                "conservation_status_public",
                "distribution_public",
                "threats_public",
            ]

            for canonical in valid_rows:
                migrated_id = canonical.get("migrated_from_id")
                community = all_communities.get(migrated_id)
                if not community:
                    continue

                is_public = canonical.get("active_cs") is True

                pub_defaults = {
                    "community_public": is_public,
                    "conservation_status_public": is_public,
                    "distribution_public": is_public,
                    "threats_public": is_public,
                }

                if community.id in existing_publishing_statuses:
                    pub_obj = existing_publishing_statuses[community.id]
                    changed = False
                    for k, v in pub_defaults.items():
                        if getattr(pub_obj, k) != v:
                            setattr(pub_obj, k, v)
                            changed = True
                    if changed:
                        pub_to_update.append(pub_obj)
                else:
                    pub_obj = CommunityPublishingStatus(
                        community=community, **pub_defaults
                    )
                    pub_to_create.append(pub_obj)

            if pub_to_create:
                logger.info(
                    f"Creating {len(pub_to_create)} new CommunityPublishingStatus records..."
                )
                CommunityPublishingStatus.objects.bulk_create(
                    pub_to_create, batch_size=1000
                )

            if pub_to_update:
                logger.info(
                    f"Updating {len(pub_to_update)} existing CommunityPublishingStatus records..."
                )
                CommunityPublishingStatus.objects.bulk_update(
                    pub_to_update, pub_update_fields, batch_size=1000
                )

            # 4e. Update Regions and Districts (Many-to-Many)
            logger.info("Updating Community Regions and Districts...")

            # Load legacy mappings
            region_map = load_legacy_to_pk_map(legacy_system="TEC", model_name="Region")
            district_map = load_legacy_to_pk_map(
                legacy_system="TEC", model_name="District"
            )

            def parse_keys(raw, mapping, label, mig_id):
                if not raw:
                    return []
                # Handle list/tuple if pipeline already split it
                if isinstance(raw, (list, tuple)):
                    items = [str(x).strip() for x in raw if x]
                elif isinstance(raw, str):
                    # Split by comma
                    items = [x.strip() for x in raw.split(",") if x.strip()]
                else:
                    items = [str(raw)]

                ids = []
                for item in items:
                    pk = mapping.get(item)
                    if pk:
                        ids.append(pk)
                    else:
                        logger.warning(
                            f"{label}: '{item}' not found in legacy map for community {mig_id}"
                        )
                return ids

            for canonical in valid_rows:
                migrated_id = canonical.get("migrated_from_id")
                community = all_communities.get(migrated_id)
                if not community:
                    continue

                # Regions
                regions_raw = canonical.get("regions")
                region_ids = parse_keys(regions_raw, region_map, "Region", migrated_id)
                if region_ids:
                    community.regions.set(region_ids)

                # Districts
                districts_raw = canonical.get("districts")
                district_ids = parse_keys(
                    districts_raw, district_map, "District", migrated_id
                )
                if district_ids:
                    community.districts.set(district_ids)

            # 4e. Create CommunityDocument
            logger.info("Creating CommunityDocument records...")

            # Fetch DocumentCategory
            try:
                doc_category = DocumentCategory.objects.get(
                    document_category_name="TEC Database Publication Reference"
                )
            except DocumentCategory.DoesNotExist:
                logger.warning(
                    "DocumentCategory 'TEC Database Publication Reference' not found. Skipping document creation."
                )
                doc_category = None

            if doc_category:
                docs_to_create = []
                for canonical in valid_rows:
                    migrated_id = canonical.get("migrated_from_id")
                    community = all_communities.get(migrated_id)
                    if not community:
                        continue

                    # Check if community has publications in the mapping
                    # Maps are keyed by raw ID, so strip 'tec-' if present
                    lookup_key = migrated_id
                    if lookup_key.lower().startswith("tec-"):
                        lookup_key = lookup_key[4:]
                    pub_nos = community_pub_map.get(lookup_key, [])
                    if not pub_nos:
                        continue

                    # Load publication details for each publication number
                    for pub_no in pub_nos:
                        pub_data = publications_data.get(str(pub_no))
                        if not pub_data:
                            logger.warning(
                                f"Publication {pub_no} not found for community {migrated_id}"
                            )
                            continue

                        pub_title = (pub_data.get("PUB_TITLE") or "").strip()
                        pub_author = (pub_data.get("PUB_AUTHOR") or "").strip()
                        pub_date = (pub_data.get("PUB_DATE") or "").strip()
                        pub_place = (pub_data.get("PUB_PLACE") or "").strip()

                        # Construct description
                        parts = [
                            p for p in [pub_title, pub_author, pub_date, pub_place] if p
                        ]
                        description = " ".join(parts) if parts else pub_no

                        doc = CommunityDocument(
                            community=community,
                            document_category=doc_category,
                            active=True,
                            description=description,
                            input_name="community_doc",
                            name="",
                            uploaded_date=timezone.now(),
                        )
                        docs_to_create.append(doc)

                if docs_to_create:
                    logger.info(
                        f"Creating {len(docs_to_create)} new CommunityDocument records..."
                    )
                    CommunityDocument.objects.bulk_create(
                        docs_to_create, batch_size=1000
                    )

                    # Update document_number for created documents
                    # Since bulk_create doesn't return PKs on all DBs (but Postgres does),
                    # and we need to set document_number = D{pk}.
                    # We can iterate and save, or fetch and update.
                    # Given the volume might be high, let's try to optimize.
                    # For now, let's iterate and save those that were just created?
                    # No, we can't easily identify them without PKs if we don't have them.
                    # Postgres returns PKs if we use bulk_create(..., returning=True) (Django 4.x feature?)
                    # Boranga uses Django 3.2 or 4?
                    # Let's assume we need to fetch them.
                    # But how to identify them? We don't have a unique legacy ID for documents.
                    # We can filter by input_name="community_doc" and document_number=""

                    docs_to_update = []
                    for doc in CommunityDocument.objects.filter(
                        input_name="community_doc", document_number=""
                    ):
                        doc.document_number = f"D{doc.pk}"
                        docs_to_update.append(doc)

                    if docs_to_update:
                        CommunityDocument.objects.bulk_update(
                            docs_to_update, ["document_number"], batch_size=1000
                        )

            # 4f. Create ConservationThreat
            logger.info("Creating ConservationThreat records...")

            # Load legacy threat code mappings from LegacyValueMap
            threat_code_map = load_legacy_to_pk_map(
                legacy_system="TEC", model_name="ThreatCategory"
            )
            logger.info(f"Loaded {len(threat_code_map)} threat code mappings")

            # Fetch "Unknown" CurrentImpact
            try:
                unknown_impact = CurrentImpact.objects.get(name="Unknown")
            except CurrentImpact.DoesNotExist:
                logger.warning("CurrentImpact 'Unknown' not found. Creating it.")
                unknown_impact = CurrentImpact.objects.create(name="Unknown")

            # Load threat code to ThreatCategory ID mapping from LegacyValueMap
            threat_code_map = load_legacy_to_pk_map(
                legacy_system="TEC", model_name="ThreatCategory"
            )
            logger.info(f"Loaded {len(threat_code_map)} threat code mappings")

            threats_to_create = []
            for canonical in valid_rows:
                migrated_id = canonical.get("migrated_from_id")
                community = all_communities.get(migrated_id)
                if not community:
                    continue

                # Check if community has threats in the mapping
                # Maps are keyed by raw ID, so strip 'tec-' if present
                lookup_key = migrated_id
                if lookup_key.lower().startswith("tec-"):
                    lookup_key = lookup_key[4:]

                com_threats = community_threats_map.get(lookup_key, [])
                if not com_threats:
                    continue

                # Create a threat for each threat record
                for threat_row in com_threats:
                    threat_code = (threat_row.get("CTHR_THREAT_CODE") or "").strip()
                    if not threat_code:
                        continue

                    # Look up threat category ID from legacy mapping
                    threat_cat_id = threat_code_map.get(threat_code)
                    if not threat_cat_id:
                        error_msg = f"ThreatCategory code '{threat_code}' not found in legacy mappings"
                        logger.error(f"{error_msg} for community {migrated_id}")
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": migrated_id,
                                "column": "threat_category",
                                "level": "ERROR",
                                "message": error_msg,
                                "raw_value": threat_code,
                                "reason": "ThreatCategory code not found in LegacyValueMap",
                                "row_json": json.dumps(threat_row),
                            }
                        )
                        continue

                    # Get the ThreatCategory object
                    try:
                        threat_cat = ThreatCategory.objects.get(pk=threat_cat_id)
                    except ThreatCategory.DoesNotExist:
                        error_msg = f"ThreatCategory with ID {threat_cat_id} not found"
                        logger.error(f"{error_msg} for community {migrated_id}")
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": migrated_id,
                                "column": "threat_category",
                                "level": "ERROR",
                                "message": error_msg,
                                "raw_value": threat_code,
                                "reason": f"ThreatCategory ID {threat_cat_id} not found",
                                "row_json": json.dumps(threat_row),
                            }
                        )
                        continue

                    # Parse date observed
                    date_observed = None
                    date_str = (threat_row.get("CTHR_DATE") or "").strip()
                    if date_str:
                        try:
                            from datetime import datetime

                            # Try ISO format first
                            date_observed = datetime.fromisoformat(
                                date_str.replace("Z", "+00:00")
                            ).date()
                        except Exception:
                            logger.warning(
                                f"Could not parse date '{date_str}' for threat in community {migrated_id}"
                            )

                    threat = ConservationThreat(
                        community=community,
                        threat_category=threat_cat,
                        current_impact=unknown_impact,
                        comment=threat_row.get("CTHR_DESC"),
                        date_observed=date_observed,
                        visible=True,
                    )
                    threats_to_create.append(threat)

            if threats_to_create:
                logger.info(
                    f"Creating {len(threats_to_create)} new ConservationThreat records..."
                )
                ConservationThreat.objects.bulk_create(
                    threats_to_create, batch_size=1000
                )

                # Update threat_number
                threats_to_update = []
                # Similar strategy: fetch threats with empty threat_number
                # Note: This might pick up threats created by other means if they have empty number,
                # but in migration context it's likely fine.
                # To be safer, we could filter by community__in=all_communities.values()
                for t in ConservationThreat.objects.filter(
                    community__in=all_communities.values(), threat_number=""
                ):
                    t.threat_number = f"T{t.pk}"
                    threats_to_update.append(t)

                if threats_to_update:
                    ConservationThreat.objects.bulk_update(
                        threats_to_update, ["threat_number"], batch_size=1000
                    )

        # 5. Reporting
        stats["processed"] = processed
        stats["created"] = len(to_create)
        stats["updated"] = len(dirty_communities)
        stats["errors"] = errors

        logger.info(
            "CommunityImporter finished: %d processed, %d created, %d updated, %d errors",
            processed,
            len(to_create),
            len(dirty_communities),
            errors,
        )

        if errors_details:
            import csv
            import json
            from pathlib import Path

            from django.conf import settings

            # Write to private-media/handler_output directory
            base_dir = getattr(
                settings, "BASE_DIR", Path(__file__).resolve().parents[3]
            )
            output_dir = Path(base_dir) / "private-media" / "handler_output"
            output_dir.mkdir(parents=True, exist_ok=True)

            out_file = (
                output_dir
                / f"community_migration_errors_{timezone.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            with open(out_file, "w", newline="") as f:
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
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for rec in errors_details:
                    writer.writerow(
                        {
                            "migrated_from_id": rec.get("migrated_from_id"),
                            "column": rec.get("column", ""),
                            "level": rec.get("level", "ERROR"),
                            "message": rec.get("errors", rec.get("message", "")),
                            "raw_value": rec.get("raw_value", ""),
                            "reason": rec.get("reason", ""),
                            "row_json": json.dumps(rec.get("row", {}), default=str),
                            "timestamp": timezone.now().isoformat(),
                        }
                    )
            logger.warning(f"Errors written to {out_file}")
