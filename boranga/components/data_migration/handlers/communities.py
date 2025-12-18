from __future__ import annotations

import logging

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

        logger.warning("CommunityImporter: deleting Community and related data...")

        # Perform deletes in an autocommit block so they are committed immediately.
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)
        try:
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
                except Exception:
                    logger.exception(
                        "Failed to truncate tables with CASCADE; falling back to DELETE"
                    )
                    Community.objects.all().delete()
            else:
                Community.objects.all().delete()

            # Reset PK sequence
            if vendor == "postgresql":
                try:
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

                    # Check if publication data exists
                    pub_title = canonical.get("pub_title")
                    pub_author = canonical.get("pub_author")
                    pub_date = canonical.get("pub_date")
                    pub_place = canonical.get("pub_place")

                    # If all are empty, skip
                    if not any([pub_title, pub_author, pub_date, pub_place]):
                        continue

                    # Construct description
                    parts = [
                        str(p)
                        for p in [pub_title, pub_author, pub_date, pub_place]
                        if p
                    ]
                    description = " ".join(parts)

                    doc = CommunityDocument(
                        community=community,
                        document_category=doc_category,
                        active=True,
                        description=description,
                        input_name="community_doc",
                        _file="None",
                        name=pub_title or "Legacy Publication",
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

            # Fetch ThreatCategory and CurrentImpact
            # We'll cache ThreatCategory by name for lookup
            threat_categories = {
                tc.name.lower(): tc for tc in ThreatCategory.objects.all()
            }

            # Fetch "Unknown" CurrentImpact
            try:
                unknown_impact = CurrentImpact.objects.get(name="Unknown")
            except CurrentImpact.DoesNotExist:
                logger.warning("CurrentImpact 'Unknown' not found. Creating it.")
                unknown_impact = CurrentImpact.objects.create(name="Unknown")

            threats_to_create = []
            for canonical in valid_rows:
                migrated_id = canonical.get("migrated_from_id")
                community = all_communities.get(migrated_id)
                if not community:
                    continue

                threat_code = canonical.get("threat_category")
                if not threat_code:
                    continue

                # Try to find threat category
                # The user says "List value Match (S&C) and Program transformation of matched values (OIM)"
                # Assuming threat_code might match name or we need a mapping.
                # For now, try exact match (case-insensitive)
                threat_cat = threat_categories.get(str(threat_code).lower())
                if not threat_cat:
                    # Log warning and skip or create?
                    # User says "This is a closed list... values are to equal or be matched"
                    # If not found, we can't link it.
                    logger.warning(
                        f"ThreatCategory '{threat_code}' not found for community {migrated_id}"
                    )
                    continue

                threat = ConservationThreat(
                    community=community,
                    threat_category=threat_cat,
                    current_impact=unknown_impact,
                    comment=canonical.get("threat_comment"),
                    date_observed=canonical.get("date_observed"),
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

            out_file = f"community_migration_errors_{timezone.now().strftime('%Y%m%d_%H%M%S')}.csv"
            with open(out_file, "w", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=["source", "row_index", "migrated_from_id", "errors"]
                )
                writer.writeheader()
                writer.writerows(errors_details)
            logger.warning(f"Errors written to {out_file}")
