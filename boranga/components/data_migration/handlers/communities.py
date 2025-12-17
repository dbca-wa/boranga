from __future__ import annotations

import logging

from django.utils import timezone

from boranga.components.data_migration.adapters.communities import schema
from boranga.components.data_migration.adapters.communities.tpfl import (
    CommunityTpflAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    register,
    run_pipeline,
)
from boranga.components.species_and_communities.models import (
    Community,
    CommunityDistribution,
    CommunityTaxonomy,
    GroupType,
)

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TPFL.value: CommunityTpflAdapter(),
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

        to_create = []
        to_update = []
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
                    res = run_pipeline(val, pipeline, ctx)
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

            if migrated_id in existing_communities:
                obj = existing_communities[migrated_id]
                changed = False
                for k, v in defaults.items():
                    if getattr(obj, k) != v:
                        setattr(obj, k, v)
                        changed = True
                if changed:
                    to_update.append(obj)
            else:
                obj = Community(migrated_from_id=migrated_id, **defaults)
                to_create.append(obj)

        # 4. Bulk operations
        if not ctx.dry_run:
            if to_create:
                logger.info(f"Creating {len(to_create)} new communities...")
                Community.objects.bulk_create(to_create, batch_size=1000)

            if to_update:
                logger.info(f"Updating {len(to_update)} existing communities...")
                Community.objects.bulk_update(to_update, update_fields, batch_size=1000)

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

        # 5. Reporting
        stats["processed"] = processed
        stats["created"] = len(to_create)
        stats["updated"] = len(to_update)
        stats["errors"] = errors

        logger.info(
            "CommunityImporter finished: %d processed, %d created, %d updated, %d errors",
            processed,
            len(to_create),
            len(to_update),
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
