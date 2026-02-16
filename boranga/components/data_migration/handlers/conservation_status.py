from __future__ import annotations

import csv
import json
import logging
import os

from django.apps import apps
from django.utils import timezone

from boranga.components.data_migration.adapters.conservation_status import schema
from boranga.components.data_migration.adapters.conservation_status.tec import (
    ConservationStatusTecAdapter,
)
from boranga.components.data_migration.adapters.conservation_status.tpfl import (
    ConservationStatusTpflAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TPFL.value: ConservationStatusTpflAdapter(),
    Source.TEC.value: ConservationStatusTecAdapter(),
}


@register
class ConservationStatusImporter(BaseSheetImporter):
    slug = "conservation_status_legacy"
    description = "Import conservation status from legacy TPFL sources"

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """Delete ConservationStatus target data. Respect `ctx.dry_run`."""
        if ctx.dry_run:
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
                return
            logger.warning(
                "ConservationStatusImporter: deleting ConservationStatus data for group_types: %s ...",
                target_group_types,
            )
            # ConservationStatus uses application_type for group_type
            cs_filter = {
                "application_type__name__in": target_group_types,
                "migrated_from_id__isnull": False,
            }
        else:
            logger.warning("ConservationStatusImporter: deleting ConservationStatus data...")
            cs_filter = {"migrated_from_id__isnull": False}

        # Delete reversion history first (ConservationStatus and ConservationStatusDocument)
        from boranga.components.conservation_status.models import ConservationStatus, ConservationStatusDocument
        from boranga.components.data_migration.utils.reversion_cleanup import ReversionHistoryCleaner

        cleaner = ReversionHistoryCleaner(batch_size=2000)
        cleaner.clear_for_model(ConservationStatus, cs_filter)
        cleaner.clear_for_related_model(ConservationStatusDocument, "conservation_status", cs_filter)
        logger.info("Reversion cleanup completed. Stats: %s", cleaner.get_stats())

        from django.apps import apps
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)

        try:
            ConservationStatus = apps.get_model("boranga", "ConservationStatus")
            # Only delete migrated records
            ConservationStatus.objects.filter(**cs_filter).delete()

            # Reset the primary key sequence for ConservationStatus when using PostgreSQL.
            try:
                if getattr(conn, "vendor", None) == "postgresql":
                    table = ConservationStatus._meta.db_table
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
                logger.exception("Failed to reset ConservationStatus primary key sequence")

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
        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        stats["extracted"] = 0
        all_rows: list[dict] = []
        warnings = []
        errors_details = []

        # 1. Extract
        for source_key in sources:
            adapter = SOURCE_ADAPTERS[source_key]
            source_path = path_map.get(source_key, path)
            logger.info(f"Extracting from {source_key} ({source_path})...")

            res = adapter.extract(source_path)
            # Tag each row with its source for pipeline selection later
            for row in res.rows:
                row["_source"] = source_key
            stats["extracted"] += len(res.rows)
            warnings.extend(res.warnings)
            all_rows.extend(res.rows)

        # 2. Build pipelines per source
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

        if ctx.dry_run:
            logger.info(f"[DRY RUN] Would import {len(all_rows)} rows.")
            return

        # 3. Load dependencies
        Species = apps.get_model("boranga", "Species")
        SubmitterInformation = apps.get_model("boranga", "SubmitterInformation")
        SubmitterCategory = apps.get_model("boranga", "SubmitterCategory")
        ConservationStatus = apps.get_model("boranga", "ConservationStatus")

        # Map Taxonomy ID -> Species PK (since pipeline returns Taxonomy PK)
        species_qs = Species.objects.filter(taxonomy__isnull=False)
        tax_to_species_pk_map = dict(species_qs.values_list("taxonomy_id", "id"))

        # Submitter Category 'DBCA'
        submitter_category_dbca = SubmitterCategory.objects.filter(name="DBCA").first()
        if not submitter_category_dbca:
            logger.warning("SubmitterCategory 'DBCA' not found. SubmitterInformation records may be incomplete.")

        # Prepare lists for bulk operations
        submitter_infos = []
        cs_objects = []

        for row in all_rows:
            # Run pipeline transformations
            pipelines = pipelines_by_source.get(row.get("_source"), [])

            tcx = TransformContext(row=row, user_id=ctx.user_id)
            has_error = False
            for col, pipeline in pipelines.items():
                raw_val = row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)
                row[col] = res.value

                if res.errors:
                    has_error = True
                    stats["errors"] += 1
                    for err in res.errors:
                        errors_details.append(
                            {
                                "migrated_from_id": row.get("migrated_from_id"),
                                "column": col,
                                "level": "error",
                                "message": err.message,
                                "raw_value": raw_val,
                                "reason": "Pipeline error",
                                "row_json": json.dumps(row, default=str),
                                "timestamp": timezone.now().isoformat(),
                            }
                        )

            if has_error:
                stats["skipped"] += 1
                continue

            # Prepend source prefix to migrated_from_id if present
            _src = row.get("_source")
            if _src and row.get("migrated_from_id"):
                prefix = _src.lower().replace("_", "-")
                if not str(row["migrated_from_id"]).startswith(f"{prefix}-"):
                    row["migrated_from_id"] = f"{prefix}-{row['migrated_from_id']}"

            try:
                # Check for required migrated_from_id
                mig_from_id = row.get("migrated_from_id")
                if not mig_from_id:
                    src_key = row.get("_source")
                    msg = f"Missing migrated_from_id for {src_key} source."
                    logger.warning(msg)
                    stats["skipped"] += 1
                    stats["errors"] += 1
                    errors_details.append(
                        {
                            "migrated_from_id": "N/A",
                            "column": "migrated_from_id",
                            "level": "error",
                            "message": msg,
                            "raw_value": "None",
                            "reason": "Missing required field",
                            "row_json": json.dumps(row, default=str),
                            "timestamp": timezone.now().isoformat(),
                        }
                    )
                    continue

                # Resolve Species PK (for TPFL)
                # row["species_id"] now holds the canonical Taxonomy PK (from SPECIES_LOOKUP pipeline)
                # We need to map this Taxonomy PK to the Species internal PK
                species_pk = None
                species_taxonomy_id = None

                if row.get("species_id"):
                    # Pipeline transformed species_id to Taxonomy PK
                    species_taxonomy_id = row["species_id"]
                    species_pk = tax_to_species_pk_map.get(species_taxonomy_id)

                # community_migrated_from_id pipeline should return Community PK or None
                community_pk = row.get("community_migrated_from_id")

                # Determine submitter for SubmitterInformation
                # Pipeline returns ID for 'submitter'
                si_email_user_id = row.get("submitter")

                # Create SubmitterInformation instance (do not save yet)
                sub_info = SubmitterInformation(
                    email_user=si_email_user_id,
                    organisation="DBCA",
                    submitter_category=submitter_category_dbca,
                )
                submitter_infos.append(sub_info)

                # Create ConservationStatus instance (do not save yet)
                cs = ConservationStatus(
                    migrated_from_id=row.get("migrated_from_id"),
                    processing_status=row.get("processing_status"),
                    customer_status=row.get("customer_status"),
                    species_id=species_pk,
                    species_taxonomy_id=species_taxonomy_id,
                    community_id=community_pk,
                    wa_priority_list=row.get("wa_priority_list"),  # Transformed to object
                    wa_priority_category=row.get("wa_priority_category"),  # Transformed to object
                    wa_legislative_list=row.get("wa_legislative_list"),  # Transformed to object
                    wa_legislative_category=row.get("wa_legislative_category"),  # Transformed to object
                    commonwealth_conservation_category_id=row.get("commonwealth_conservation_category"),
                    iucn_version_id=row.get("iucn_version"),
                    change_code_id=row.get("change_code"),
                    conservation_criteria=row.get("conservation_criteria"),
                    review_due_date=row.get("review_due_date"),
                    effective_from=row.get("effective_from_date"),
                    effective_to=row.get("effective_to_date"),
                    listing_date=row.get("listing_date"),
                    lodgement_date=row.get("lodgement_date"),
                    submitter=row.get("submitter"),  # ID
                    assigned_approver=row.get("assigned_approver"),  # ID
                    approved_by=row.get("approved_by"),  # ID
                    comment=row.get("comment"),
                    locked=row.get("locked"),
                    internal_application=row.get("internal_application"),
                    application_type_id=row.get("group_type_id"),
                    approval_level=row.get("approval_level"),
                )
                cs_objects.append(cs)

            except Exception as e:
                logger.error(f"Error preparing row {row.get('migrated_from_id')}: {e}")
                stats["errors"] += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": "N/A",
                        "level": "error",
                        "message": str(e),
                        "raw_value": "N/A",
                        "reason": "Exception during preparation",
                        "row_json": json.dumps(row, default=str),
                        "timestamp": timezone.now().isoformat(),
                    }
                )

        # Bulk create SubmitterInformation
        if submitter_infos:
            logger.info(f"Bulk creating {len(submitter_infos)} SubmitterInformation records...")
            SubmitterInformation.objects.bulk_create(submitter_infos)

            # Assign saved SubmitterInformation to ConservationStatus objects
            # Since lists are ordered and we appended in sync, we can zip them.
            for cs, sub_info in zip(cs_objects, submitter_infos):
                cs.submitter_information = sub_info

        # Bulk create ConservationStatus
        if cs_objects:
            logger.info(f"Bulk creating {len(cs_objects)} ConservationStatus records...")
            # Use bulk_create and get back objects with IDs (Postgres feature)
            created_cs = ConservationStatus.objects.bulk_create(cs_objects)

            # Post-creation updates (conservation_status_number)
            # We need to update conservation_status_number = "CS{id}"
            to_update = []
            for cs in created_cs:
                if not cs.conservation_status_number:
                    cs.conservation_status_number = f"CS{cs.pk}"
                    to_update.append(cs)

            if to_update:
                logger.info(f"Bulk updating conservation_status_number for {len(to_update)} records...")
                ConservationStatus.objects.bulk_update(to_update, ["conservation_status_number"])

            stats["created"] += len(created_cs)

        # Write errors to CSV
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

            logger.info("Writing ConservationStatusImporter error CSV to %s", csv_path)
            print(f"Writing error CSV to: {csv_path}")

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
                    writer = csv.DictWriter(fh, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(errors_details)
                print(f"Successfully wrote {len(errors_details)} error records to CSV.")
            except Exception as e:
                logger.error(f"Failed to write error CSV: {e}")
                print(f"Failed to write error CSV: {e}")

        elapsed = timezone.now() - start_time
        logger.info(f"Import complete. Stats: {stats} time_taken={elapsed}")
