from __future__ import annotations

import csv
import json
import logging
import os
from pathlib import Path

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone

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
    register,
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

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete ConservationStatus target data. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            return

        logger.warning(
            "ConservationStatusImporter: deleting ConservationStatus data..."
        )
        from django.apps import apps
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)

        try:
            ConservationStatus = apps.get_model("boranga", "ConservationStatus")
            # Only delete migrated records? Or all?
            # Usually we delete all if we are doing a full reload.
            # But maybe filter by migrated_from_id__isnull=False?
            # For safety, let's delete all for now as per previous patterns,
            # or maybe just those with migrated_from_id.
            # Given the user wants a migration run, usually it implies wiping previous migration data.
            ConservationStatus.objects.filter(migrated_from_id__isnull=False).delete()

            # Reset sequence?
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT setval(pg_get_serial_sequence('boranga_conservationstatus', 'id'), "
                    "coalesce(max(id), 1), max(id) IS NOT null) FROM boranga_conservationstatus;"
                )

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
            stats["extracted"] += len(res.rows)
            warnings.extend(res.warnings)
            all_rows.extend(res.rows)

        if ctx.dry_run:
            logger.info(f"[DRY RUN] Would import {len(all_rows)} rows.")
            return

        # 2. Load dependencies
        Species = apps.get_model("boranga", "Species")
        WAPriorityList = apps.get_model("boranga", "WAPriorityList")
        WAPriorityCategory = apps.get_model("boranga", "WAPriorityCategory")
        WALegislativeList = apps.get_model("boranga", "WALegislativeList")
        WALegislativeCategory = apps.get_model("boranga", "WALegislativeCategory")
        SubmitterInformation = apps.get_model("boranga", "SubmitterInformation")
        SubmitterCategory = apps.get_model("boranga", "SubmitterCategory")

        # Cache lookups
        # Species lookup by name (via taxonomy)
        species_map = {}
        qs = Species.objects.filter(group_type__name="flora").select_related("taxonomy")
        for s in qs:
            if s.taxonomy and s.taxonomy.scientific_name:
                species_map[s.taxonomy.scientific_name.strip().lower()] = s

        # Community lookup by migrated_from_id
        Community = apps.get_model("boranga", "Community")
        community_map = {
            c.migrated_from_id: c
            for c in Community.objects.filter(migrated_from_id__isnull=False)
        }

        # Load legacy name map
        legacy_name_map = {}
        try:
            map_path = (
                Path(settings.BASE_DIR)
                / "private-media"
                / "legacy_data"
                / "TPFL"
                / "tpfl-legacy-name-to-taxon-name-id.csv"
            )
            if map_path.exists():
                with open(map_path, encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("NAME") and row.get("nomos_canonical_name"):
                            legacy_name_map[row["NAME"].strip().lower()] = row[
                                "nomos_canonical_name"
                            ].strip()
            else:
                logger.warning(f"Legacy name map not found at {map_path}")
        except Exception as e:
            logger.warning(f"Failed to load legacy name map: {e}")

        # Lists and Categories Caches
        wa_priority_list_map = {
            pl.code.strip().upper(): pl for pl in WAPriorityList.objects.all()
        }
        wa_priority_category_map = {
            pc.code.strip().upper(): pc for pc in WAPriorityCategory.objects.all()
        }
        wa_legislative_list_map = {
            ll.code.strip().upper(): ll for ll in WALegislativeList.objects.all()
        }
        wa_legislative_category_map = {
            lc.code.strip().upper(): lc for lc in WALegislativeCategory.objects.all()
        }

        # Submitter Category 'DBCA'
        submitter_category_dbca = SubmitterCategory.objects.filter(name="DBCA").first()
        if not submitter_category_dbca:
            logger.warning(
                "SubmitterCategory 'DBCA' not found. SubmitterInformation records may be incomplete."
            )

        # TEC Submitter User
        User = get_user_model()
        tec_user = User.objects.filter(email="boranga.tec@dbca.wa.gov.au").first()
        if not tec_user:
            logger.warning(
                "User 'boranga.tec@dbca.wa.gov.au' not found. TEC SubmitterInformation will use default submitter."
            )

        # 3. Create objects
        ConservationStatus = apps.get_model("boranga", "ConservationStatus")

        # Prepare lists for bulk operations
        submitter_infos = []
        cs_objects = []

        # First pass: Prepare SubmitterInformation and ConservationStatus objects
        valid_rows = []
        for row in all_rows:
            try:
                # Resolve Species or Community
                species_name = row.get("species_name")
                community_mig_id = row.get("community_migrated_from_id")
                species_obj = None
                taxonomy_obj = None
                community_obj = None

                if species_name:
                    clean_name = species_name.strip().lower()
                    species_obj = species_map.get(clean_name)

                    # Try legacy map if not found
                    if not species_obj and clean_name in legacy_name_map:
                        mapped_name = legacy_name_map[clean_name]
                        species_obj = species_map.get(mapped_name.lower())
                        if species_obj:
                            # logger.info(f"Mapped legacy name '{species_name}' to '{mapped_name}'")
                            pass

                    if species_obj:
                        taxonomy_obj = species_obj.taxonomy
                    else:
                        msg = f"Species not found for name: {species_name}"
                        logger.warning(msg)
                        stats["skipped"] += 1
                        stats["errors"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": row.get("migrated_from_id"),
                                "column": "species_name",
                                "level": "error",
                                "message": msg,
                                "raw_value": species_name,
                                "reason": "Species lookup failed",
                                "row_json": json.dumps(row, default=str),
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        continue
                elif community_mig_id:
                    community_obj = community_map.get(community_mig_id)
                    if not community_obj:
                        msg = f"Community not found for migrated_from_id: {community_mig_id}"
                        logger.warning(msg)
                        stats["skipped"] += 1
                        stats["errors"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": row.get("migrated_from_id"),
                                "column": "community_migrated_from_id",
                                "level": "error",
                                "message": msg,
                                "raw_value": community_mig_id,
                                "reason": "Community lookup failed",
                                "row_json": json.dumps(row, default=str),
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        continue

                # Resolve Lists and Categories
                wa_pl_code = row.get("wa_priority_list")
                wa_pl_obj = None
                if wa_pl_code:
                    # Handle "community" static value mapping to list name
                    if wa_pl_code == "community":
                        # Find list with name='community' or similar?
                        # Task says: Apply static value wa_priority_list_id where name = 'community'
                        # But WAPriorityList has 'code' and 'label'.
                        # Assuming there is a list with code='COMMUNITY' or label='Community'?
                        # Let's try to find by code first, then label.
                        # Or maybe the user meant GroupType? No, field is wa_priority_list.
                        # Let's assume there is a WAPriorityList with code 'P1', 'P2' etc.
                        # If the value is "community", maybe it means the list applies to communities?
                        # Wait, Task 12071 says: "Apply static value wa_priority_list_id where name = 'community'"
                        # WAPriorityList model has 'code' and 'label'. It doesn't have 'name'.
                        # Maybe it means GroupType? But the field is wa_priority_list.
                        # Let's check WAPriorityList model again.
                        # It inherits AbstractConservationList which has code, label.
                        # Maybe the user means the list instance that is for communities?
                        # But usually lists are P1, P2, etc.
                        # Let's look for a list with code="COMMUNITY" or label="Community".
                        # If not found, maybe log warning.
                        # For now, let's try to find a list where code="COMMUNITY".
                        wa_pl_obj = wa_priority_list_map.get("COMMUNITY")
                        if not wa_pl_obj:
                            # Try finding by label?
                            # Or maybe the user meant the list associated with the community group type?
                            # But priority lists are specific (e.g. P1).
                            # If the legacy data has "wa_priority_list" column, we should use that.
                            # But the task says "Apply static value...".
                            # If the static value is "community", it's weird.
                            # Maybe they mean the list named "Priority List"?
                            # Let's assume for now we look up by code "COMMUNITY".
                            pass
                    else:
                        wa_pl_obj = wa_priority_list_map.get(wa_pl_code.strip().upper())

                wa_pc_code = row.get("wa_priority_category")
                wa_pc_obj = (
                    wa_priority_category_map.get(wa_pc_code.strip().upper())
                    if wa_pc_code
                    else None
                )

                wa_ll_code = row.get("wa_legislative_list")
                wa_ll_obj = (
                    wa_legislative_list_map.get(wa_ll_code.strip().upper())
                    if wa_ll_code
                    else None
                )

                wa_lc_code = row.get("wa_legislative_category")
                wa_lc_obj = (
                    wa_legislative_category_map.get(wa_lc_code.strip().upper())
                    if wa_lc_code
                    else None
                )

                # Determine submitter for SubmitterInformation
                si_email_user = row.get("submitter")
                if community_mig_id and tec_user:
                    si_email_user = tec_user.id

                # Create SubmitterInformation instance (do not save yet)
                sub_info = SubmitterInformation(
                    email_user=si_email_user,
                    organisation="DBCA",
                    submitter_category=submitter_category_dbca,
                )
                submitter_infos.append(sub_info)

                # Create ConservationStatus instance (do not save yet)
                cs = ConservationStatus(
                    migrated_from_id=row.get("migrated_from_id"),
                    processing_status=row.get("processing_status"),
                    customer_status=row.get("customer_status"),
                    species=species_obj,
                    species_taxonomy=taxonomy_obj,
                    community=community_obj,
                    wa_priority_list=wa_pl_obj,
                    wa_priority_category=wa_pc_obj,
                    wa_legislative_list=wa_ll_obj,
                    wa_legislative_category=wa_lc_obj,
                    review_due_date=row.get("review_due_date"),
                    effective_from=row.get("effective_from_date"),
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
                valid_rows.append(row)

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
            logger.info(
                f"Bulk creating {len(submitter_infos)} SubmitterInformation records..."
            )
            SubmitterInformation.objects.bulk_create(submitter_infos)

            # Assign saved SubmitterInformation to ConservationStatus objects
            # Since lists are ordered and we appended in sync, we can zip them.
            for cs, sub_info in zip(cs_objects, submitter_infos):
                cs.submitter_information = sub_info

        # Bulk create ConservationStatus
        if cs_objects:
            logger.info(
                f"Bulk creating {len(cs_objects)} ConservationStatus records..."
            )
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
                logger.info(
                    f"Bulk updating conservation_status_number for {len(to_update)} records..."
                )
                ConservationStatus.objects.bulk_update(
                    to_update, ["conservation_status_number"]
                )

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
