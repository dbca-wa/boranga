from __future__ import annotations

import logging

from django.apps import apps

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
        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []

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
        WALegislativeCategory = apps.get_model("boranga", "WALegislativeCategory")
        SubmitterInformation = apps.get_model("boranga", "SubmitterInformation")
        SubmitterCategory = apps.get_model("boranga", "SubmitterCategory")

        # Cache lookups
        # Species lookup by name (via taxonomy)
        # Assuming species_name in CSV matches taxonomy.scientific_name
        # And we need the Species object that links to that Taxonomy.
        # Note: Multiple species might link to same taxonomy? No, usually 1:1 or N:1.
        # Species has taxonomy FK.

        # Let's build a map: scientific_name -> Species object
        # We only care about FLORA species since we set group_type_id=FLORA
        species_map = {}
        qs = Species.objects.filter(group_type__name="flora").select_related("taxonomy")
        for s in qs:
            if s.taxonomy and s.taxonomy.scientific_name:
                species_map[s.taxonomy.scientific_name.strip().lower()] = s

        # WA Priority List
        # Try to find 'FLORA' list
        wa_priority_list_obj = WAPriorityList.objects.filter(
            code__iexact="FLORA"
        ).first()
        if not wa_priority_list_obj:
            wa_priority_list_obj = WAPriorityList.objects.filter(
                label__iexact="FLORA"
            ).first()

        if not wa_priority_list_obj:
            logger.warning("Could not find WAPriorityList with code/label 'FLORA'.")

        # WA Legislative Category Cache
        wa_leg_cat_map = {}
        for cat in WALegislativeCategory.objects.all():
            wa_leg_cat_map[cat.code.strip().upper()] = cat

        # Submitter Category 'DBCA'
        submitter_category_dbca = SubmitterCategory.objects.filter(name="DBCA").first()
        if not submitter_category_dbca:
            # Create if not exists? Or warn?
            # Usually exists.
            pass

        # 3. Create objects
        ConservationStatus = apps.get_model("boranga", "ConservationStatus")

        # Prepare lists for bulk operations
        submitter_infos = []
        cs_objects = []

        # First pass: Prepare SubmitterInformation and ConservationStatus objects
        valid_rows = []
        for row in all_rows:
            try:
                # Resolve Species
                species_name = row.get("species_name")
                species_obj = None
                taxonomy_obj = None

                if species_name:
                    species_obj = species_map.get(species_name.strip().lower())
                    if species_obj:
                        taxonomy_obj = species_obj.taxonomy
                    else:
                        logger.warning(f"Species not found for name: {species_name}")
                        stats["skipped"] += 1
                        continue

                # Resolve WA Legislative Category
                wa_leg_cat_code = row.get("wa_legislative_category")
                wa_leg_cat_obj = None
                if wa_leg_cat_code:
                    wa_leg_cat_obj = wa_leg_cat_map.get(wa_leg_cat_code.strip().upper())
                    if not wa_leg_cat_obj:
                        logger.warning(
                            f"WA Legislative Category not found for code: {wa_leg_cat_code}"
                        )

                # Create SubmitterInformation instance (do not save yet)
                sub_info = SubmitterInformation(
                    email_user=row.get("submitter"),
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
                    wa_priority_list=(
                        wa_priority_list_obj
                        if row.get("wa_priority_list") == "FLORA"
                        else None
                    ),
                    wa_legislative_category=wa_leg_cat_obj,
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

        logger.info(f"Import complete. Stats: {stats}")
