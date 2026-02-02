import logging
from collections import defaultdict

from django.db import transaction
from django.utils.module_loading import import_string

from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import BaseSheetImporter, ImportContext, register
from boranga.components.occurrence.models import AssociatedSpeciesTaxonomy, OccurrenceReport, OCRAssociatedSpecies
from boranga.components.species_and_communities.models import Taxonomy

logger = logging.getLogger(__name__)


@register
class AssociatedSpeciesImporter(BaseSheetImporter):
    slug = "associated_species"
    description = "Import Associated Species from legacy sources"

    SOURCE_ADAPTERS = {
        Source.TEC_SITE_SPECIES.value: "boranga.components.data_migration.adapters.occurrence_report.tec_site_species.OccurrenceReportTecSiteSpeciesAdapter",
    }

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """Delete AssociatedSpeciesTaxonomy target data. Respect `ctx.dry_run`."""
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
                "AssociatedSpeciesImporter: deleting AssociatedSpeciesTaxonomy data for group_types: %s ...",
                target_group_types,
            )
        else:
            logger.warning("AssociatedSpeciesImporter: deleting all AssociatedSpeciesTaxonomy data...")

        with transaction.atomic():
            try:
                # Delete AssociatedSpeciesTaxonomy records linked to OCRs of specific group types
                if is_filtered:
                    # Find all OCRs with the target group types
                    ocr_ids = OccurrenceReport.objects.filter(group_type__name__in=target_group_types).values_list(
                        "id", flat=True
                    )

                    # Find all OCRAssociatedSpecies for those OCRs
                    ocr_assoc_ids = OCRAssociatedSpecies.objects.filter(occurrence_report_id__in=ocr_ids).values_list(
                        "id", flat=True
                    )

                    # Delete AST records linked to those OCRAssociatedSpecies
                    deleted_count = AssociatedSpeciesTaxonomy.objects.filter(
                        ocrrelatedspecies__id__in=ocr_assoc_ids
                    ).delete()[0]
                else:
                    # Delete all
                    deleted_count = AssociatedSpeciesTaxonomy.objects.all().delete()[0]

                logger.info(f"Deleted {deleted_count} AssociatedSpeciesTaxonomy records.")

                # Reset PK sequence
                from django.db import connection as conn

                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT setval(pg_get_serial_sequence('occurrence_associatedspeciestaxonomy', 'id'), "
                        "COALESCE((SELECT MAX(id) FROM occurrence_associatedspeciestaxonomy), 1), false);"
                    )
                    logger.info("Reset AssociatedSpeciesTaxonomy PK sequence.")

            except Exception as e:
                logger.error(f"Error clearing AssociatedSpeciesTaxonomy: {e}")
                raise

    def run(self, path: str, ctx: ImportContext, sources=None, **options):
        # Allow filtering sources
        if not sources:
            sources = self.SOURCE_ADAPTERS.keys()

        all_rows = []
        for src in sources:
            if src not in self.SOURCE_ADAPTERS:
                continue

            # Lazily load adapter
            adapter_path = self.SOURCE_ADAPTERS[src]
            adapter_cls = import_string(adapter_path)
            adapter = adapter_cls()

            # Determine file path.
            src_path = path
            if path and not path.lower().endswith(".csv") and not path.lower().endswith(".json"):
                # Assume directory
                if src == Source.TEC_SITE_SPECIES.value:
                    src_path = f"{path}/SITE_SPECIES.csv"

            logger.info(f"Extracting associated species from {src_path}")
            try:
                result = adapter.extract(src_path)
                logger.info(f"Extracted {len(result.rows)} rows from {src_path}")
                all_rows.extend(result.rows)
            except FileNotFoundError:
                logger.warning(f"File not found: {src_path}. Skipping.")
                continue

        if not all_rows:
            logger.info("No rows extracted.")
            return

        self.process_rows(all_rows)

    def process_rows(self, rows):
        # Group by migrated_from_id (SITE_VISIT_ID)
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["migrated_from_id"]].append(row)

        logger.info(f"Processing {len(grouped)} Site Visits for Associated Species.")

        site_visit_ids = list(grouped.keys())

        # Batch fetch Occurrence Reports
        # Assuming migrated_from_id is populated from SITE_VISIT_ID in TEC source migration of sites.
        ocrs = {o.migrated_from_id: o for o in OccurrenceReport.objects.filter(migrated_from_id__in=site_visit_ids)}

        logger.info(f"Found {len(ocrs)} matching Occurrence Reports.")

        # Resolve Taxonomies
        taxon_ids = set()
        for row in rows:
            # taxon_name_id might be string in CSV, ensure int?
            val = row.get("taxon_name_id")
            if val:
                try:
                    taxon_ids.add(int(val))
                except ValueError:
                    logger.warning(f"Invalid taxon_name_id: {val}")

        taxonomies = {t.taxon_name_id: t for t in Taxonomy.objects.filter(taxon_name_id__in=taxon_ids)}

        logger.info(f"Resolved {len(taxonomies)} Taxonomies out of {len(taxon_ids)} requested.")

        with transaction.atomic():
            created_count = 0
            for vid, species_rows in grouped.items():
                ocr = ocrs.get(vid)
                if not ocr:
                    # Only warn if it's expected to be there?
                    # logger.warning(f"Associated Species: No Occurrence Report found for Site Visit ID {vid}")
                    continue

                # Get or Create OCRAssociatedSpecies
                try:
                    ocr_assoc = ocr.associated_species
                except OCRAssociatedSpecies.DoesNotExist:
                    ocr_assoc = OCRAssociatedSpecies.objects.create(occurrence_report=ocr)

                for s_row in species_rows:
                    tid_raw = s_row.get("taxon_name_id")
                    if not tid_raw:
                        continue

                    try:
                        tid = int(tid_raw)
                    except ValueError:
                        continue

                    taxonomy = taxonomies.get(tid)
                    if not taxonomy:
                        logger.debug(f"Taxonomy ID {tid} not found for Site Visit {vid}.")
                        # As per note: "not able to be matched to a taxon - these have been retained in the Excel file... but deleted from the CSV"
                        # So we might not see them, or if we do, skip them.
                        continue

                    comments = s_row.get("comments", "")

                    # Create AST
                    ast = AssociatedSpeciesTaxonomy.objects.create(
                        taxonomy=taxonomy, comments=comments, species_role=None
                    )

                    ocr_assoc.related_species.add(ast)
                    created_count += 1

            logger.info(f"Successfully created {created_count} AssociatedSpeciesTaxonomy records.")
