import csv
import json
import logging
from collections import defaultdict

from django.db import transaction
from django.utils import timezone
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
                        ocrassociatedspecies__id__in=ocr_assoc_ids
                    ).delete()[0]
                else:
                    # Delete all
                    deleted_count = AssociatedSpeciesTaxonomy.objects.all().delete()[0]

                logger.info(f"Deleted {deleted_count} AssociatedSpeciesTaxonomy records.")

                # Reset PK sequence
                from django.db import connection as conn

                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT setval(pg_get_serial_sequence('boranga_associatedspeciestaxonomy', 'id'), "
                        "COALESCE((SELECT MAX(id) FROM boranga_associatedspeciestaxonomy), 0) + 1, false);"
                    )
                    logger.info("Reset AssociatedSpeciesTaxonomy PK sequence.")

            except Exception as e:
                logger.error(f"Error clearing AssociatedSpeciesTaxonomy: {e}")
                raise

    def run(self, path: str, ctx: ImportContext, sources=None, **options):
        # Initialize stats
        stats = ctx.stats.setdefault(self.slug, self.new_stats())

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
                stats["extracted"] = stats.get("extracted", 0) + len(result.rows)
            except FileNotFoundError:
                logger.warning(f"File not found: {src_path}. Skipping.")
                continue

        if not all_rows:
            logger.info("No rows extracted.")
            return stats

        self.process_rows(all_rows, stats)
        return stats

    def process_rows(self, rows, stats):
        # Group by migrated_from_id (S_ID from SITE_VISITS mapping)
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["migrated_from_id"]].append(row)

        logger.info(f"Processing {len(grouped)} Site Visits for Associated Species.")

        site_visit_ids = list(grouped.keys())

        # Batch fetch Occurrence Reports by migrated_from_id
        ocrs = {o.migrated_from_id: o for o in OccurrenceReport.objects.filter(migrated_from_id__in=site_visit_ids)}

        logger.info(f"Found {len(ocrs)} matching Occurrence Reports.")
        stats["ocrs_matched"] = len(ocrs)
        stats["site_visits_without_ocr"] = len(grouped) - len(ocrs)

        # Collect all taxon_name_id values from rows
        taxon_name_ids = set()
        for row in rows:
            val = row.get("taxon_name_id")
            if val:
                try:
                    taxon_name_ids.add(int(val))
                except ValueError:
                    logger.warning(f"Invalid taxon_name_id: {val}")

        # Resolve Taxonomy objects by taxon_name_id
        taxonomies = {t.taxon_name_id: t for t in Taxonomy.objects.filter(taxon_name_id__in=taxon_name_ids)}

        logger.info(f"Resolved {len(taxonomies)} Taxonomies out of {len(taxon_name_ids)} requested.")
        stats["taxonomies_resolved"] = len(taxonomies)
        stats["taxonomies_not_found"] = len(taxon_name_ids) - len(taxonomies)

        errors_details = []  # Track skipped records for CSV output

        with transaction.atomic():
            # Step 1: Ensure all OCRs have an OCRAssociatedSpecies record
            ocr_ids = [ocr.id for ocr in ocrs.values()]
            existing_ocr_assoc = {
                oa.occurrence_report_id: oa
                for oa in OCRAssociatedSpecies.objects.filter(occurrence_report_id__in=ocr_ids)
            }

            to_create_ocr_assoc = []
            for ocr in ocrs.values():
                if ocr.id not in existing_ocr_assoc:
                    to_create_ocr_assoc.append(OCRAssociatedSpecies(occurrence_report=ocr))

            if to_create_ocr_assoc:
                created_ocr_assoc = OCRAssociatedSpecies.objects.bulk_create(to_create_ocr_assoc)
                logger.info(f"Bulk created {len(created_ocr_assoc)} OCRAssociatedSpecies records.")
                stats["ocr_assoc_created"] = len(created_ocr_assoc)
                for oa in created_ocr_assoc:
                    existing_ocr_assoc[oa.occurrence_report_id] = oa

            # Step 2: Collect unique taxonomy_ids (not taxon_name_id!) needed for AssociatedSpeciesTaxonomy
            # Following occurrence_reports.py pattern: use taxonomy.pk (id), not taxon_name_id
            taxonomy_ids_needed = set()
            for row in rows:
                tid_raw = row.get("taxon_name_id")
                if not tid_raw:
                    continue
                try:
                    taxon_name_id = int(tid_raw)
                except ValueError:
                    continue

                taxonomy = taxonomies.get(taxon_name_id)
                if taxonomy:
                    taxonomy_ids_needed.add(taxonomy.pk)  # Use taxonomy.pk (the id field)

            # Step 3: Load existing AssociatedSpeciesTaxonomy rows (following occurrence_reports.py pattern)
            # Only care about taxonomy_id matching; comments and species_role are not unique constraints
            ast_qs = AssociatedSpeciesTaxonomy.objects.filter(taxonomy_id__in=list(taxonomy_ids_needed))
            taxid_to_ast = {}
            for ast in ast_qs:
                # Per occurrence_reports.py: take first if multiple exist for same taxonomy
                if ast.taxonomy_id not in taxid_to_ast:
                    taxid_to_ast[ast.taxonomy_id] = ast

            # Step 4: Create missing AssociatedSpeciesTaxonomy rows
            missing_tax_ids = taxonomy_ids_needed - set(taxid_to_ast.keys())

            if missing_tax_ids:
                create_objs = [AssociatedSpeciesTaxonomy(taxonomy_id=tid) for tid in missing_tax_ids]

                try:
                    with transaction.atomic():
                        # We have pre-filtered existing IDs, so collisions shouldn't happen unless race condition.
                        # Try normal bulk_create first to catch errors.
                        created = AssociatedSpeciesTaxonomy.objects.bulk_create(create_objs, batch_size=500)
                        logger.info(f"Bulk created {len(created)} new AssociatedSpeciesTaxonomy records.")
                        stats["ast_created"] = len(created)
                except Exception as e:
                    logger.error(f"Bulk create failed: {e}. Falling back to safe individual creation.")
                    created_count = 0
                    for tid in missing_tax_ids:
                        # Use get_or_create to handle potential races/duplicates gracefully
                        try:
                            AssociatedSpeciesTaxonomy.objects.get_or_create(taxonomy_id=tid)
                            created_count += 1
                        except Exception as inner_e:
                            logger.error(
                                f"Failed to get_or_create AssociatedSpeciesTaxonomy for taxonomy_id={tid}: {inner_e}"
                            )
                    stats["ast_created"] = created_count

                # Refresh to get PKs for all records (both created and existing)
                for ast in AssociatedSpeciesTaxonomy.objects.filter(taxonomy_id__in=list(missing_tax_ids)):
                    if ast.taxonomy_id not in taxid_to_ast:
                        taxid_to_ast[ast.taxonomy_id] = ast

                # Validation check
                still_missing = missing_tax_ids - set(taxid_to_ast.keys())
                if still_missing:
                    logger.error(
                        f"Critical: The following taxonomy_ids are still missing from AssociatedSpeciesTaxonomy after creation: {still_missing}"
                    )
                    # Force check specific problem ID
                    if 147457 in still_missing:
                        exists = AssociatedSpeciesTaxonomy.objects.filter(taxonomy_id=147457).exists()
                        logger.error(f"DEBUG: Explicit check for 147457.exists() -> {exists}")

            # Step 5: Build many-to-many relationships
            # Group ASTs by OCRAssociatedSpecies to minimize queries
            m2m_by_ocr_assoc = defaultdict(set)

            skip_reasons = defaultdict(int)

            for vid, species_rows in grouped.items():
                ocr = ocrs.get(vid)
                if not ocr:
                    for s_row in species_rows:
                        skip_reasons["no_matching_ocr"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": vid,
                                "taxon_name_id": s_row.get("taxon_name_id"),
                                "level": "warning",
                                "reason": "no_matching_ocr",
                                "message": f"No OccurrenceReport found for site visit {vid}",
                                "row": s_row,
                            }
                        )
                    continue

                ocr_assoc = existing_ocr_assoc.get(ocr.id)
                if not ocr_assoc:
                    for s_row in species_rows:
                        skip_reasons["no_ocr_assoc"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": vid,
                                "taxon_name_id": s_row.get("taxon_name_id"),
                                "level": "error",
                                "reason": "no_ocr_assoc",
                                "message": f"No OCRAssociatedSpecies for OCR {ocr.id}",
                                "row": s_row,
                            }
                        )
                    continue

                for s_row in species_rows:
                    tid_raw = s_row.get("taxon_name_id")
                    if not tid_raw:
                        skip_reasons["missing_taxon_name_id"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": vid,
                                "taxon_name_id": None,
                                "level": "warning",
                                "reason": "missing_taxon_name_id",
                                "message": "taxon_name_id field is empty",
                                "row": s_row,
                            }
                        )
                        continue

                    try:
                        taxon_name_id = int(tid_raw)
                    except ValueError:
                        skip_reasons["invalid_taxon_name_id"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": vid,
                                "taxon_name_id": tid_raw,
                                "level": "warning",
                                "reason": "invalid_taxon_name_id",
                                "message": f"Invalid taxon_name_id: {tid_raw}",
                                "row": s_row,
                            }
                        )
                        continue

                    taxonomy = taxonomies.get(taxon_name_id)
                    if not taxonomy:
                        skip_reasons["taxonomy_not_resolved"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": vid,
                                "taxon_name_id": taxon_name_id,
                                "level": "warning",
                                "reason": "taxonomy_not_resolved",
                                "message": f"Taxonomy with taxon_name_id {taxon_name_id} not found",
                                "row": s_row,
                            }
                        )
                        continue

                    ast = taxid_to_ast.get(taxonomy.pk)
                    if ast:
                        if ast in m2m_by_ocr_assoc[ocr_assoc]:
                            skip_reasons["duplicate_merged"] += 1
                            errors_details.append(
                                {
                                    "migrated_from_id": vid,
                                    "taxon_name_id": taxon_name_id,
                                    "level": "warning",
                                    "reason": "duplicate_merged",
                                    "message": f"Duplicate species entry merged for taxonomy_id {taxonomy.pk}",
                                    "row": s_row,
                                }
                            )
                        m2m_by_ocr_assoc[ocr_assoc].add(ast)
                    else:
                        skip_reasons["ast_not_found"] += 1
                        errors_details.append(
                            {
                                "migrated_from_id": vid,
                                "taxon_name_id": taxon_name_id,
                                "level": "error",
                                "reason": "ast_not_found",
                                "message": f"AssociatedSpeciesTaxonomy not found for taxonomy.pk {taxonomy.pk}",
                                "row": s_row,
                            }
                        )

            # Bulk add M2M relationships
            total_links = sum(len(asts) for asts in m2m_by_ocr_assoc.values())
            logger.info(
                f"Adding {total_links} AssociatedSpeciesTaxonomy links to {len(m2m_by_ocr_assoc)} OCRAssociatedSpecies..."
            )
            stats["m2m_links_created"] = total_links

            for ocr_assoc, ast_set in m2m_by_ocr_assoc.items():
                ocr_assoc.related_species.add(*ast_set)

            logger.info(f"Successfully linked {total_links} AssociatedSpeciesTaxonomy relationships.")

            # Log skip reasons
            if skip_reasons:
                logger.warning("Records skipped by reason:")
                for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
                    logger.warning(f"  {reason}: {count}")
                    stats[f"skip_reason_{reason}"] = count

            # Update standard stats fields
            stats["processed"] = len(rows)
            stats["created"] = total_links  # M2M links are the actual "records" created
            # Calculate how many input rows were skipped
            rows_linked = sum(len(species_rows) for vid, species_rows in grouped.items() if vid in ocrs)
            stats["skipped"] = len(rows) - rows_linked

            # Write errors/warnings CSV
            if errors_details:
                import os

                csv_path = "private-media/handler_output/associated_species_errors.csv"
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                try:
                    with open(csv_path, "w", newline="") as f:
                        writer = csv.DictWriter(
                            f,
                            fieldnames=[
                                "migrated_from_id",
                                "taxon_name_id",
                                "level",
                                "reason",
                                "message",
                                "row_json",
                                "timestamp",
                            ],
                        )
                        writer.writeheader()
                        for rec in errors_details:
                            writer.writerow(
                                {
                                    "migrated_from_id": rec.get("migrated_from_id"),
                                    "taxon_name_id": rec.get("taxon_name_id"),
                                    "level": rec.get("level"),
                                    "reason": rec.get("reason"),
                                    "message": rec.get("message"),
                                    "row_json": json.dumps(rec.get("row", {}), default=str),
                                    "timestamp": timezone.now().isoformat(),
                                }
                            )
                    stats["error_details_csv"] = csv_path
                    logger.info(f"Wrote {len(errors_details)} skipped records to {csv_path}")
                except Exception as e:
                    logger.exception(f"Failed to write error CSV: {e}")
