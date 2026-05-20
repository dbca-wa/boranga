# TPFL

## Commands to create a partitioned file for the main TPFL legacy data files

These will create a much smaller partitioned file so the business users don't have do sift through so many files when doing data verification.
They can use the report generated as a guide to which records contain the 'Interesting' columns

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.species.tpfl.SpeciesTpflAdapter \
 --input private-media/legacy_data/TPFL/species-profiles-combined.csv \
 --output private-media/legacy_data/TPFL/species-profiles-partitioned.csv \
 --report private-media/legacy_data/TPFL/species-profiles-partitioned-report.csv \
 --max-cardinality 100 \
 --heaviest-last

Once the business users have done their data verification, run the full migration of species with the --wipe-targets flag and then move on to the next migration run.

This process is completed until data verification is complete for a full legacy migration if performed

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.conservation_status.tpfl.ConservationStatusTpflAdapter \
 --input private-media/legacy_data/TPFL/FLORA_CS_Migration_Template_FULL.csv \
 --output private-media/legacy_data/TPFL/conservation-status-partitioned.csv \
 --report private-media/legacy_data/TPFL/conservation-status-partitioned-report.csv \
 --max-cardinality 50 \
 --heaviest-last

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.occurrence.tpfl.OccurrenceTpflAdapter \
 --input private-media/legacy_data/TPFL/DRF_POPULATION.csv \
 --output private-media/legacy_data/TPFL/occurrences-partitioned.csv \
 --report private-media/legacy_data/TPFL/occurrences-report.csv \
 --max-cardinality 100 \
 --heaviest-last

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.occurrence_report.tpfl.OccurrenceReportTpflAdapter \
 --input private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv \
 --output private-media/legacy_data/TPFL/occurrence-reports-partitioned.csv \
 --report private-media/legacy_data/TPFL/occurrence-reports-report.csv \
 --max-cardinality 100 \
 --heaviest-last

# --- TEC

## Conservation Status

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.conservation_status.tec.ConservationStatusTecAdapter \
 --input private-media/legacy_data/TEC/community-cs-migration-template.csv \
 --output private-media/legacy_data/TEC/community-cs-partitioned.csv \
 --report private-media/legacy_data/TEC/community-cs-report.csv \
 --max-cardinality 50 \
 --sample-skipped 2 \
 --heaviest-last

Produces ~18 rows: set-cover over the 6 categorical FK lookup columns (iucn_version,
wa_legislative/priority list+category, commonwealth) + 2 spot-check rows per date
field (effective_from, effective_to, listing_date) to verify date parsing, without
exhaustively covering every unique date value.

## TEC Occurrences

TEC stores its data across several related CSVs (OCCURRENCES, SITES, FIRE_HISTORY,
ADDITIONAL_DATA, OCCURRENCE_SPECIES_COMBINE, SURVEYS, SURVEY_THREATS, SITE_VISITS)
rather than one flat file, so the generic partition_migration_data.py cannot be used.

A dedicated script handles the multi-table join and computes a richness score for
each occurrence (weighted count of fire history entries, surveys, threats, site visits
and associated species). The greedy set-cover runs over interesting OCCURRENCES columns
(FK-mapped fields, habitat fields, identification fields) and the result is unioned
with the top-N richest records so that the "phattest" occurrences are always included.

python3 scripts/partition_tec_occurrences.py \
 --input  private-media/legacy_data/TEC/ \
 --output private-media/legacy_data/TEC/tec-occurrences-partitioned.csv \
 --report private-media/legacy_data/TEC/tec-occurrences-report.csv \
 --max-cardinality 50 \
 --top-n 200 \
 --heaviest-last

Two output files are produced:
  tec-occurrences-partitioned.csv  — subset of OCCURRENCES rows selected for
                                     verification, with _richness_score,
                                     _site_count, _fire_count, _survey_count,
                                     _threat_count, _site_visit_count etc. added
                                     as extra columns.
  tec-occurrences-report.csv       — all ~127k occurrences ranked by richness_score
                                     descending so verifiers can cherry-pick records
                                     of interest.
