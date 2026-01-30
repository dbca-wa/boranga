# TPFL

## Commands to create a partitioned file for the main TPFL legacy data files

These will create a much smaller partitioned file so the business users don't have do sift through so many files when doing data verification.
They can use the report generated as a guide to which records

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.species.tpfl.SpeciesTpflAdapter \
 --input private-media/legacy_data/TPFL/species-profiles-combined.csv \
 --output private-media/legacy_data/TPFL/species-profiles-partitioned.csv \
 --report private-media/legacy_data/TPFL/species-profiles-partitioned-report.csv \
 --max-cardinality 100 \
 --heaviest-first

Once the business users have done their data verification, run the full migration of species with the --wipe-targets flag and then move on to the next migration run.

This process is completed until data verification is complete for a full legacy migration if performed

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.occurrence.tpfl.OccurrenceTpflAdapter \
 --input private-media/legacy_data/TPFL/DRF_POPULATION.csv \
 --output private-media/legacy_data/TPFL/occurrences-partitioned.csv \
 --report private-media/legacy_data/TPFL/occurrences-report.csv \
 --max-cardinality 100 \
 --heaviest-first

python3 scripts/partition_migration_data.py \
 --adapter boranga.components.data_migration.adapters.occurrence_report.tpfl.OccurrenceReportTpflAdapter \
 --input private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv \
 --output private-media/legacy_data/TPFL/occurrence-reports-partitioned.csv \
 --report private-media/legacy_data/TPFL/occurrence-reports-report.csv \
 --max-cardinality 100 \
 --heaviest-first
