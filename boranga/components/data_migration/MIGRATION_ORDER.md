# --- TPFL

# --- Performance Optimization

# Create the functional index on lower(scientific_name) to speed up all name-based lookups

./manage.py taxonomy_index --ensure --yes

## Populate required mappings

./manage.py populate_legacy_username_map private-media/legacy_data/TPFL/legacy-username-emailuser-map-TPFL.csv --legacy-system TPFL
./manage.py populate_legacy_value_map private-media/legacy_data/TPFL/legacy-data-map-TPFL.csv --legacy-system TPFL --update
./manage.py populate_legacy_taxonomy_mapping.py private-media/legacy_data/TPFL/legacy-species-names-mapped-Nomos-ID-TPFL.csv --filter-list-name "TPFL Species" --list-name TPFL
./manage.py populate_legacy_taxonomy_mapping.py private-media/legacy_data/TPFL/legacy-species-names-mapped-Nomos-ID-TPFL.csv --filter-list-name "TPFL AssociatedSpecies"

## Species

python scripts/combine_csvs.py \
 private-media/legacy_data/TPFL/DRF_TAXON_CONSV_LISTINGS.csv \
 private-media/legacy_data/TPFL/ADDITIONAL_PROFILES_FROM_OLD_NAMES_OCC_NAMES_Sanitised.csv \
 -o private-media/legacy_data/TPFL/species-profiles-combined.csv

./manage.py migrate_data run species_legacy private-media/legacy_data/TPFL/DRF_TAXON_CONSV_LISTINGS.csv --sources TPFL --wipe-targets
./manage.py migrate_data run species_legacy private-media/legacy_data/TPFL/ADDITIONAL_PROFILES_FROM_OLD_NAMES_OCC_NAMES.csv --sources TPFL

## Conservation Status

./manage.py migrate_data run conservation_status_legacy private-media/legacy_data/TPFL/SAMPLE_CS_Data_Dec2025.csv --sources TPFL

## Occurrences

./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL
./manage.py migrate_data run occurrence_documents_legacy private-media/legacy_data/TPFL/DRF_LIAISONS.csv --sources TPFL
./manage.py migrate_data run occurrence_threats_legacy private-media/legacy_data/TPFL/DRF_POP_THREATS.csv --sources TPFL

## Occurrence Reports

./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv --sources TPFL
./manage.py migrate_data run occurrence_report_documents_legacy private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv --sources TPFL
./manage.py migrate_data run occurrence_report_threats_legacy private-media/legacy_data/TPFL/DRF_SHEET_THREATS.csv --sources TPFL

# The occurrence geometries are copied from the occurrence reports so this run must be done after the occurrence reports

./manage.py migrate_data run occurrence_tenure private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL

# --- TEC

## Communities

./manage.py migrate_data run communities_legacy private-media/legacy_data/TEC/COMMUNITIES.csv --sources TEC --wipe-targets

## Conservation Status

./manage.py migrate_data run conservation_status_legacy private-media/legacy_data/TEC/[TBC].csv --sources TEC

## Occurrences

./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TEC/ --sources TEC
./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TEC/TEC_PEC_Boundaries_Nov25.csv --sources TEC_BOUNDARIES

## Occurrence Reports

./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TEC/SITES.csv --sources TEC_SITES
./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TEC/SURVEYS.csv --sources TEC_SURVEYS

# --- Cleanup

# Drop the functional index now that migrations are complete

./manage.py taxonomy_index --drop --yes
