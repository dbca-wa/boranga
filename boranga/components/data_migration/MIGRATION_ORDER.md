# --- TPFL

## Species

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
