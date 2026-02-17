# --- TPFL

## Prepare files

1. Download latest extract from P383 - MIgration > Shared > TPFL Extract folder on teams/sharepoint (e.g. DRF-2025-11-04.zip).
2. Copy into Documents\Boranga\data-migration\test-data\TPFL\<YYYYMMDD> (today's date)
3. Extract to a folder of the same name i.e. DRF-2025-11-04
4. Download folder P383 - Migration > Ready to Migrate > Flora
5. Copy to the Documents\Boranga\data-migration\test-data\TPFL\<YYYYMMDD> folder
6. Extract to a folder of the same name i.e. Flora
7. Copy only the .csv files from the Flora folder (the files may be in a subfolder that is also called Flora)
8. Paste them into the folder within DRF-2025-11-04 (in this case it's called 'DRF-2025-11-04T09_10_43_3'), Select overwrite when prompted
9. compress that folder 'DRF-2025-11-04T09_10_43_3' into a .tar.gz file rename the file to include the group type and today's date e.g. tpfl-20260130.tar.gz
10. Open 'Microsoft Azure Storage Explorer' and navigate to dbcaoimdevtransfer > Blob Containers > transfer > <YourNAME>
11. Copy the .tar.gz file into the folder
12. Open the rancher environment being used for data verification (in this case UAT)
13. Navigate to the Deployment being used (in this case 'boranga-dev')
14. Open a shell and navigate to the folder that will be housing the files (e.g. private-media/legacy_data/TPFL)
15. If there are existing files in the folder, move them to an archive folder:

TIMESTAMP=TPFL*$(date +%Y%m%d*%H%M%S)\_archived; mkdir "$TIMESTAMP"

You will get 'mkdir: cannot set permissions '20260130_113630_archived': Permission denied' however it still creates the folder

mv \* "$TIMESTAMP"/

It will tell you: cannot move '<timestamp_archived>' to a subdirectory of itself. (harmless and expected.)

16. Move the archived folder up one folder so it doesn't get archived again in future:

mv TPFL_20260130_113630_archived ..

16. run 'azcopy login', copy the code and visit the link to authenticate.
17. run 'azcopy list https://dbcaoimdevtransfer.blob.core.windows.net/transfer/<YouName>' to confirm the file is present
18. run 'azcopy copy https://dbcaoimdevtransfer.blob.core.windows.net/transfer/<YourName>/tpfl-20260130.tar.gz .'
19. run 'tar -xzvf tpfl-20260130.tar.gz', it will give warnings 'Cannot utime: Operation not permitted'
20. It's likely the files will be in a sub folder so move all the files up one folder and remove the sub folder:

mv DRF-2025-11-04T09_10_43_3/\* . && rmdir DRF-2025-11-04T09_10_43_3

21. Remove the .tar.gz file, run 'rm tpfl-20260130.tar.gz'
22. Now the folder private-media/legacy_data/TPFL (for example) should have all the files needed for the following migration steps.
23. Return home: cd ~

# --- Performance Optimization

# Create the functional index on lower(scientific_name) to speed up all name-based lookups

./manage.py taxonomy_index --ensure --yes

## Populate required mappings

./manage.py populate_legacy_username_map private-media/legacy_data/TPFL/legacy-username-emailuser-map-TPFL.csv --legacy-system TPFL --update
./manage.py populate_legacy_value_map private-media/legacy_data/TPFL/legacy-data-map-TPFL.csv --legacy-system TPFL --update
./manage.py populate_legacy_taxonomy_mapping private-media/legacy_data/TPFL/legacy-species-names-mapped-Nomos-ID-TPFL.csv --filter-list-name "TPFL Species" --list-name TPFL
./manage.py populate_legacy_taxonomy_mapping private-media/legacy_data/TPFL/legacy-species-names-mapped-Nomos-ID-TPFL.csv --filter-list-name "TPFL AssociatedSpecies"

## Species

python scripts/combine_csvs.py \
 private-media/legacy_data/TPFL/DRF_TAXON_CONSV_LISTINGS.csv \
 private-media/legacy_data/TPFL/ADDITIONAL_PROFILES_FROM_OLD_NAMES_OCC_NAMES.csv \
 -o private-media/legacy_data/TPFL/species-profiles-combined.csv

./manage.py migrate_data run species_legacy private-media/legacy_data/TPFL/DRF_TAXON_CONSV_LISTINGS.csv --sources TPFL --wipe-targets
./manage.py migrate_data run species_legacy private-media/legacy_data/TPFL/ADDITIONAL_PROFILES_FROM_OLD_NAMES_OCC_NAMES.csv --sources TPFL

or, if combined into one file:

./manage.py migrate_data run species_legacy private-media/legacy_data/TPFL/species-profiles-combined.csv --sources TPFL --wipe-targets

## Conservation Status

./manage.py migrate_data run conservation_status_legacy private-media/legacy_data/TPFL/SAMPLE_CS_Data_Dec2025.csv --sources TPFL --wipe-targets

## Occurrences

./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL --wipe-targets
./manage.py migrate_data run occurrence_documents_legacy private-media/legacy_data/TPFL/DRF_LIAISONS.csv --sources TPFL --wipe-targets
./manage.py migrate_data run occurrence_threats_legacy private-media/legacy_data/TPFL/DRF_POP_THREATS.csv --sources TPFL --wipe-targets

## Occurrence Reports

./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv --sources TPFL --wipe-targets
./manage.py migrate_data run occurrence_report_documents_legacy private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv --sources TPFL --wipe-targets
./manage.py migrate_data run occurrence_report_threats_legacy private-media/legacy_data/TPFL/DRF_SHEET_THREATS.csv --sources TPFL --wipe-targets

# The occurrence geometries are copied from the occurrence reports so this run must be done after the occurrence reports

./manage.py migrate_data run occurrence_tenure private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL --wipe-targets

# --- TEC

## Communities

./manage.py migrate_data run communities_legacy private-media/legacy_data/TEC/COMMUNITIES.csv --sources TEC --wipe-targets

## Conservation Status

./manage.py migrate_data run conservation_status_legacy private-media/legacy_data/TEC/[TBC].csv --sources TEC --wipe-targets

## Occurrences

./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TEC/ --sources TEC --wipe-targets

# Don't wipe targets for the boundaries run as the Occurrence Geometry records are created in the previous run and contain important data (wipe targets is disabled in the handler anyway as a precaution)

./manage.py migrate_data run occurrence_legacy private-media/
legacy_data/TEC/TEC_PEC_Boundaries_Nov25.csv --sources TEC_BOUNDARIES

## Occurrence Reports

./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TEC/SITE_VISITS.csv --sources TEC_SITE_VISITS --wipe-targets

# Don't wipe targets for this one either as it would delete the site visit OCRS that were created in the prior run

./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TEC/SURVEYS.csv --sources TEC_SURVEYS

## Occurrence Report Threats

./manage.py migrate_data run occurrence_report_threats_legacy private-media/legacy_data/TEC/SURVEY_THREATS.csv --sources TEC_SURVEY_THREATS --wipe-targets

## Associated Species (from structured SITE_SPECIES.csv)

./manage.py migrate_data run associated_species private-media/legacy_data/TEC/SITE_SPECIES.csv --sources TEC_SITE_SPECIES --wipe-targets

# -- TFAUNA

## Species

./manage.py migrate_data run species_legacy private-media/legacy_data/TFAUNA/Species List.csv --sources TFAUNA --wipe-targets

# --- Cleanup

# Drop the functional index now that migrations are complete

./manage.py taxonomy_index --drop --yes
