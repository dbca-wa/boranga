# Unattended Migration Runs

Each section below is a single copy-paste block for an unattended full run of one system.
Output is appended to a dated log file on the PVC.

---

## TPFL — Full Run

```bash
nohup sh -c '
set -e
echo "=== TPFL FULL RUN START $(date) ==="

echo "--- taxonomy_index --ensure ---"
./manage.py taxonomy_index --ensure --yes

echo "--- populate_legacy_username_map ---"
./manage.py populate_legacy_username_map private-media/legacy_data/TPFL/legacy-username-emailuser-map-TPFL.csv --legacy-system TPFL --update

echo "--- populate_legacy_value_map ---"
./manage.py populate_legacy_value_map private-media/legacy_data/TPFL/legacy-data-map-TPFL.csv --legacy-system TPFL --update

echo "--- populate_legacy_taxonomy_mapping (Species) ---"
./manage.py populate_legacy_taxonomy_mapping private-media/legacy_data/TPFL/legacy-species-names-mapped-Nomos-ID-TPFL.csv --filter-list-name "TPFL Species" --list-name TPFL

echo "--- populate_legacy_taxonomy_mapping (AssociatedSpecies) ---"
./manage.py populate_legacy_taxonomy_mapping private-media/legacy_data/TPFL/legacy-species-names-mapped-Nomos-ID-TPFL.csv --filter-list-name "TPFL AssociatedSpecies"

echo "--- species_legacy (DRF_TAXON_CONSV_LISTINGS) ---"
./manage.py migrate_data run species_legacy private-media/legacy_data/TPFL/DRF_TAXON_CONSV_LISTINGS.csv --sources TPFL --wipe-targets --seed-history

echo "--- species_legacy (ADDITIONAL_PROFILES) ---"
./manage.py migrate_data run species_legacy private-media/legacy_data/TPFL/ADDITIONAL_PROFILES_FROM_OLD_NAMES_OCC_NAMES.csv --sources TPFL --seed-history

echo "--- conservation_status_legacy ---"
./manage.py migrate_data run conservation_status_legacy private-media/legacy_data/TPFL/SAMPLE_CS_Data_Dec2025.csv --sources TPFL --wipe-targets --seed-history

echo "--- occurrence_legacy ---"
./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL --wipe-targets --seed-history

echo "--- occurrence_documents_legacy ---"
./manage.py migrate_data run occurrence_documents_legacy private-media/legacy_data/TPFL/DRF_LIAISONS.csv --sources TPFL --wipe-targets --seed-history

echo "--- occurrence_threats_legacy ---"
./manage.py migrate_data run occurrence_threats_legacy private-media/legacy_data/TPFL/DRF_POP_THREATS.csv --sources TPFL --wipe-targets --seed-history

echo "--- occurrence_report_legacy + occurrence_report_documents_legacy (runmany, same file) ---"
./manage.py migrate_data runmany private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv --only occurrence_report_legacy occurrence_report_documents_legacy --sources TPFL --wipe-targets --seed-history

echo "--- occurrence_report_threats_legacy ---"
./manage.py migrate_data run occurrence_report_threats_legacy private-media/legacy_data/TPFL/DRF_SHEET_THREATS.csv --sources TPFL --wipe-targets --seed-history

echo "--- occurrence_tenure ---"
./manage.py migrate_data run occurrence_tenure private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL --wipe-targets --seed-history

echo "=== TPFL FULL RUN DONE $(date) ==="
' >> /app/private-media/migration-tpfl-unattended.log 2>&1 &
disown
echo "TPFL run queued. Tail log with: tail -f /app/private-media/migration-tpfl-unattended.log"
```

---

## TEC — Full Run

> Note: Update `[TBC].csv` with the actual conservation status filename when available.

```bash
nohup sh -c '
set -e
echo "=== TEC FULL RUN START $(date) ==="

echo "--- communities_legacy ---"
./manage.py migrate_data run communities_legacy private-media/legacy_data/TEC/COMMUNITIES.csv --sources TEC --wipe-targets --seed-history

echo "--- conservation_status_legacy ---"
./manage.py migrate_data run conservation_status_legacy private-media/legacy_data/TEC/[TBC].csv --sources TEC --wipe-targets --seed-history

echo "--- occurrence_legacy ---"
./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TEC/ --sources TEC --wipe-targets --seed-history

echo "--- occurrence_legacy (TEC_BOUNDARIES, no wipe) ---"
./manage.py migrate_data run occurrence_legacy private-media/legacy_data/TEC/TEC_PEC_Boundaries_Nov25.csv --sources TEC_BOUNDARIES --seed-history

echo "--- occurrence_report_legacy (SITE_VISITS) ---"
./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TEC/SITE_VISITS.csv --sources TEC_SITE_VISITS --wipe-targets --seed-history

echo "--- occurrence_report_legacy (SURVEYS, no wipe) ---"
./manage.py migrate_data run occurrence_report_legacy private-media/legacy_data/TEC/SURVEYS.csv --sources TEC_SURVEYS --seed-history

echo "--- occurrence_report_threats_legacy ---"
./manage.py migrate_data run occurrence_report_threats_legacy private-media/legacy_data/TEC/SURVEY_THREATS.csv --sources TEC_SURVEY_THREATS --wipe-targets --seed-history

echo "--- associated_species ---"
./manage.py migrate_data run associated_species private-media/legacy_data/TEC/SITE_SPECIES.csv --sources TEC_SITE_SPECIES --wipe-targets --seed-history

echo "=== TEC FULL RUN DONE $(date) ==="
' >> /app/private-media/migration-tec-unattended.log 2>&1 &
disown
echo "TEC run queued. Tail log with: tail -f /app/private-media/migration-tec-unattended.log"
```

---

## TFAUNA — Full Run

> Note: Update `[TBC].csv` with the actual conservation status filename when available.

```bash
nohup sh -c '
set -e
echo "=== TFAUNA FULL RUN START $(date) ==="

echo "--- species_legacy ---"
./manage.py migrate_data run species_legacy "private-media/legacy_data/TFAUNA/Species List.csv" --sources TFAUNA --wipe-targets --seed-history

echo "--- conservation_status_legacy ---"
./manage.py migrate_data run conservation_status_legacy private-media/legacy_data/TFAUNA/[TBC].csv --sources TFAUNA --wipe-targets --seed-history

echo "--- occurrence_report_legacy ---"
./manage.py migrate_data run occurrence_report_legacy "private-media/legacy_data/TFAUNA/Fauna Records.csv" --sources TFAUNA --wipe-targets --seed-history

echo "=== TFAUNA FULL RUN DONE $(date) ==="
' >> /app/private-media/migration-tfauna-unattended.log 2>&1 &
disown
echo "TFAUNA run queued. Tail log with: tail -f /app/private-media/migration-tfauna-unattended.log"
```

---

## Cleanup — Drop taxonomy index

Run this **after all systems are complete**:

```bash
./manage.py taxonomy_index --drop --yes
```
