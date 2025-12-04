# Tuple Unpacking Validation Guide

## Problem

When adding OneToOne child relationships to the occurrence_report_legacy importer, data is packed into tuples that are later unpacked in multiple locations. If tuple structure changes (new child model added), ALL unpacking locations must be updated consistently, or the code will fail with `ValueError: too many values to unpack`.

This happened in the OCRLocation implementation: the tuple was updated from 4 elements to 5, but only 2 of 3 unpacking locations were fixed, causing the code to fail when the bulk update path was tested.

## Current Tuple Structure

As of the OCRLocation implementation, child data tuples contain **5 elements**:

```python
(
    migrated_from_id,           # Only in create_meta
    habitat_data,               # In both to_update and create_meta
    habitat_condition,          # In both to_update and create_meta
    submitter_information_data, # In both to_update and create_meta
    location_data,              # In both to_update and create_meta
)
```

OR (for to_update only, with OccurrenceReport instance instead of migrated_from_id):

```python
(
    inst,                       # OccurrenceReport instance
    habitat_data,
    habitat_condition,
    submitter_information_data,
    location_data,
)
```

## All Unpacking Locations (MUST be kept in sync)

### 1. **to_update appending** (Line ~585)
**File:** `occurrence_reports.py`
**What:** Building the to_update tuple for instances with existing migrated_from_id

```python
to_update.append(
    (
        obj,
        habitat_data,
        habitat_condition,
        submitter_information_data,
        location_data,
    )
)
```

✅ **Currently correct (5 elements)**

### 2. **create_meta appending** (Line ~606)
**File:** `occurrence_reports.py`
**What:** Building the create_meta tuple for new instances

```python
create_meta.append(
    (
        migrated_from_id,
        habitat_data,
        habitat_condition,
        submitter_information_data,
        location_data,
    )
)
```

✅ **Currently correct (5 elements)**

### 3. **to_update unpacking - Habitat/Condition/SubmitterInfo section** (Line ~1264)
**File:** `occurrence_reports.py`
**What:** Processing habitat, condition, and submitter info for updates

```python
for up in to_update:
    (
        inst,
        habitat_data,
        habitat_condition,
        submitter_information_data,
        location_data,
    ) = up
```

✅ **Currently correct (5 elements)**

### 4. **to_update unpacking - Habitat/Condition/Identification/Location section** (Line ~1605)
**File:** `occurrence_reports.py`
**What:** Processing habitat, condition, identification, and location for updates

```python
for up in to_update:
    (
        inst,
        habitat_data,
        habitat_condition,
        submitter_information_data,
        location_data,
    ) = up
```

✅ **Currently correct (5 elements)**

### 5. **create_meta unpacking - Handle created ones section** (Line ~1726)
**File:** `occurrence_reports.py`
**What:** Processing habitat, condition, identification, and location for newly created records

```python
for (
    mig,
    habitat_data,
    habitat_condition,
    submitter_information_data,
    location_data,
) in create_meta:
```

✅ **Currently correct (5 elements)**

## Validation Checklist for Adding New Child Models

When adding a new OneToOne child model (e.g., OCRNewThing) to the importer, follow this checklist:

- [ ] **Schema** (`schema.py`): Add 6 fields to COLUMN_MAP and OccurrenceReportRow dataclass
- [ ] **Adapter** (`tpfl.py`): Add transforms and PIPELINES entries; add explicit field copying in extract()
- [ ] **Handler** (`occurrence_reports.py`):
  - [ ] Extract `newthing_data` from merged dict in main loop (around line 575)
  - [ ] Add `newthing_data` to ops.append() dict (around line 542)
  - [ ] **Add `newthing_data` to all tuple structures:**
    - [ ] to_update.append() - Line ~585 (LOCATION 1)
    - [ ] create_meta.append() - Line ~606 (LOCATION 2)
  - [ ] **Update all unpacking locations to 6 elements:**
    - [ ] to_update unpacking (first habitat loop) - Line ~1264 (LOCATION 3)
    - [ ] to_update unpacking (second habitat loop) - Line ~1605 (LOCATION 4)
    - [ ] create_meta unpacking - Line ~1726 (LOCATION 5)
  - [ ] Add bulk_create and bulk_update logic for new model

## How to Verify All Locations Are Updated

Run this grep command to find all unpacking locations:

```bash
grep -n "for.*in (to_update|create_meta):" boranga/components/data_migration/handlers/occurrence_reports.py
```

Should show exactly 3 lines (two to_update loops + one create_meta loop):
- Line ~1264: `for up in to_update:` (habitat/condition/submitter)
- Line ~1605: `for up in to_update:` (habitat/condition/ident/location)
- Line ~1726: `for (mig, ...` (create_meta loop)

For each match, verify the unpacking includes all expected fields by reading the next 10 lines.

## Testing Strategy

After making changes:

1. **Test with `--wipe-targets`** (creates only):
   ```bash
   ./manage.py migrate_data run occurrence_report_legacy \
     private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv \
     --limit 5 --wipe-targets
   ```

2. **Test without `--wipe-targets`** (creates + updates):
   ```bash
   ./manage.py migrate_data run occurrence_report_legacy \
     private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv \
     --limit 5
   ```

Both must pass. If the second fails with `ValueError: too many values to unpack`, check Location 3 and 4 (to_update unpacking).

## History

- **2025-12-04**: Added OCRLocation (5 elements). Fixed missing unpacking at Location 3 after discovering error during update path testing.
