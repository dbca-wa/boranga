# Data Migration - Overview

This folder contains the architecture used to import legacy spreadsheets into the Django models in this project.

It is designed to be modular so you can add:

- New adapters (for different sources/formats)
- Importers (handlers for specific domain objects)
- Reusable transforms (validation / mapping functions).

This README explains the pieces and the typical workflow in simple language.

## Core concepts

- Registry (boranga/components/data_migration/registry.py)

  - Holds reusable transform functions (e.g. strip, to_int, fk_lookup, choice validators).
  - Exposes `run_pipeline` to execute a pipeline (list of transforms) on a value.
  - Provides helpers to build results with issues (warnings/errors).

- Schema (adapters/\*/schema.py and schema_base.Schema)

  - Defines how raw spreadsheet headers map to canonical column names: `column_map`.
  - Declares which canonical fields are required.
  - Describes transformation pipelines per field (names that refer to registered transforms).
  - Supports child expansion specs (for repeating groups / multi-row expansions).

- Mappings (mappings.py)

  - Helpers for mapping legacy enumerated values to current database objects (e.g. `build_legacy_map_transform`).
  - Caches LegacyValueMap lookups to avoid repeated DB queries.

- Row expansion (row_expansion.py)

  - Converts wide spreadsheet rows with indexed column names into a parent row plus list(s) of child rows. For example, document columns in a sheet can be expanded into multiple OccurrenceDocument instances: columns like `doc_1_file`, `doc_1_category`, `doc_1_visible`, `doc_2_file`, `doc_2_category`, ... are turned into a list of document dicts:
    - [{"file": ..., "category": ..., "visible": ...}, {"file": ..., "category": ..., "visible": ...}, ...]
  - The importer then iterates that list and creates one OccurrenceDocument model instance per child dict.
  - Also supports splitting multi-valued columns (e.g. semicolon-separated tags).

- Adapters (adapters/)

  - Source-specific logic to parse a file (or detect file type) and produce canonical raw rows.
  - Implement `SourceAdapter` (extract -> returns rows + warnings).
  - Example: `adapters/occurrence/tpfl.py` maps TPFL spreadsheets into canonical fields expected by the importer.

- Importers / Handlers (components/data_migration/handlers/)

  - High-level classes (registered via the registry) that orchestrate an import:
    - Validate headers via Schema
    - Expand child rows
    - Run field pipelines (validate / convert values)
    - Create/update Django models (usually inside DB transactions)
  - Example: `handlers/occurrences.py` contains the occurrence importer.

- Management command
  - `boranga/management/commands/migrate_data.py` exposes:
    - `list` — list available importers
    - `run <slug> <path> [--dry-run]` — run one importer
    - `runmany <path> [--only <slugs>] [--dry-run]` — run multiple importers sequentially

## Typical workflow

1. Adapter reads spreadsheet and emits canonical raw rows.
2. Schema maps raw headers to canonical keys and reports missing required fields.
3. `expand_children` splits repeating or multi-value columns into child lists.
4. For each row (and child row), the importer runs the pipeline for each field using `run_pipeline`.
5. The importer applies the results to the Django models inside a transaction.
6. Warnings and issues are collected and surfaced to the user / logs. Use `--dry-run` to validate without persisting.

## How to add support for a new source / data type

1. Add a Source enum value in `adapters/sources.py` (if needed).
2. Create an Adapter implementing `SourceAdapter.extract()` that parses the file and returns canonical rows.
3. Create a Schema describing `column_map`, `required`, and `pipelines` for the importer.
4. Add any new transforms to the registry (`registry.register`) and reference them in pipelines.
5. Add or register an Importer (in `handlers/`) that:
   - Declares a `slug` and `description`
   - Calls the adapter and runs the import logic
   - Register the importer with the registry (so the command can find it)

## Adding OneToOne relationships (child models) to an importer

When adding a new OneToOne child model to an importer (e.g., OCRLocation to OccurrenceReport), follow this pattern to avoid common mistakes:

1. **Schema** (schema.py):

   - Add COLUMN_MAP entries to map raw CSV columns to `ChildModel__field_name` keys
   - Add fields to the OccurrenceReportRow dataclass for each child field
   - Update from_dict() to coerce/validate child fields using utils.to_int_maybe(), utils.safe_strip(), etc.

2. **Adapter** (tpfl.py):

   - Add transform functions using build_legacy_map_transform() for FK lookups, dependent_from_column_factory() for derived fields, static_value_factory() for defaults
   - Add entries to PIPELINES dict mapping `ChildModel__field_name` to their transform pipelines
   - **CRITICAL**: In extract() method, copy raw CSV column values into the canonical dict with `ChildModel__` prefix (e.g., `canonical["ChildModel__field"] = canonical.get("RAW_CSV_COLUMN")`) so pipelines can process them. This mirrors how habitat/identification fields are handled.

3. **Handler** (occurrence_reports.py):
   - In ops collection loop (around line 536): Extract child data into `child_data = {}` dict from merged dict with `ChildModel__` prefix keys
   - In ops.append() call: Add `"child_data": child_data` to the ops dict
   - In to_update loop unpacking (around line 1200+): **CRITICAL** - Unpack child_data from the tuple, e.g., `(inst, habitat_data, habitat_condition, submitter_information_data, location_data) = up`
   - In create_meta.append() call: Add child_data to the tuple being appended
   - In create_meta for loop unpacking (around line 1323): **CRITICAL** - Unpack child_data from create_meta, e.g., `for (mig, habitat_data, habitat_condition, submitter_information_data, location_data) in create_meta:`
   - After identifying existing child instances: Build create/update lists and bulk_create/bulk_update them (follow pattern from OCRHabitatComposition, OCRIdentification, etc.)

**Common mistakes to avoid:**

- Forgetting to copy raw values to canonical dict in adapter.extract() → child fields won't reach pipelines
- Forgetting to unpack child_data in to_update loop or create_meta loop → ValueError: too many values to unpack
- Forgetting to add child_data to ops.append() or create_meta.append() → child data is lost before reaching persistence code
- Not adding bulk_create/bulk_update code for the child model → model instances created but not persisted to database

## Running imports

- List importers:
  - `python manage.py migrate_data list`
- Run a single importer:
  - `python manage.py migrate_data run <slug> <spreadsheet-path> [--dry-run]`
- Run many (shared spreadsheet):
  - `python manage.py migrate_data runmany <path> [--only slug1 slug2] [--dry-run]`

## Debugging tips

- Use `--dry-run` to validate transformations without writing DB changes.
- Check transform issues returned by `run_pipeline` for field-level problems.
- If a migration or lookup fails, ensure the related lookup tables (e.g. `LegacyValueMap`) have the expected data.
- If you rename migration files after applying them, the migration history in `django_migrations` will not match — avoid renaming applied migrations.

## Where to look next

- registry.py — transform implementations and pipeline runner
- mappings.py — legacy lookup helpers and cache
- adapters/\* — parsing logic for each source
- handlers/\* — import orchestration and model creation
- row_expansion.py — expanding repeating columns into child rows
- migrate_data.py — CLI entrypoint

Keep schemas, adapters and transforms simple and well tested. Small, composable transforms make mapping and debugging much easier.
