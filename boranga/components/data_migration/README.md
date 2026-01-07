# Data Migration Framework

This folder contains the architecture used to import legacy spreadsheets into the Django models in this project.

It is designed to be modular to separate:

- **Adapters**: Source-specific extraction (files -> canonical rows).
- **Schemas**: Definitions of expected columns, requirements, and transformation pipelines.
- **Handlers (Importers)**: Orchestrators that run the migration logic.
- **Transforms**: Reusable functions to clean/convert data.

## Core Components

### 1. Registry (`registry.py`)

The central registry for helper functions and transforms.

- **`TransformRegistry`**: Stores named transform functions (e.g., `'strip'`, `'to_int'`, `'date_iso'`).
- **`build_pipeline(names)`**: Converts a list of transform names into a callable pipeline.
- **`run_pipeline(pipeline, value, ctx)`**: Executes a pipeline on a value. returns `TransformResult`.
- **`BaseSheetImporter`**: Abstract base class for all Importers (Handlers). provides common structure.
- **`TransformContext`**: Dataclass passed to transforms (access to parameters like `row`, `user_id`).

### 2. Schema (`adapters/schema_base.py`)

Defines the structure of the input data.

- **`Schema` dataclass**:
  - `column_map`: Maps raw CSV/Excel headers to internal canonical keys.
  - `required`: List of keys that must be present.
  - **`pipelines`**: Dictionary mapping canonical keys to a list of transform names (e.g. `["strip", "to_int"]`).
  - **`child_specs`**: List of `ChildExpansionSpec` for processing 1-to-many data (row expansion).

### 3. Mappings (`mappings.py`)

Helpers for legacy value lookups.

- **`LegacyValueMap`**: Django model mapping (Legacy System, List Name, Legacy Value) -> (Boranga Value/PK).
- **`preload_map`**: Loads mappings into memory caches to prevent N+1 DB queries.
- **`load_legacy_to_pk_map`**: returns a simple dict `{"legacy_val": pk}`.

### 4. Adapters (`adapters/`)

Responsible for reading files and producing "Canonical Rows".

- **`SourceAdapter`** (`adapters/base.py`): Base class.
- **`read_table(path)`**: Helper to read CSV or Excel files (uses `pandas`).
- **`extract(path)`**: Main method to implement. Must return `ExtractionResult(rows, warnings)`.

### 5. Row Expansion (`row_expansion.py`)

Utilities to turn "wide" rows (spreadsheet style) into "deep" structures (objects with children).

- **`expand_children(schema, row)`**: Iterates `child_specs` to extracting nested lists.
  - _Regex Pattern_: e.g. `doc_1_title`, `doc_2_title` -> `docs: [{title: ...}, {title: ...}]`.
  - _Split Column_: e.g. `tags: "A;B;C"` -> `tags: [{tag: A}, {tag: B}, ...]`.

### 6. Handlers / Importers (`handlers/`)

The classes that run the import.

- Located in `handlers/`.
- Must inherit **`BaseSheetImporter`** and use `@register` decorator.
- **`run(path, ctx)`**: The entry point called by the management command.
  - Coordinates Adapters, Transformation, Expansion, and Database persistence.
- **`clear_targets(ctx)`**: Optional hook to delete existing data before import (invoked by `--wipe-targets`).

## Workflow: Creating a New Import

To add support for a new entity or source:

1.  **Define Source (Optional)**: If it's a new system, add to `Source` enum in `adapters/sources.py`.
2.  **Create Adapter**:
    - In `adapters/<entity>/<source>.py`.
    - Inherit `SourceAdapter`. Implement `extract()`.
    - Use `read_table()` for easy file reading.
    - Map raw headers to valid canonical keys expected by your schema.
3.  **Define Schema**:
    - In `adapters/<entity>/schema.py`.
    - Define `COLUMN_MAP`, `REQUIRED_FIELDS`, and `PIPELINES`.
    - Define `ChildExpansionSpec` if you have repeated columns (documents, etc).
4.  **Create Handler**:
    - In `handlers/<entity>.py`.
    - Decorate with `@register` and inherit `BaseSheetImporter`.
    - Implement `run()`.
      - Get rows from Adapter.
      - Validate headers (`schema.validate_headers`).
      - Loop rows:
        - `run_pipeline` for each field.
        - `expand_children`.
        - **Persist**: Create/Update Django models. **Respect `ctx.dry_run`**.
    - (Optional) Implement `clear_targets(ctx)` to support `--wipe-targets`.

## Registered Handlers and Source Files

Use this reference to identify which file to pass to each handler. Paths are typically relative to `private-media/legacy_data/`.

### Source: TPFL

| Slug                               | Typical Source File                                                       | Description                                 |
| :--------------------------------- | :------------------------------------------------------------------------ | :------------------------------------------ |
| `species_legacy`                   | `TPFL/DRF_TAXON_CONSV_LISTINGS.csv`                                       | Species (Taxa)                              |
| `conservation_status_legacy`       | `TPFL/SAMPLE_CS_Data_Dec2025.csv`<br>_(Note: filename subject to change)_ | Conservation Status                         |
| `occurrence_legacy`                | `TPFL/DRF_POPULATION.csv`                                                 | Occurrences (Populations)                   |
| `occurrence_documents_legacy`      | `TPFL/DRF_LIAISONS.csv`                                                   | Occurrence Documents (Liaisons)             |
| `occurrence_threats_legacy`        | `TPFL/DRF_POP_THREATS.csv`                                                | Occurrence Threats (SHEETNO=Null)           |
| `occurrence_tenure`                | `TPFL/DRF_POPULATION.csv`                                                 | Occurrence Tenure (and spatial calculation) |
| `occurrence_report_legacy`         | `TPFL/DRF_RFR_FORMS.csv`                                                  | Occurrence Reports                          |
| `occurrence_report_documents`      | `TPFL/DRF_RFR_FORMS.csv`                                                  | Report Documents                            |
| `occurrence_report_threats_legacy` | `TPFL/DRF_SHEET_THREATS.csv`                                              | Report Threats                              |

### Source: TEC

| Slug                 | Typical Source File   | Description                                                         |
| :------------------- | :-------------------- | :------------------------------------------------------------------ |
| `communities_legacy` | `TEC/COMMUNITIES.csv` | Communities                                                         |
| `occurrence_legacy`  | `TEC/` (Folder)       | Pass the folder path containing `OCCURRENCES.csv`, `SITES.csv` etc. |

### Source: TFAUNA

| Slug | Typical Source File | Description |
| :--- | :------------------ | :---------- |
|      |                     |             |

## Management Command

`boranga/management/commands/migrate_data.py`

- `python manage.py migrate_data list`
- `python manage.py migrate_data run <slug> <path> [--dry-run] [--limit N] [--wipe-targets] [--sources SRC1 SRC2]`
- `python manage.py migrate_data runmany <path> [--only slug1 slug2] [--wipe-targets] [--sources SRC1 SRC2]`
- `slug`: The value defined in your Handler class `slug` attribute.
- `--sources`: Optional subset of sources to import (e.g. `TPFL TEC`). Useful when a handler supports multiple sources.
- `--wipe-targets`: **WARNING** - Deletes existing data. Requires handler to implement `clear_targets()`.

## Specific Implementation Patterns

### Adding OneToOne relationships (child models) to an importer

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
   - In ops collection loop: Extract child data into `child_data = {}` dict from merged dict with `ChildModel__` prefix keys
   - In ops.append() call: Add `"child_data": child_data` to the ops dict
   - In to_update loop unpacking: **CRITICAL** - Unpack child_data from the tuple, e.g., `(inst, habitat_data, habitat_condition, submitter_information_data, location_data) = up`
   - In create_meta.append() call: Add child_data to the tuple being appended
   - In create_meta for loop unpacking: **CRITICAL** - Unpack child_data from create_meta, e.g., `for (mig, habitat_data, habitat_condition, submitter_information_data, location_data) in create_meta:`
   - After identifying existing child instances: Build create/update lists and bulk_create/bulk_update them (follow pattern from OCRHabitatComposition, OCRIdentification, etc.)

**Common mistakes to avoid:**

- Forgetting to copy raw values to canonical dict in adapter.extract() → child fields won't reach pipelines
- Forgetting to unpack child_data in to_update loop or create_meta loop → ValueError: too many values to unpack
- Forgetting to add child_data to ops.append() or create_meta.append() → child data is lost before reaching persistence code
  model instances created but not persisted to database

**VALIDATION**: After adding a new child model, run the tuple unpacking validator to catch any mismatches before testing:

```bash
# For occurrence_reports.py (default)
python boranga/components/data_migration/validate_tuple_unpacking.py

# For other handlers
python boranga/components/data_migration/validate_tuple_unpacking.py <handler_name_or_path>
```

This script detects when tuple structures change (new fields added) but not all unpacking locations are updated. Works with any handler that uses tuple unpacking. See `TUPLE_UNPACKING_VALIDATION.md` for detailed information about all unpacking locations and manual verification steps.

## Debugging tips

- Use `--dry-run` to validate transformations without writing DB changes.
- Check transform issues returned by `run_pipeline` for field-level problems.
- If a migration or lookup fails, ensure the related lookup tables (e.g. `LegacyValueMap`) have the expected data.
- If you rename migration files after applying them, the migration history in `django_migrations` will not match — avoid renaming applied migrations.
