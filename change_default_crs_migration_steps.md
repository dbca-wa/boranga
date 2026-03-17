# CRS Migration Guide

## Changing the Default CRS

The Boranga application uses a single setting `DEFAULT_SRID` (in `boranga/settings.py`)
to control the coordinate reference system for all geometry storage and display.

### Steps to change the CRS

1. **Update the setting** in `boranga/settings.py`:

   ```python
   DEFAULT_SRID = config("DEFAULT_SRID", default=7844, cast=int)  # GDA2020
   ```

   Or set the `DEFAULT_SRID` environment variable.

2. **Create a data migration** to transform existing geometry data:

   ```bash
   # Copy the template migration and update OLD_SRID / NEW_SRID
   cp boranga/migrations/0637_transform_geometries_to_default_srid.py \
      boranga/migrations/NNNN_transform_geometries_OLDSRID_to_NEWSRID.py
   ```

   Edit the new file:
   - Update `OLD_SRID` and `NEW_SRID` at the top
   - Update the `dependencies` list to point to the previous migration
   - Add any new geometry tables to `GEOMETRY_COLUMNS` if models were added
     since the last CRS migration

3. **Run `makemigrations`** to generate the schema migration:

   ```bash
   python manage.py makemigrations boranga
   ```

   Django will detect that the `srid` argument on the geometry fields has
   changed and create an `AlterField` migration. Because the data migration
   file already exists, Django will make the new schema migration depend on
   it — guaranteeing that data is transformed **before** column metadata is
   updated. (If you reverse this order, you'll need to manually edit the
   migration dependencies to ensure the data migration runs first.)

4. **Run migrations**:

   ```bash
   python manage.py migrate
   ```

5. **Deploy** — no other code changes are needed. The frontend fetches the
   default SRID from `/api/gis-settings/` at startup.

### Common SRID values

| SRID | Name    | Description                        |
| ---- | ------- | ---------------------------------- |
| 4326 | WGS 84  | World Geodetic System 1984         |
| 4283 | GDA94   | Geocentric Datum of Australia 1994 |
| 7844 | GDA2020 | Geocentric Datum of Australia 2020 |

### What gets updated automatically

- **Django geometry fields**: `srid=settings.DEFAULT_SRID` on all GeometryField
  definitions. `makemigrations` detects the change.
- **Backend spatial utilities**: WFS queries, buffer calculations, coordinate
  transformations all reference `settings.DEFAULT_SRID`.
- **Frontend map**: Fetches the SRID from `/api/gis-settings/` and uses it for
  map projection, WFS queries, feature CRS, and measurements.
- **Shapefile export**: Uses `settings.DEFAULT_SRID` for the output CRS.
- **Management commands**: `import_cadastre_geojson` and `import_wfs_layer`
  default to `settings.DEFAULT_SRID`.

### What does NOT change automatically

- **`original_geometry_ewkb`**: This binary field preserves the original
  geometry as uploaded by the user (in whatever CRS they used). It is not
  transformed during CRS migrations.
- **Data migration adapters** (`boranga/components/data_migration/`): Legacy
  import adapters and handlers now use `settings.DEFAULT_SRID` as their target
  SRID, so re-running legacy imports after a CRS change will produce geometries
  in the correct CRS automatically. Source SRIDs (e.g. GDA94/4283 for TPFL
  coordinates) are left as-is since they describe the incoming data.
- **SQL reporting scripts** (`docs/sql-scripts/`): These contain hardcoded
  `ST_Transform(..., 4326)` calls. Update them if you need area calculations
  in the new CRS.
- **Unmanaged tables** (e.g. `kb_cadastre`): The CadastreLayer model is
  `managed=False`. It will be transformed appropriately the next time
  ./manage.py import_cadastre_geojson is run.
