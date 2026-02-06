from boranga.components.data_migration.registry import (
    static_value_factory,
    wkt_to_geometry_factory,
)

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source


class OccurrenceTecBoundariesAdapter(SourceAdapter):
    source_key = Source.TEC_BOUNDARIES.value
    domain = "occurrence"

    # Pipelines for the geometry record
    PIPELINES = {
        # Transform WKT (Source GDA94/4283 -> Target WGS84/4326)
        "OccurrenceGeometry__geometry": [wkt_to_geometry_factory(source_srid=4283, target_srid=4326)],
        "OccurrenceGeometry__drawn_by": [static_value_factory(None)],
        "OccurrenceGeometry__locked": [static_value_factory(False)],
        # Default show_on_map to True?
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        """
        Extract boundary data from TEC_PEC_Boundaries.csv.
        Links to existing Occurrences via migrated_from_id (OCC_UNIQUE).
        """
        import os

        if os.path.isdir(path):
            candidates = [
                "TEC_PEC_Boundaries_Nov25.csv",
                "TEC_PEC_BOUNDARIES_NOV25.csv",
            ]
            found = False
            for name in candidates:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    path = p
                    found = True
                    break
            if not found:
                # If explicit file not found, fall back to assuming path IS the file if not dir (handled by read_table)
                # or return empty if dir and file missing (though read_table might fail)
                pass

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            # map OCC_UNIQUE to migrated_from_id
            occ_unique = raw.get("OCC_UNIQUE")
            if not occ_unique:
                warnings.append(f"Row {raw.get('_row_num')} missing OCC_UNIQUE")
                continue

            migrated_id = f"tec-{occ_unique}"

            # Check if we have WKT
            wkt = raw.get("WKT")
            if not wkt:
                continue

            row = {
                "migrated_from_id": migrated_id,
                "OccurrenceGeometry__geometry": wkt,
            }
            rows.append(row)

        return ExtractionResult(rows=rows, warnings=warnings)
