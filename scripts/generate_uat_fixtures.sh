#!/usr/bin/env bash
# generate_uat_fixtures.sh
#
# Dumps fixtures for all Django lookup/reference models marked
# "Replicate UAT values in PROD = Yes" in the UAT scripts spreadsheet.
# Nomos-sourced models (Taxonomies, Taxonomy ranks, Classification systems,
# Kingdoms) are intentionally excluded — those are managed by Nomos.
#
# Usage (from the project root):
#   bash scripts/generate_uat_fixtures.sh
#
# Fixtures are written to boranga/fixtures/uat/ (one JSON file per model).
# Pass a custom output directory as the first argument, e.g.:
#   bash scripts/generate_uat_fixtures.sh /tmp/my_fixtures

set -euo pipefail

MANAGE="python manage.py"
OUT_DIR="${1:-boranga/fixtures/uat}"
mkdir -p "$OUT_DIR"

dump() {
    local label="$1"   # app_label.ModelName
    local slug="$2"    # filename-friendly slug
    echo "  Dumping $label → ${slug}.json"
    $MANAGE dumpdata "$label" \
        --indent 2 \
        --natural-foreign \
        --natural-primary \
        --output "${OUT_DIR}/${slug}.json"
}

echo "=== Generating UAT fixtures into ${OUT_DIR}/ ==="

# ── Species & Communities ──────────────────────────────────────────────────────
dump boranga.GroupType                  group_types
dump boranga.Region                     regions
dump boranga.District                   districts
dump boranga.DocumentCategory           document_categories
dump boranga.DocumentSubCategory        document_sub_categories
dump boranga.FaunaGroup                 fauna_groups
dump boranga.FaunaSubGroup              fauna_sub_groups
dump boranga.ThreatCategory             threat_categories
dump boranga.ThreatAgent                threat_agents
dump boranga.CurrentImpact              current_impacts
dump boranga.PotentialImpact            potential_impacts
dump boranga.PotentialThreatOnset       potential_threat_onsets
dump boranga.SystemEmailGroup           system_email_groups

# ── Conservation Status ────────────────────────────────────────────────────────
dump boranga.ConservationChangeCode         conservation_change_codes
dump boranga.WALegislativeList              wa_legislative_lists
dump boranga.WALegislativeCategory          wa_legislative_categories
dump boranga.WAPriorityList                 wa_priority_lists
dump boranga.WAPriorityCategory             wa_priority_categories
dump boranga.IUCNVersion                    iucn_versions
dump boranga.CommonwealthConservationList   commonwealth_conservation_lists
dump boranga.OtherConservationAssessmentList other_conservation_assessment_lists
dump boranga.ProposalAmendmentReason        proposal_amendment_reasons

# ── Occurrence ─────────────────────────────────────────────────────────────────
dump boranga.ObservationTime            observation_times
dump boranga.ObserverCategory           observer_categories
dump boranga.ObserverRole               observer_roles
dump boranga.LocationAccuracy           location_accuracies
dump boranga.CoordinateSource           coordinate_sources
dump boranga.Datum                      datums
dump boranga.LandForm                   land_forms
dump boranga.RockType                   rock_types
dump boranga.SoilType                   soil_types
dump boranga.SoilColour                 soil_colours
dump boranga.SoilCondition              soil_conditions
dump boranga.Drainage                   drainages
dump boranga.Intensity                  intensities
dump boranga.SpeciesListRelatesTo       species_list_relates_to
dump boranga.SpeciesRole                species_roles
dump boranga.ObservationMethod          observation_methods
dump boranga.AreaAssessment             area_assessments
dump boranga.IdentificationCertainty    identification_certainties
dump boranga.SampleType                 sample_types
dump boranga.SampleDestination          sample_destinations
dump boranga.PermitType                 permit_types
dump boranga.PlantCountMethod           plant_count_methods
dump boranga.PlantCountAccuracy         plant_count_accuracies
dump boranga.CountedSubject             counted_subjects
dump boranga.PlantCondition             plant_conditions
dump boranga.AnimalBehaviour            animal_behaviours
dump boranga.PrimaryDetectionMethod     primary_detection_methods
dump boranga.SecondarySign              secondary_signs
dump boranga.ReproductiveState          reproductive_states
dump boranga.AnimalHealth               animal_health
dump boranga.DeathReason                death_reasons
dump boranga.WildStatus                 wild_statuses
dump boranga.SiteType                   site_types
dump boranga.OccurrenceTenurePurpose    occurrence_tenure_purposes
dump boranga.OccurrenceTenureVesting    occurrence_tenure_vestings

# ── Users ──────────────────────────────────────────────────────────────────────
dump boranga.SubmitterCategory          submitter_categories

# ── Main ───────────────────────────────────────────────────────────────────────
dump boranga.FileExtensionWhitelist     file_extension_whitelists
dump boranga.HelpTextEntry              help_text_entries

# ── Spatial ───────────────────────────────────────────────────────────────────
dump boranga.TileLayer                  tile_layers
dump boranga.Proxy                      proxies
dump boranga.GeoserverUrl               geoserver_urls
dump boranga.PlausibilityGeometry       plausibility_geometries

# ── Ledger (System Groups) ────────────────────────────────────────────────────
# SystemGroup lives in the ledger_api_client package. Use the app label
# registered there (typically "ledger_api_client"). Adjust if your
# INSTALLED_APPS registers it under a different label.
echo "  Dumping ledger_api_client.SystemGroup → system_groups.json"
$MANAGE dumpdata ledger_api_client.SystemGroup \
    --indent 2 \
    --natural-foreign \
    --natural-primary \
    --output "${OUT_DIR}/system_groups.json" || \
    echo "  WARNING: Could not dump SystemGroup — check the app label for your ledger installation."

echo ""
echo "=== Done. Fixtures written to ${OUT_DIR}/ ==="
