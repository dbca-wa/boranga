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
# All fixtures are written to a single file: boranga/fixtures/uat/uat_fixtures.json
# Pass a custom output path as the first argument, e.g.:
#   bash scripts/generate_uat_fixtures.sh /tmp/my_fixtures.json

set -euo pipefail

MANAGE="python manage.py"
OUT_FILE="${1:-boranga/fixtures/uat/uat_fixtures.json}"
mkdir -p "$(dirname "$OUT_FILE")"

echo "=== Generating UAT fixtures into ${OUT_FILE} ==="

$MANAGE dumpdata \
    --indent 2 \
    --natural-foreign \
    --natural-primary \
    --output "$OUT_FILE" \
    \
    boranga.GroupType \
    boranga.Region \
    boranga.District \
    boranga.DocumentCategory \
    boranga.DocumentSubCategory \
    boranga.FaunaGroup \
    boranga.FaunaSubGroup \
    boranga.ThreatCategory \
    boranga.ThreatAgent \
    boranga.CurrentImpact \
    boranga.PotentialImpact \
    boranga.PotentialThreatOnset \
    boranga.SystemEmailGroup \
    \
    boranga.ConservationChangeCode \
    boranga.WALegislativeList \
    boranga.WALegislativeCategory \
    boranga.WAPriorityList \
    boranga.WAPriorityCategory \
    boranga.IUCNVersion \
    boranga.CommonwealthConservationList \
    boranga.OtherConservationAssessmentList \
    boranga.ProposalAmendmentReason \
    \
    boranga.ObservationTime \
    boranga.ObserverCategory \
    boranga.ObserverRole \
    boranga.LocationAccuracy \
    boranga.CoordinateSource \
    boranga.Datum \
    boranga.LandForm \
    boranga.RockType \
    boranga.SoilType \
    boranga.SoilColour \
    boranga.SoilCondition \
    boranga.Drainage \
    boranga.Intensity \
    boranga.SpeciesListRelatesTo \
    boranga.SpeciesRole \
    boranga.ObservationMethod \
    boranga.AreaAssessment \
    boranga.IdentificationCertainty \
    boranga.SampleType \
    boranga.SampleDestination \
    boranga.PermitType \
    boranga.PlantCountMethod \
    boranga.PlantCountAccuracy \
    boranga.CountedSubject \
    boranga.PlantCondition \
    boranga.AnimalBehaviour \
    boranga.PrimaryDetectionMethod \
    boranga.SecondarySign \
    boranga.ReproductiveState \
    boranga.AnimalHealth \
    boranga.DeathReason \
    boranga.WildStatus \
    boranga.SiteType \
    boranga.OccurrenceTenurePurpose \
    boranga.OccurrenceTenureVesting \
    \
    boranga.SubmitterCategory \
    boranga.FileExtensionWhitelist \
    boranga.HelpTextEntry \
    \
    boranga.TileLayer \
    boranga.GeoserverUrl \
    boranga.PlausibilityGeometry

# ── Ledger (System Groups) ────────────────────────────────────────────────────
# SystemGroup lives in the ledger_api_client package. Dumped separately because
# it may not be present in all environments — failure is non-fatal.
$MANAGE dumpdata ledger_api_client.SystemGroup \
    --indent 2 \
    --natural-foreign \
    --natural-primary \
    --output "${OUT_FILE%.json}_system_groups.json" 2>/dev/null && \
    echo "  Note: ledger_api_client.SystemGroup written to ${OUT_FILE%.json}_system_groups.json" || \
    echo "  WARNING: Could not dump SystemGroup — skipping (check the app label for your ledger installation)."

echo ""
echo "=== Done. Fixtures written to ${OUT_FILE} ==="
