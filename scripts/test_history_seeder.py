"""
End-to-end smoke test for MigratedHistorySeeder.

Run with:
    GIT_HASH=dev python manage.py shell < scripts/test_history_seeder.py

Strategy:
  1. Create synthetic migrated records via normal .save() (RevisionedMixin creates
     an initial revision automatically).
  2. Delete those auto-created Version rows to simulate the post-wipe-targets state
     (i.e. records exist but have no history, exactly as they would be after the
     data migration importer runs).
  3. Run the seeder and assert the correct history is created.
  4. Roll back the entire transaction so no test data is persisted.
"""

import json
import sys

from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from reversion.models import Revision, Version

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.data_migration.history_seeding.reversion_seeder import MigratedHistorySeeder
from boranga.components.occurrence.models import (
    OccurrenceReport,
    OCRConservationThreat,
    OCRHabitatComposition,
    OCRHabitatCondition,
    OCRObserverDetail,
)
from boranga.components.species_and_communities.models import GroupType, Species

PASS = "PASS"
FAIL = "FAIL"
errors = []


def check(description, condition):
    if condition:
        print(f"  [{PASS}]  {description}")
    else:
        print(f"  [{FAIL}]  {description}  <-- FAILED")
        errors.append(description)


def version_count(obj):
    ct = ContentType.objects.get_for_model(obj.__class__)
    return Version.objects.filter(content_type=ct, object_id=str(obj.pk)).count()


def revision_comment(obj):
    ct = ContentType.objects.get_for_model(obj.__class__)
    v = Version.objects.filter(content_type=ct, object_id=str(obj.pk)).select_related("revision").first()
    return v.revision.comment if v else None


def wipe_history(*objs):
    """Delete all Version (and orphaned Revision) rows for the given objects."""
    for obj in objs:
        ct = ContentType.objects.get_for_model(obj.__class__)
        Version.objects.filter(content_type=ct, object_id=str(obj.pk)).delete()
    # Remove Revisions that now have no versions
    Revision.objects.filter(version=None).delete()


print("\n=== MigratedHistorySeeder smoke test ===\n")

with transaction.atomic():
    # ── 1. Create test records ────────────────────────────────────────────────
    print("Step 1: Creating test records ...")

    # OCR: TPFL with 2 follow-relation 1-to-1 children + 1 observer + 1 threat
    ocr_tpfl = OccurrenceReport(migrated_from_id="tpfl-smoke-001")
    ocr_tpfl.save(no_revision=True)
    # OCR.save() calls itself twice (to set occurrence_report_number); the second
    # call goes through RevisionedMixin and creates a version — so wipe it below.
    hc = OCRHabitatComposition.objects.create(occurrence_report=ocr_tpfl)
    hcond = OCRHabitatCondition.objects.create(occurrence_report=ocr_tpfl)
    obs = OCRObserverDetail.objects.create(occurrence_report=ocr_tpfl)
    threat = OCRConservationThreat.objects.create(occurrence_report=ocr_tpfl)

    # OCR: TEC (no children)
    ocr_tec = OccurrenceReport(migrated_from_id="tec-smoke-001")
    ocr_tec.save(no_revision=True)

    # OCR: TFAUNA (no children)
    ocr_tfauna = OccurrenceReport(migrated_from_id="tfauna-smoke-001")
    ocr_tfauna.save(no_revision=True)

    # Species requires group_type FK
    flora = GroupType.objects.get(name="flora")
    sp = Species(migrated_from_id="tpfl-smoke-sp-001", group_type=flora)
    sp.save(no_revision=True)

    # ConservationStatus — CharField blank=False fields can stay empty for a
    # direct save; we bypass full_clean by going to the DB via bulk_create.
    cs = ConservationStatus.objects.bulk_create([ConservationStatus(migrated_from_id="tpfl-smoke-cs-001")])[0]
    # bulk_create doesn't call save(), so conservation_status_number is empty.
    # That's fine — we only need migrated_from_id populated for the seeder.

    print(f"  OCRs: pk={ocr_tpfl.pk} (tpfl), pk={ocr_tec.pk} (tec), pk={ocr_tfauna.pk} (tfauna)")
    print(f"  Species pk={sp.pk}, ConservationStatus pk={cs.pk}")
    print(f"  OCR tpfl children: hc={hc.pk}, hcond={hcond.pk}, obs={obs.pk}, threat={threat.pk}")

    # ── 2. Wipe any auto-created history (simulates post-wipe-targets state) ──
    print("\nStep 2: Wiping auto-created reversion history ...")
    wipe_history(ocr_tpfl, ocr_tec, ocr_tfauna, hc, hcond, obs, threat, sp, cs)

    check("OCR tpfl has no history after wipe", version_count(ocr_tpfl) == 0)
    check("OCR tec has no history after wipe", version_count(ocr_tec) == 0)
    check("Species has no history after wipe", version_count(sp) == 0)

    # ── 3. Run the seeder ─────────────────────────────────────────────────────
    print("\nStep 3: Running seeder ...")
    seeder = MigratedHistorySeeder(batch_size=500)
    seeder.seed_occurrence_reports()
    seeder.seed_species()
    seeder.seed_conservation_statuses()
    stats = seeder.get_stats()
    print(f"  Stats: {stats}")

    # ── 4. Assert results ─────────────────────────────────────────────────────
    print("\nStep 4: Asserting results ...")

    # Each OCR gets exactly 1 Version
    check("OCR tpfl has 1 Version", version_count(ocr_tpfl) == 1)
    check("OCR tec has 1 Version", version_count(ocr_tec) == 1)
    check("OCR tfauna has 1 Version", version_count(ocr_tfauna) == 1)

    # Follow-relation children share the *same* Revision as the parent
    ct_ocr = ContentType.objects.get_for_model(OccurrenceReport)
    parent_revision = (
        Version.objects.filter(content_type=ct_ocr, object_id=str(ocr_tpfl.pk))
        .select_related("revision")
        .first()
        .revision
    )

    def child_revision_id(obj):
        ct = ContentType.objects.get_for_model(obj.__class__)
        v = Version.objects.filter(content_type=ct, object_id=str(obj.pk)).first()
        return v.revision_id if v else None

    check(
        "habitat_composition has 1 Version and shares parent Revision",
        version_count(hc) == 1 and child_revision_id(hc) == parent_revision.pk,
    )
    check(
        "habitat_condition has 1 Version and shares parent Revision",
        version_count(hcond) == 1 and child_revision_id(hcond) == parent_revision.pk,
    )

    # Sub-records (observer, threat) get their own independent Revision
    check("OCRObserverDetail has 1 Version", version_count(obs) == 1)
    check("OCRObserverDetail Revision is separate from parent", child_revision_id(obs) != parent_revision.pk)
    check("OCRConservationThreat has 1 Version", version_count(threat) == 1)

    # Comment correctness
    check("TPFL comment correct", revision_comment(ocr_tpfl) == "Migrated from TPFL (initial baseline)")
    check("TEC comment correct", revision_comment(ocr_tec) == "Migrated from TEC (initial baseline)")
    check("TFAUNA comment correct", revision_comment(ocr_tfauna) == "Migrated from TFAUNA (initial baseline)")

    # Revision.user is None (system action, no user attribution)
    check("Revision.user is None", parent_revision.user is None)

    # Species & ConservationStatus seeded
    check("Species has 1 Version", version_count(sp) == 1)
    check("ConservationStatus has 1 Version", version_count(cs) == 1)

    # serialized_data is valid JSON and contains migrated_from_id
    parent_v = Version.objects.filter(content_type=ct_ocr, object_id=str(ocr_tpfl.pk)).first()
    try:
        parsed = json.loads(parent_v.serialized_data)
        check("Serialized data is valid JSON", True)
        check(
            "Serialized data contains migrated_from_id",
            any("migrated_from_id" in str(item.get("fields", {})) for item in parsed),
        )
    except Exception as exc:
        check(f"Serialized data is valid JSON ({exc})", False)

    # ── 5. Idempotency: re-run, nothing new created ───────────────────────────
    print("\nStep 5: Idempotency check (running seeder a second time) ...")
    seeder2 = MigratedHistorySeeder(batch_size=500)
    seeder2.seed_occurrence_reports()
    seeder2.seed_species()
    seeder2.seed_conservation_statuses()
    stats2 = seeder2.get_stats()
    print(f"  Stats on 2nd run: {stats2}")
    check("2nd run: 0 new OccurrenceReport versions", stats2.get("OccurrenceReport", 0) == 0)
    check("2nd run: 0 new Species versions", stats2.get("Species", 0) == 0)
    check("2nd run: 0 new ConservationStatus versions", stats2.get("ConservationStatus", 0) == 0)

    # ── 6. Roll back everything ───────────────────────────────────────────────
    print("\nStep 6: Rolling back all test data (nothing persisted) ...")
    transaction.set_rollback(True)

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 50)
if errors:
    print(f"RESULT: {len(errors)} assertion(s) FAILED:")
    for e in errors:
        print(f"  [{FAIL}] {e}")
    sys.exit(1)
else:
    print("RESULT: All assertions passed")
