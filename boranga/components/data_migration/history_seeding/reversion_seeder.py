"""
Seeding of initial django-reversion history for migrated records.

After a data migration run (migrate_data) the records exist in the database but have
no reversion history, because the bulk-insert path intentionally bypasses the
RevisionedMixin.save() path.  If a user later edits any field the very first history
entry created will reflect the post-edit state, permanently losing the migrated values.

This module creates that "baseline" revision for every migrated record using the
same serialisation path that django-reversion itself uses, but with a single bulk
insert of Revision + Version rows so the operation is fast enough to run after every
migration run on a large dataset.

Design decisions
----------------
* One Revision per object-group (parent + followed children).  This matches the
  behaviour of RevisionedMixin.save() which wraps the whole save in
  ``reversion.revisions.create_revision()``.
* User is None (a system / background action with no logged-in user).
* The comment encodes the legacy source system so it is visible in the Django
  admin history view.
* No signals are fired – this module writes directly to the reversion tables.
* Idempotent: objects that already have at least one Version record are skipped so
  the command is safe to re-run (though in practice wipe-targets clears history
  first anyway, per answer 4).

Applies to (per BA specification)
-----------------------------------
Main dashboards
  Species          (migrated_from_id != "")  + follow relations
  Community        (migrated_from_id != "")  + follow relations
  ConservationStatus (migrated_from_id != "") (no follow in reversion.register)
  OccurrenceReport (migrated_from_id != "")  + follow relations
  Occurrence       (migrated_from_id != "")  + follow relations

Sub-records (parent migrated_from_id != "")
  SpeciesDocument          -> species.migrated_from_id
  CommunityDocument        -> community.migrated_from_id
  ConservationStatusDocument -> conservation_status.migrated_from_id
  ConservationThreat       -> species.migrated_from_id | community.migrated_from_id
  OccurrenceReportDocument -> occurrence_report.migrated_from_id
  OCRConservationThreat    -> occurrence_report.migrated_from_id
  OCRObserverDetail        -> occurrence_report.migrated_from_id
  OccurrenceDocument       -> occurrence.migrated_from_id
  OCCConservationThreat    -> occurrence.migrated_from_id
  OCCContactDetail         -> occurrence.migrated_from_id
  OccurrenceSite           -> occurrence.migrated_from_id
  OccurrenceTenure         -> (via occurrence_geometry -> occurrence.migrated_from_id)
"""

import logging
from datetime import datetime, timezone
from typing import Any

from django.contrib.contenttypes.models import ContentType
from django.core import serializers
from django.db import models, router, transaction
from django.utils.encoding import force_str
from reversion.models import Revision, Version
from reversion.revisions import _get_options

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

COMMENT_TEMPLATE = "Migrated from {source} (initial baseline)"
COMMENT_UNKNOWN = "Migrated from legacy system (initial baseline)"


def _source_comment(migrated_from_id: str) -> str:
    """
    Derive the source system label from the migrated_from_id prefix convention.

    migrated_from_id values follow  "<source_prefix>-<legacy_id>", e.g.:
        tpfl-1234     -> TPFL
        tec-5678      -> TEC
        tfauna-9012   -> TFAUNA

    For records that don't use the prefix convention (e.g. raw numeric strings
    from older importers) we fall back to the generic comment.
    """
    if not migrated_from_id:
        return COMMENT_UNKNOWN
    prefix = migrated_from_id.split("-")[0].upper()
    if prefix in ("TPFL", "TEC", "TFAUNA"):
        return COMMENT_TEMPLATE.format(source=prefix)
    return COMMENT_UNKNOWN


def _serialize_obj(obj: models.Model) -> str:
    """Serialize a single model instance using the format registered with reversion."""
    version_options = _get_options(obj.__class__)
    return serializers.serialize(
        version_options.format,
        (obj,),
        fields=version_options.fields,
        use_natural_foreign_keys=version_options.use_natural_foreign_keys,
    )


def _make_version(obj: models.Model, revision: Revision, db: str) -> Version:
    """Build an unsaved Version instance for *obj* attached to *revision*."""
    content_type = ContentType.objects.get_for_model(obj.__class__)
    return Version(
        revision=revision,
        object_id=force_str(obj.pk),
        content_type=content_type,
        db=db,
        format=_get_options(obj.__class__).format,
        serialized_data=_serialize_obj(obj),
        object_repr=force_str(obj),
    )


def _follow_one_to_one(obj: models.Model, follow_names: list[str]) -> list[models.Model]:
    """
    Return all follow-related objects registered on *obj*'s reversion options.

    We only follow the names explicitly listed in the reversion.register(follow=[...])
    call because that is exactly what a normal revision would capture.  M2M and
    reverse FK managers are skipped here (they are not versioned as part of the parent
    in these models).
    """
    related = []
    for name in follow_names:
        try:
            val = getattr(obj, name)
        except Exception:
            continue
        if val is None:
            continue
        if isinstance(val, models.Model):
            related.append(val)
        # QuerySet / Manager (e.g. follow=["taxon_previous_queryset", "vernaculars"]) —
        # these are small sets so iterate them
        elif isinstance(val, (models.Manager | models.QuerySet)):
            try:
                related.extend(list(val.all()))
            except Exception:
                pass
    return related


# ---------------------------------------------------------------------------
# Core seeder class
# ---------------------------------------------------------------------------


class MigratedHistorySeeder:
    """
    Create initial django-reversion history for every migrated record in the
    database that does not already have any history.

    Usage::

        seeder = MigratedHistorySeeder(batch_size=500)
        seeder.seed_all()
        logger.info("Seeder stats: %s", seeder.get_stats())

    Or for a single group::

        seeder.seed_species()
        seeder.seed_occurrence_reports()
    """

    def __init__(self, batch_size: int = 500):
        self.batch_size = batch_size
        self._stats: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def seed_all(self) -> dict[str, int]:
        """Seed initial history for all applicable migrated models."""
        self.seed_species()
        self.seed_communities()
        self.seed_conservation_statuses()
        self.seed_occurrence_reports()
        self.seed_occurrences()
        return self.get_stats()

    def get_stats(self) -> dict[str, int]:
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Per-domain seed methods (called individually or via seed_all)
    # ------------------------------------------------------------------

    def seed_species(self) -> int:
        """
        Seed Species + followed relations + SpeciesDocuments + ConservationThreats
        for species.
        """
        from boranga.components.species_and_communities.models import (
            ConservationThreat,
            Species,
            SpeciesDocument,
        )

        total = 0
        qs = Species.objects.filter(migrated_from_id__gt="").select_related(
            "taxonomy",
            "species_distribution",
            "species_conservation_attributes",
            "species_publishing_status",
        )
        follow_names = [
            "taxonomy",
            "species_distribution",
            "species_conservation_attributes",
            "species_publishing_status",
        ]
        total += self._seed_parent_objects(qs, follow_names, lambda s: _source_comment(s.migrated_from_id), "Species")

        # Sub-records
        sub_qs = SpeciesDocument.objects.filter(species__migrated_from_id__gt="")
        total += self._seed_simple_objects(
            sub_qs,
            lambda d: _source_comment(d.species.migrated_from_id),
            "SpeciesDocument",
        )

        threat_qs = ConservationThreat.objects.filter(species__migrated_from_id__gt="", species__isnull=False)
        total += self._seed_simple_objects(
            threat_qs,
            lambda t: _source_comment(t.species.migrated_from_id),
            "ConservationThreat(species)",
        )

        return total

    def seed_communities(self) -> int:
        """
        Seed Community + followed relations + CommunityDocuments + ConservationThreats
        for communities.
        """
        from boranga.components.species_and_communities.models import (
            Community,
            CommunityDocument,
            ConservationThreat,
        )

        total = 0
        qs = Community.objects.filter(migrated_from_id__gt="").select_related(
            "taxonomy",
            "community_distribution",
            "community_conservation_attributes",
            "community_publishing_status",
        )
        follow_names = [
            "taxonomy",
            "community_distribution",
            "community_conservation_attributes",
            "community_publishing_status",
        ]
        total += self._seed_parent_objects(qs, follow_names, lambda c: _source_comment(c.migrated_from_id), "Community")

        sub_qs = CommunityDocument.objects.filter(community__migrated_from_id__gt="")
        total += self._seed_simple_objects(
            sub_qs,
            lambda d: _source_comment(d.community.migrated_from_id),
            "CommunityDocument",
        )

        threat_qs = ConservationThreat.objects.filter(community__migrated_from_id__gt="", community__isnull=False)
        total += self._seed_simple_objects(
            threat_qs,
            lambda t: _source_comment(t.community.migrated_from_id),
            "ConservationThreat(community)",
        )

        return total

    def seed_conservation_statuses(self) -> int:
        """Seed ConservationStatus + ConservationStatusDocuments."""
        from boranga.components.conservation_status.models import (
            ConservationStatus,
            ConservationStatusDocument,
        )

        total = 0
        qs = ConservationStatus.objects.filter(migrated_from_id__gt="")
        # ConservationStatus is registered without follow relations
        total += self._seed_simple_objects(
            qs,
            lambda cs: _source_comment(cs.migrated_from_id),
            "ConservationStatus",
        )

        sub_qs = ConservationStatusDocument.objects.filter(conservation_status__migrated_from_id__gt="")
        total += self._seed_simple_objects(
            sub_qs,
            lambda d: _source_comment(d.conservation_status.migrated_from_id),
            "ConservationStatusDocument",
        )

        return total

    def seed_occurrence_reports(self) -> int:
        """
        Seed OccurrenceReport + followed 1-to-1 relations + Documents +
        OCRConservationThreats + OCRObserverDetails.
        """
        from boranga.components.occurrence.models import (
            OccurrenceReport,
            OccurrenceReportDocument,
            OCRConservationThreat,
            OCRObserverDetail,
        )

        total = 0
        qs = (
            OccurrenceReport.objects.filter(migrated_from_id__isnull=False)
            .exclude(migrated_from_id="")
            .select_related(
                "habitat_composition",
                "habitat_condition",
                "vegetation_structure",
                "fire_history",
                "associated_species",
                "observation_detail",
                "plant_count",
                "animal_observation",
                "identification",
            )
        )
        follow_names = [
            "habitat_composition",
            "habitat_condition",
            "vegetation_structure",
            "fire_history",
            "associated_species",
            "observation_detail",
            "plant_count",
            "animal_observation",
            "identification",
        ]
        total += self._seed_parent_objects(
            qs,
            follow_names,
            lambda ocr: _source_comment(ocr.migrated_from_id),
            "OccurrenceReport",
        )

        # Sub-records
        doc_qs = OccurrenceReportDocument.objects.filter(occurrence_report__migrated_from_id__isnull=False).exclude(
            occurrence_report__migrated_from_id=""
        )
        total += self._seed_simple_objects(
            doc_qs,
            lambda d: _source_comment(d.occurrence_report.migrated_from_id),
            "OccurrenceReportDocument",
        )

        threat_qs = OCRConservationThreat.objects.filter(occurrence_report__migrated_from_id__isnull=False).exclude(
            occurrence_report__migrated_from_id=""
        )
        total += self._seed_simple_objects(
            threat_qs,
            lambda t: _source_comment(t.occurrence_report.migrated_from_id),
            "OCRConservationThreat",
        )

        observer_qs = OCRObserverDetail.objects.filter(occurrence_report__migrated_from_id__isnull=False).exclude(
            occurrence_report__migrated_from_id=""
        )
        total += self._seed_simple_objects(
            observer_qs,
            lambda o: _source_comment(o.occurrence_report.migrated_from_id),
            "OCRObserverDetail",
        )

        return total

    def seed_occurrences(self) -> int:
        """
        Seed Occurrence + followed 1-to-1 relations + Documents +
        OCCConservationThreats + OCCContactDetails + OccurrenceSites + OccurrenceTenures.
        """
        from boranga.components.occurrence.models import (
            OCCConservationThreat,
            OCCContactDetail,
            Occurrence,
            OccurrenceDocument,
            OccurrenceSite,
            OccurrenceTenure,
        )

        total = 0
        qs = (
            Occurrence.objects.filter(migrated_from_id__isnull=False)
            .exclude(migrated_from_id="")
            .select_related(
                "habitat_composition",
                "habitat_condition",
                "vegetation_structure",
                "fire_history",
                "associated_species",
                "observation_detail",
                "plant_count",
                "animal_observation",
                "identification",
            )
        )
        follow_names = [
            "habitat_composition",
            "habitat_condition",
            "vegetation_structure",
            "fire_history",
            "associated_species",
            "observation_detail",
            "plant_count",
            "animal_observation",
            "identification",
        ]
        total += self._seed_parent_objects(
            qs,
            follow_names,
            lambda occ: _source_comment(occ.migrated_from_id),
            "Occurrence",
        )

        doc_qs = OccurrenceDocument.objects.filter(occurrence__migrated_from_id__isnull=False).exclude(
            occurrence__migrated_from_id=""
        )
        total += self._seed_simple_objects(
            doc_qs,
            lambda d: _source_comment(d.occurrence.migrated_from_id),
            "OccurrenceDocument",
        )

        threat_qs = OCCConservationThreat.objects.filter(occurrence__migrated_from_id__isnull=False).exclude(
            occurrence__migrated_from_id=""
        )
        total += self._seed_simple_objects(
            threat_qs,
            lambda t: _source_comment(t.occurrence.migrated_from_id),
            "OCCConservationThreat",
        )

        contact_qs = OCCContactDetail.objects.filter(occurrence__migrated_from_id__isnull=False).exclude(
            occurrence__migrated_from_id=""
        )
        total += self._seed_simple_objects(
            contact_qs,
            lambda c: _source_comment(c.occurrence.migrated_from_id),
            "OCCContactDetail",
        )

        site_qs = OccurrenceSite.objects.filter(occurrence__migrated_from_id__isnull=False).exclude(
            occurrence__migrated_from_id=""
        )
        total += self._seed_simple_objects(
            site_qs,
            lambda s: _source_comment(s.occurrence.migrated_from_id),
            "OccurrenceSite",
        )

        # OccurrenceTenure links via occurrence_geometry -> occurrence
        tenure_qs = (
            OccurrenceTenure.objects.filter(occurrence_geometry__occurrence__migrated_from_id__isnull=False)
            .exclude(occurrence_geometry__occurrence__migrated_from_id="")
            .select_related("occurrence_geometry__occurrence")
        )
        total += self._seed_simple_objects(
            tenure_qs,
            lambda t: _source_comment(t.occurrence_geometry.occurrence.migrated_from_id),
            "OccurrenceTenure",
        )

        return total

    # ------------------------------------------------------------------
    # Core bulk-seed helpers
    # ------------------------------------------------------------------

    def _already_versioned_ids(self, model_class: type[models.Model], object_ids: list[str]) -> set[str]:
        """Return the string PKs of objects that already have at least one Version."""
        ct = ContentType.objects.get_for_model(model_class)
        existing = (
            Version.objects.filter(
                content_type=ct,
                object_id__in=object_ids,
            )
            .values_list("object_id", flat=True)
            .distinct()
        )
        return set(existing)

    def _seed_simple_objects(
        self,
        queryset: models.QuerySet,
        comment_fn: Any,
        label: str,
    ) -> int:
        """
        Create one Revision+Version per object in *queryset*.

        Each object gets its own Revision (comment derived from comment_fn).
        Objects that already have history are skipped.
        """
        db = router.db_for_write(queryset.model)
        model_class = queryset.model
        now = datetime.now(tz=timezone.utc)

        all_ids = list(queryset.values_list("pk", flat=True))
        if not all_ids:
            logger.info("seed_simple(%s): no migrated objects found", label)
            self._stats[label] = 0
            return 0

        str_ids = [str(pk) for pk in all_ids]
        already = self._already_versioned_ids(model_class, str_ids)
        logger.info(
            "seed_simple(%s): %d total, %d already have history → %d to seed",
            label,
            len(all_ids),
            len(already),
            len(all_ids) - len(already),
        )

        total_created = 0

        for i in range(0, len(all_ids), self.batch_size):
            batch_pks = all_ids[i : i + self.batch_size]
            batch_str = [str(pk) for pk in batch_pks]
            to_seed_pks = [pk for pk, s in zip(batch_pks, batch_str) if s not in already]
            if not to_seed_pks:
                continue

            objects = list(queryset.model._default_manager.filter(pk__in=to_seed_pks))
            if not objects:
                continue

            revisions_to_create = []
            versions_data = []  # list of (comment, obj) pairs

            for obj in objects:
                try:
                    comment = comment_fn(obj)
                except Exception:
                    comment = COMMENT_UNKNOWN
                revisions_to_create.append(Revision(date_created=now, user=None, comment=comment))
                versions_data.append(obj)

            with transaction.atomic():
                created_revisions = Revision.objects.bulk_create(revisions_to_create)
                versions_to_create = []
                for revision, obj in zip(created_revisions, versions_data):
                    try:
                        versions_to_create.append(_make_version(obj, revision, db))
                    except Exception as exc:
                        logger.warning("seed_simple(%s): failed to serialize pk=%s: %s", label, obj.pk, exc)
                Version.objects.bulk_create(versions_to_create)
                total_created += len(versions_to_create)

        self._stats.setdefault(label, 0)
        self._stats[label] += total_created
        if total_created:
            logger.info("seed_simple(%s): created %d version(s)", label, total_created)
        return total_created

    def _seed_parent_objects(
        self,
        queryset: models.QuerySet,
        follow_names: list[str],
        comment_fn: Any,
        label: str,
    ) -> int:
        """
        Create one Revision that covers a parent object AND all its reversion
        follow-relations (one-to-one children).

        This matches what ``RevisionedMixin.save()`` produces: a single Revision
        with multiple Version rows — one for the parent and one for each followed
        child that exists.
        """
        db = router.db_for_write(queryset.model)
        model_class = queryset.model
        now = datetime.now(tz=timezone.utc)

        all_ids = list(queryset.values_list("pk", flat=True))
        if not all_ids:
            logger.info("seed_parent(%s): no migrated objects found", label)
            self._stats[label] = 0
            return 0

        str_ids = [str(pk) for pk in all_ids]
        already = self._already_versioned_ids(model_class, str_ids)
        to_seed_pks = [pk for pk, s in zip(all_ids, str_ids) if s not in already]

        logger.info(
            "seed_parent(%s): %d total, %d already have history → %d to seed",
            label,
            len(all_ids),
            len(already),
            len(to_seed_pks),
        )

        total_versions = 0

        for i in range(0, len(to_seed_pks), self.batch_size):
            batch_pks = to_seed_pks[i : i + self.batch_size]
            objects = list(queryset.filter(pk__in=batch_pks))
            if not objects:
                continue

            revisions_to_create = []
            # list of (parent_obj, [child_objs])
            obj_groups: list[tuple[models.Model, list[models.Model]]] = []

            for obj in objects:
                try:
                    comment = comment_fn(obj)
                except Exception:
                    comment = COMMENT_UNKNOWN
                children = _follow_one_to_one(obj, follow_names)
                revisions_to_create.append(Revision(date_created=now, user=None, comment=comment))
                obj_groups.append((obj, children))

            with transaction.atomic():
                created_revisions = Revision.objects.bulk_create(revisions_to_create)
                versions_to_create = []
                for revision, (parent, children) in zip(created_revisions, obj_groups):
                    try:
                        versions_to_create.append(_make_version(parent, revision, db))
                    except Exception as exc:
                        logger.warning("seed_parent(%s): failed to serialize parent pk=%s: %s", label, parent.pk, exc)
                        continue
                    for child in children:
                        try:
                            versions_to_create.append(_make_version(child, revision, db))
                        except Exception as exc:
                            logger.warning(
                                "seed_parent(%s): failed to serialize child %s pk=%s: %s",
                                label,
                                child.__class__.__name__,
                                child.pk,
                                exc,
                            )
                Version.objects.bulk_create(versions_to_create)
                total_versions += len(versions_to_create)

        self._stats.setdefault(label, 0)
        self._stats[label] += total_versions
        if total_versions:
            logger.info("seed_parent(%s): created %d version(s)", label, total_versions)
        return total_versions
