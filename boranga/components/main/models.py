from __future__ import annotations

import html
import logging
import os
from abc import ABCMeta, abstractmethod

import nh3
from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.contrib.gis.db import models as gis_models
from django.core.cache import cache
from django.core.files.storage import FileSystemStorage
from django.db import models
from django.utils import timezone
from django.utils.html import strip_tags
from ordered_model.models import OrderedModel, OrderedModelManager
from reversion.models import Version

from boranga.helpers import check_file

private_storage = FileSystemStorage(
    location=settings.BASE_DIR + "/private-media/", base_url="/private-media/"
)
model_type = models.base.ModelBase

logger = logging.getLogger(__name__)


class TagStrippingModelMixin:
    """
    Mixin to strip HTML tags from all CharField and TextField string values before saving.

    Uses Django's `strip_tags` to remove HTML tags and `html.unescape` to
    convert entities like `&amp;` back to readable characters.
    """

    def save(self, *args, **kwargs):
        for field in self._meta.get_fields():
            if isinstance(field, (models.CharField, models.TextField)):
                value = getattr(self, field.name, None)
                if isinstance(value, str) and value:
                    cleaned = strip_tags(value)
                    cleaned = html.unescape(cleaned)
                    setattr(self, field.name, cleaned)

        super().save(*args, **kwargs)


def neutralise_html(value: str) -> str:
    """
    Neutralise HTML tags in a string using nh3.
    """
    if not value:
        return value
    cleaned = nh3.clean(value)
    cleaned = cleaned.replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'")
    return cleaned


class Nh3SanitizationModelMixin:
    """
    Mixin to sanitize all CharField and TextField string values using nh3 before saving.
    """

    def save(self, *args, **kwargs):
        # Sanitize CharField and TextField values
        for field in self._meta.get_fields():
            if isinstance(field, (models.CharField, models.TextField)):
                value = getattr(self, field.name, None)
                if isinstance(value, str):
                    setattr(self, field.name, neutralise_html(value))

        super().save(*args, **kwargs)


class BaseModel(TagStrippingModelMixin, Nh3SanitizationModelMixin, models.Model):
    """
    Base model class that all models should inherit from.
    It provides a common interface for saving and retrieving models.
    """

    class Meta:
        abstract = True
        app_label = "boranga"


# GeoDjango model for the imported cadastre layer. The table is expected to be
# created outside of Django (via ogr2ogr), so this model is unmanaged.


class CadastreLayer(models.Model):
    gid = models.BigIntegerField(primary_key=True)
    geom = gis_models.GeometryField(srid=4326, null=True)

    cad_owner_name = models.CharField(max_length=512, null=True, blank=True)
    cad_owner_count = models.IntegerField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"public"."kb_cadastre"'
        app_label = "boranga"
        verbose_name = "Cadastre layer"
        verbose_name_plural = "Cadastre layer"


class AbstractModelMeta(ABCMeta, model_type):
    pass


class RevisionedMixin(BaseModel):
    """
    A model tracked by reversion through the save method.
    """

    def save(self, **kwargs):
        from reversion import revisions

        if kwargs.pop("no_revision", False):
            if "version_user" in kwargs:
                kwargs.pop("version_user", None)
            if "version_comment" in kwargs:
                kwargs.pop("version_comment", "")
            super().save(**kwargs)
        # kwargs can be set as attributes via serializers sometimes
        elif hasattr(self, "no_revision") and self.no_revision:
            if "version_user" in kwargs:
                kwargs.pop("version_user", None)
            if "version_comment" in kwargs:
                kwargs.pop("version_comment", "")
            super().save(**kwargs)
            # set no_revision to False - if an instance is saved twice for some reason
            # this should NOT be carried over (unless set to True again)
            self.no_revision = False
        else:
            with revisions.create_revision():
                if "version_user" in kwargs:
                    revisions.set_user(kwargs.pop("version_user", None))
                elif hasattr(self, "version_user") and self.version_user is not None:
                    revisions.set_user(self.version_user)
                    self.version_user = None
                if "version_comment" in kwargs:
                    revisions.set_comment(kwargs.pop("version_comment", ""))
                super().save(**kwargs)

    @property
    def revision_created_date(self):
        return Version.objects.get_for_object(self).last().revision.date_created

    @property
    def revision_modified_date(self):
        return Version.objects.get_for_object(self).first().revision.date_created

    class Meta:
        abstract = True


class UserAction(BaseModel):
    who = models.IntegerField()  # EmailUserRO
    when = models.DateTimeField(null=False, blank=False, auto_now_add=True)
    what = models.TextField(blank=False)

    def __str__(self):
        return "{what} ({who} at {when})".format(
            what=self.what, who=self.who, when=self.when
        )

    class Meta:
        abstract = True
        app_label = "boranga"


class CommunicationsLogEntry(BaseModel):
    TYPE_CHOICES = [
        ("email", "Email"),
        ("phone", "Phone Call"),
        ("mail", "Mail"),
        ("person", "In Person"),
        ("onhold", "On Hold"),
        ("onhold_remove", "Remove On Hold"),
        ("with_qaofficer", "With QA Officer"),
        ("with_qaofficer_completed", "QA Officer Completed"),
        ("referral_complete", "Referral Completed"),
    ]
    DEFAULT_TYPE = TYPE_CHOICES[0][0]

    to = models.TextField(blank=True, verbose_name="To")
    fromm = models.CharField(max_length=200, blank=True, verbose_name="From")
    cc = models.TextField(blank=True, verbose_name="cc")

    type = models.CharField(max_length=35, choices=TYPE_CHOICES, default=DEFAULT_TYPE)
    reference = models.CharField(max_length=100, blank=True)
    subject = models.CharField(
        max_length=200, blank=True, verbose_name="Subject / Description"
    )
    text = models.TextField(blank=True)

    customer = models.IntegerField(null=True)  # EmailUserRO
    staff = models.IntegerField()  # EmailUserRO

    created = models.DateTimeField(auto_now_add=True, null=False, blank=False)

    class Meta:
        app_label = "boranga"


class FileExtensionWhitelist(BaseModel):

    name = models.CharField(
        max_length=16,
        help_text="The file extension without the dot, e.g. jpg, pdf, docx, etc",
    )
    model = models.CharField(max_length=255, default="all")

    compressed = models.BooleanField(
        help_text="Check this box for extensions such as zip, 7z, and tar",
    )

    class Meta:
        app_label = "boranga"
        unique_together = ("name", "model")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._meta.get_field("model").choices = (
            (
                "all",
                "all",
            ),
        ) + tuple(
            map(
                lambda m: (m, m),
                filter(
                    lambda m: Document
                    in apps.get_app_config("boranga").models[m].__bases__,
                    apps.get_app_config("boranga").models,
                ),
            )
        )

    def __str__(self):
        return f"File extension: {self.name} is whitelisted for model: {self.model}"

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        cache.delete(settings.CACHE_KEY_FILE_EXTENSION_WHITELIST)


class DocumentQuerySet(models.QuerySet):
    def delete(self) -> None:
        self.update(active=False)


class ActiveManager(models.Manager):
    def active(self) -> models.QuerySet:
        return self.model.objects.filter(active=True)

    def get_queryset(self) -> models.QuerySet:
        return DocumentQuerySet(self.model, using=self._db)


class Document(RevisionedMixin, metaclass=AbstractModelMeta):
    name = models.CharField(
        max_length=255, blank=True, verbose_name="name", help_text=""
    )
    description = models.TextField(blank=True, verbose_name="description", help_text="")
    uploaded_date = models.DateTimeField(auto_now_add=True)
    active = models.BooleanField(default=True)
    uploaded_by = models.IntegerField(null=True)  # EmailUserRO

    class Meta:
        app_label = "boranga"
        abstract = True

    def __str__(self):
        return self.name or self.filename

    def delete(self, *args, **kwargs):
        # Users are allowed to remove documents from our file system
        # if they are related to a proposal that has not yet been submitted
        parent_instance = self.get_parent_instance()
        if not parent_instance:
            raise AttributeError(
                "Document does not have an associated parent instance. Cannot delete."
            )

        # If the parent instance doesn't have a lodgement_date field
        # or it has a lodgement_date field and it is not None
        # then we do not allow the file to be removed from the file system
        if self.parent_submitted:
            self.deactivate()
            return

        # If the parent instance has a lodgement_date field and it is None
        # then we allow the file to be removed from the file system as the
        # parent instance has not yet been submitted
        if (
            hasattr(parent_instance, "lodgement_date")
            and parent_instance.lodgement_date is None
            and self._file
        ):
            if os.path.exists(self._file.path):
                os.remove(self._file.path)
            else:
                logger.warning(
                    "File not found on file system: %s (%s). Setting _file to None",
                    self._file.path,
                    self._file.name,
                )
            self._file = None

        # Document records are never actually deleted, just marked as inactive
        self.deactivate()

    def deactivate(self):
        self.active = False
        self.save()

    @abstractmethod
    def get_parent_instance(self) -> BaseModel:
        raise NotImplementedError(
            "Subclasses of Document must implement a get_parent_instance method"
        )

    @property
    def parent_submitted(self):
        parent_instance = self.get_parent_instance()
        if (
            hasattr(parent_instance, "lodgement_date")
            and parent_instance.lodgement_date
        ):
            return True
        return False

    @property
    def path(self):
        # return self.file.path
        # return self._file.path
        # comment above line to fix the error "The '_file' attribute has no file
        # associated with it." when adding comms log entry.
        if self._file:
            return self._file.path
        else:
            return ""

    @property
    def filename(self):
        return os.path.basename(self.path)

    def check_file(self, file):
        return check_file(file, self._meta.model_name)


# @python_2_unicode_compatible
class SystemMaintenance(BaseModel):
    name = models.CharField(max_length=100)
    description = models.TextField()
    start_date = models.DateTimeField()
    end_date = models.DateTimeField()

    def duration(self):
        """Duration of system maintenance (in mins)"""
        return (
            int((self.end_date - self.start_date).total_seconds() / 60.0)
            if self.end_date and self.start_date
            else ""
        )
        # return (datetime.now(tz=tz) - self.start_date).total_seconds()/60.

    duration.short_description = "Duration (mins)"

    class Meta:
        app_label = "boranga"
        verbose_name_plural = "System maintenance"

    def __str__(self):
        return (
            f"System Maintenance: {self.name} ({self.description}) "
            f"- starting {self.start_date}, ending {self.end_date}"
        )


class UserSystemSettings(BaseModel):
    user = models.IntegerField(unique=True)  # EmailUserRO
    area_of_interest = models.ForeignKey(
        "GroupType", on_delete=models.PROTECT, null=True, blank=True
    )

    class Meta:
        app_label = "boranga"
        verbose_name_plural = "User System Settings"


class ArchivableManager(models.Manager):
    def active(self):
        return super().get_queryset().filter(archived=False)

    def archived(self):
        return super().get_queryset().filter(archived=True)


class ArchivableModel(BaseModel):
    objects = ArchivableManager()

    archived = models.BooleanField(default=False)

    class Meta:
        abstract = True

    def archive(self):
        if not self.archived:
            self.archived = True
            self.save()

    def unarchive(self):
        if self.archived:
            self.archived = False
            self.save()


class OrderedArchivableManager(OrderedModelManager, ArchivableManager):
    pass


class HelpTextEntry(ArchivableModel):
    section_id = models.CharField(max_length=255, unique=True)
    text = models.TextField()
    icon_with_popover = models.BooleanField(
        default=False,
        help_text="Instead of showing the text in situ, show a popover with the text",
    )
    authenticated_users_only = models.BooleanField(default=True)
    internal_users_only = models.BooleanField(default=True)

    class Meta:
        app_label = "boranga"
        verbose_name_plural = "Help Text Entries"

    def __str__(self):
        return self.section_id


class AbstractOrderedListManager(OrderedModelManager):
    def active(self):
        return super().get_queryset().filter(archived=False)

    def archived(self):
        return super().get_queryset().filter(archived=True)


class AbstractOrderedList(OrderedModel, ArchivableModel):
    objects = AbstractOrderedListManager()

    item = models.CharField(max_length=100)

    class Meta(OrderedModel.Meta):
        abstract = True
        app_label = "boranga"

    def __str__(self):
        return str(self.item)

    def get_lists_dict(
        cls: models.base.ModelBase,
        active_only: bool = False,
    ) -> list:
        lists = cls.objects.all()

        if active_only:
            lists = cls.objects.active()

        lists = lists.values("id", "item").order_by("order")

        return list(lists)


class LockableModel(BaseModel):
    locked = models.BooleanField(
        null=False, blank=False, default=False, help_text="Whether the record is locked"
    )

    class Meta:
        abstract = True
        app_label = "boranga"

    def lock(self):
        if not self.locked:
            self.locked = True
            self.save()

    def unlock(self):
        if self.locked:
            self.locked = False
            self.save()

    def toggle_lock(self):
        self.locked = not self.locked
        self.save()


# ---------------- Models used for data migration only ----------------------------


class MigrationRun(models.Model):
    """Represents a single data-migration run invoked via management commands.

    This model is minimal by design: it records who started the run (if any),
    when it started, an optional name, and any arbitrary options used.
    """

    name = models.CharField(max_length=255, null=True, blank=True)
    started_by = models.IntegerField(null=True, blank=True)
    started_at = models.DateTimeField(default=timezone.now)
    options = models.JSONField(null=True, blank=True)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return f"MigrationRun {self.id} - {self.name or self.started_at}"


class LegacyValueMap(models.Model):
    """
    Maps a legacy enumerated value to a target Django object (any model) or a
    canonical free value (when target object not applicable).

    Use cases:
      - Enumerated lookups (taxon rank, region codes, statuses, etc.)
      - Synonyms: multiple legacy_value rows can point to the same target
    """

    legacy_system = models.CharField(max_length=30)  # e.g. 'TPFL', 'TEC'
    list_name = models.CharField(max_length=50)  # e.g. 'taxon_rank'
    legacy_value = models.CharField(max_length=255)

    # Optional canonical normalised string (if not pointing to an object)
    canonical_name = models.CharField(max_length=255, blank=True, null=True)

    # Generic FK to target object (optional)
    target_content_type = models.ForeignKey(
        ContentType, on_delete=models.CASCADE, blank=True, null=True
    )
    target_object_id = models.PositiveIntegerField(blank=True, null=True)
    target_object = GenericForeignKey("target_content_type", "target_object_id")

    active = models.BooleanField(default=True)
    notes = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "boranga"
        unique_together = (("legacy_system", "list_name", "legacy_value"),)
        indexes = [
            models.Index(fields=["legacy_system", "list_name"]),
            models.Index(fields=["legacy_system", "list_name", "legacy_value"]),
        ]

    def __str__(self):
        tgt = self.target_object or self.canonical_name or "∅"
        return f"{self.legacy_system}:{self.list_name}:{self.legacy_value} -> {tgt}"

    @property
    def key_tuple(self):
        return (self.legacy_system, self.list_name, self.legacy_value)

    @classmethod
    def get_target(
        cls,
        legacy_system: str,
        list_name: str,
        legacy_value: str,
        *,
        require_active: bool = True,
        use_cache: bool = True,
    ):
        """
        Return the resolved target object or canonical_name (if no target_object) for the
        given legacy tuple. Returns None if no mapping found or (require_active and not active).

        Usage:
            obj_or_name = LegacyValueMap.get_target('TPFL', 'community', 'COMM123')
        """
        if not (legacy_system and list_name and legacy_value):
            return None

        cache_key = f"legacymap:{legacy_system}:{list_name}:{legacy_value}"
        if use_cache:
            from django.core.cache import cache

            cached = cache.get(cache_key)
            if cached is not None:
                return cached

        try:
            rec = cls.objects.get(
                legacy_system=legacy_system,
                list_name=list_name,
                legacy_value=legacy_value,
            )
        except cls.DoesNotExist:
            if use_cache:
                cache.set(cache_key, None, 60)  # negative cache short TTL
            return None

        if require_active and not rec.active:
            if use_cache:
                cache.set(cache_key, None, 60)
            return None

        # prefer target object if present, otherwise return canonical string (or None)
        result = (
            rec.target_object if rec.target_object is not None else rec.canonical_name
        )

        if use_cache:
            cache.set(cache_key, result, 300)
        return result


class OccToOcrSectionMapping(models.Model):
    """
    Model to map the sections of an OCR that should be copied to a specific OCC.
    To be populated during the migration of OCCs so that we can copy the relevant
    section data when we are migrating in the ORFs without having to do an extra pass.
    """

    # source context
    legacy_system = models.CharField(max_length=50, db_index=True)

    # legacy identifiers from spreadsheets (fast to look up while importing)
    occ_migrated_from_id = models.CharField(max_length=255, db_index=True)
    ocr_migrated_from_id = models.CharField(max_length=255, db_index=True)

    # canonical section name; use choices to avoid typos and to make code deterministic
    SECTION_LOCATION = "location"
    SECTION_HABITAT_COMPOSITION = "habitat_composition"
    SECTION_HABITAT_CONDITION = "habitat_condition"
    SECTION_VEGETATION_STRUCTURE = "vegetation_structure"
    SECTION_FIRE_HISTORY = "fire_history"
    SECTION_ASSOCIATED_SPECIES = "associated_species"
    SECTION_OBSERVATION_DETAIL = "observation_detail"
    SECTION_PLANT_COUNT = "plant_count"
    SECTION_ANIMAL_OBSERVATION = "animal_observation"
    SECTION_IDENTIFICATION = "identification"

    SECTION_CHOICES = (
        (SECTION_LOCATION, "Location"),
        (SECTION_HABITAT_COMPOSITION, "Habitat Composition"),
        (SECTION_HABITAT_CONDITION, "Habitat Condition"),
        (SECTION_VEGETATION_STRUCTURE, "Vegetation Structure"),
        (SECTION_FIRE_HISTORY, "Fire History"),
        (SECTION_ASSOCIATED_SPECIES, "Associated Species"),
        (SECTION_OBSERVATION_DETAIL, "Observation Detail"),
        (SECTION_PLANT_COUNT, "Plant Count"),
        (SECTION_ANIMAL_OBSERVATION, "Animal Observation"),
        (SECTION_IDENTIFICATION, "Identification"),
    )

    section = models.CharField(max_length=64, choices=SECTION_CHOICES)

    # resolution / processing state
    processed = models.BooleanField(default=False, db_index=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(blank=True)

    # optional resolved FKs to speed processing once ORF/OCC objects exist
    resolved_occ_id = models.PositiveIntegerField(null=True, blank=True, db_index=True)
    resolved_ocr_id = models.PositiveIntegerField(null=True, blank=True, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = "boranga"
        indexes = [
            models.Index(fields=["legacy_system", "occ_migrated_from_id"]),
            models.Index(fields=["legacy_system", "ocr_migrated_from_id", "processed"]),
            models.Index(fields=["resolved_occ_id"]),
            models.Index(fields=["resolved_ocr_id"]),
        ]
        unique_together = (
            (
                "legacy_system",
                "occ_migrated_from_id",
                "ocr_migrated_from_id",
                "section",
            ),
        )

    def mark_done(self):
        self.processed = True
        self.processed_at = timezone.now()
        self.error = ""
        self.save(update_fields=["processed", "processed_at", "error"])


class LegacyUsernameEmailuserMapping(models.Model):
    """
    Model to map legacy usernames to EmailUser instances.
    """

    legacy_system = models.CharField(max_length=50, db_index=True)
    legacy_username = models.CharField(max_length=255, unique=True)
    email = models.EmailField(max_length=255)
    first_name = models.CharField(max_length=255)
    last_name = models.CharField(max_length=255)
    emailuser_id = models.PositiveIntegerField()

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return f"{self.legacy_username} -> {self.emailuser_id}"


class LegacyTaxonomyMapping(models.Model):
    """
    Mapping model for legacy taxonomy canonical names to taxon_name_id and
    optional resolved `Taxonomy` FK (populated later by import script).

    Fields:
      - list_name: name of the list/context (e.g. 'taxon')
      - legacy_canonical_name: canonical string from legacy data
      - taxon_name_id: numeric ID from external taxonomy source (required)
      - taxonomy: optional FK to `Taxonomy` to be populated after import
    """

    list_name = models.CharField(max_length=50)
    legacy_taxon_name_id = models.CharField(max_length=255)
    legacy_canonical_name = models.CharField(max_length=255)
    taxon_name_id = models.PositiveIntegerField()
    taxonomy = models.ForeignKey(
        "boranga.Taxonomy",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="legacy_mappings",
    )

    class Meta:
        app_label = "boranga"
        # Once data is populated and all rows have a unique value re-enable this constraint
        # unique_together = (("list_name", "legacy_taxon_name_id"),)
        indexes = [
            models.Index(fields=["list_name"], name="idx_legacytax_listname"),
            models.Index(fields=["taxon_name_id"], name="idx_legacytax_taxonid"),
        ]

    def __str__(self):
        tgt = self.taxonomy or self.taxon_name_id
        return f"{self.list_name}:{self.legacy_taxon_name_id}:{self.legacy_canonical_name} -> {tgt}"
