import json
import logging
import os
from decimal import Decimal

import reversion
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.files.storage import FileSystemStorage
from django.core.validators import MinValueValidator
from django.db import models, transaction
from django.http import HttpRequest
from django.utils.functional import cached_property
from ledger_api_client.managed_models import SystemGroup
from multiselectfield import MultiSelectField
from ordered_model.models import OrderedModel

from boranga.components.main.models import (
    ArchivableModel,
    BaseModel,
    CommunicationsLogEntry,
    Document,
    OrderedArchivableManager,
    RevisionedMixin,
    UserAction,
)
from boranga.components.main.related_item import RelatedItem
from boranga.helpers import (
    abbreviate_species_name,
    filefield_exists,
    is_species_communities_approver,
    no_commas_validator,
)
from boranga.ledger_api_utils import retrieve_email_user
from boranga.settings import GROUP_NAME_SPECIES_COMMUNITIES_APPROVER

logger = logging.getLogger(__name__)


private_storage = FileSystemStorage(
    location=settings.BASE_DIR + "/private-media/", base_url="/private-media/"
)


def update_species_doc_filename(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/species/{instance.species.id}/species_documents/{filename}"


def update_community_doc_filename(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/community/{instance.community.id}/community_documents/{filename}"


def update_species_comms_log_filename(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/species/{instance.log_entry.species.id}/communications/{filename}"


def update_community_comms_log_filename(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/community/{instance.log_entry.community.id}/communications/{filename}"


def _sum_area_of_occupancy_m2(obj, owner_field: str):
    """
    Sum area (in metres squared) for occurrence geometries related to an object.

    Note: Since fauna are based on points (which have no area), the front end option to auto
    calculate area of occupancy has been hidden for fauna species.

    owner_field: 'species' or 'community' - used to build filters for Occurrence relations.
    Returns 0 when no area can be determined.
    """
    from django.db.models import F, Func

    from boranga.components.occurrence.models import Occurrence, OccurrenceGeometry

    # Fauna uses buffered geometries for area calculation
    if (
        getattr(obj, "group_type", None)
        and obj.group_type.name == GroupType.GROUP_TYPE_FAUNA
    ):
        logger.warning(
            "_sum_area_of_occupancy_m2 called for fauna species which doesn't make sense as "
            "they are point based. Returnning 0."
        )
        return 0

    base_filter = {
        f"occurrence__{owner_field}": obj,
        "occurrence__processing_status__in": [Occurrence.PROCESSING_STATUS_ACTIVE],
    }

    qs = OccurrenceGeometry.objects.filter(**base_filter)

    # Use PostGIS to sum areas in the DB (geometry::geography to get metres)
    # SUM(ST_Area(geometry::geography)) per-row area aggregation
    area_expr = Func(
        F("geometry"),
        function="ST_Area",
        template="ST_Area(%(expressions)s::geography)",
    )
    res = qs.aggregate(area=models.Sum(area_expr, output_field=models.FloatField()))
    area = res.get("area")

    if not area:
        return 0

    try:
        return float(area)
    except Exception:
        return 0


def _convex_hull_area_m2(obj, owner_field: str):
    """
    Compute the convex hull of all occurrence geometries related to obj and return its
    geodesic area in square metres. Returns 0 on error or if no geometries.
    """
    from django.db.models import Aggregate, F

    from boranga.components.occurrence.models import Occurrence, OccurrenceGeometry

    qs_kwargs = {
        f"occurrence__{owner_field}": obj,
        "occurrence__processing_status__in": [Occurrence.PROCESSING_STATUS_ACTIVE],
    }

    qs = OccurrenceGeometry.objects.filter(**qs_kwargs)

    # Use PostGIS to compute ST_Area(ST_ConvexHull(ST_Collect(geometry))::geography)
    # Use a generic Aggregate with a custom template so Django emits a single aggregated expression
    collect_agg = Aggregate(
        F("geometry"),
        function="ST_Collect",
        template="ST_Area(ST_ConvexHull(ST_Collect(%(expressions)s))::geography)",
        output_field=models.FloatField(),
    )
    res = qs.aggregate(area=collect_agg)
    area = res.get("area")
    if not area:
        return 0

    try:
        return float(area)
    except Exception:
        return 0


class Region(BaseModel):
    name = models.CharField(
        unique=True, default=None, max_length=200, validators=[no_commas_validator]
    )
    forest_region = models.BooleanField(default=False)

    class Meta:
        app_label = "boranga"
        ordering = ["name"]

    def __str__(self):
        return self.name


class District(BaseModel):
    name = models.CharField(
        unique=True, max_length=200, validators=[no_commas_validator]
    )
    code = models.CharField(unique=True, max_length=3, null=True)
    region = models.ForeignKey(
        Region, on_delete=models.CASCADE, related_name="districts"
    )
    archive_date = models.DateField(null=True, blank=True)

    class Meta:
        app_label = "boranga"
        ordering = ["name"]

    def __str__(self):
        return self.name


class GroupType(BaseModel):
    """
    The three types of group managed by Boranga: fauna, flora and communities. These are the basis
    for all other models in Species and Communities.

    Has a:
    - N/A
    Used by:
    - Species
    - Community
    Is:
    - Enumeration (GroupTypes)
    """

    GROUP_TYPE_FLORA = "flora"
    GROUP_TYPE_FAUNA = "fauna"
    GROUP_TYPE_COMMUNITY = "community"
    GROUP_TYPES = [
        (GROUP_TYPE_FLORA, "Flora"),
        (GROUP_TYPE_FAUNA, "Fauna"),
        (GROUP_TYPE_COMMUNITY, "Community"),
    ]
    name = models.CharField(
        max_length=64,
        choices=GROUP_TYPES,
        default=GROUP_TYPES[1],
        verbose_name="GroupType Name",
    )

    class Meta:
        app_label = "boranga"
        verbose_name = "Group Type"
        verbose_name_plural = "Group Types"

    def __str__(self):
        return self.get_name_display()

    @property
    def flora_kingdoms(self):
        return Kingdom.objects.get(
            grouptype__name=GroupType.GROUP_TYPE_FLORA
        ).value_list("kingdom_name", flat=True)

    @property
    def fauna_kingdoms(self):
        return Kingdom.objects.get(
            grouptype__name=GroupType.GROUP_TYPE_FAUNA
        ).value_list("kingdom_name", flat=True)


class Kingdom(BaseModel):
    """
    create GroupType related Kingdoms matching the NOMOS api kingdom name
    """

    grouptype = models.ForeignKey(
        GroupType,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="kingdoms",
    )
    kingdom_id = models.CharField(
        max_length=100, null=True, blank=True, unique=True
    )  # nomos data
    kingdom_name = models.CharField(max_length=100, null=True, blank=True)  # nomos data

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return self.kingdom_name


class Genus(BaseModel):
    """
    # list derived from WACensus

    Used by:
    - Taxonomy

    """

    name = models.CharField(max_length=200, blank=False, unique=True)

    class Meta:
        app_label = "boranga"
        verbose_name = "Genus"
        verbose_name_plural = "Genera"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)


class TaxonomyRank(BaseModel):
    """
    Description from wacensus, to get the Kingdomwise taxon rank for particular taxon_name_id

    Used by:
    - Taxonomy
    Is:
    - Table
    """

    kingdom_id = models.IntegerField(null=True, blank=True)  # nomos data
    kingdom_fk = models.ForeignKey(
        Kingdom, on_delete=models.SET_NULL, null=True, blank=True, related_name="ranks"
    )
    taxon_rank_id = models.IntegerField(
        null=True, blank=True, unique=True
    )  # nomos data
    rank_name = models.CharField(max_length=512, null=True, blank=True)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return str(self.rank_name)


# --- Custom Taxonomy QuerySet and Manager ---
class TaxonomyQuerySet(models.QuerySet):
    def active(self):
        return self.filter(archived=False)


class TaxonomyManager(models.Manager):
    def get_queryset(self):
        return TaxonomyQuerySet(self.model, using=self._db).active()


class Taxonomy(BaseModel):
    """
    Description from wacensus, to get the main name then fill in everything else

    Has a:
    Used by:
    - Species
    Is:
    - Table
    """

    # Manager setup: `objects` excludes archived records by default; use `all_objects` to include archived
    objects = TaxonomyManager()
    all_objects = models.Manager()

    taxon_name_id = models.IntegerField(null=True, blank=True, unique=True)
    scientific_name = models.CharField(max_length=512, null=True, blank=True)
    kingdom_id = models.IntegerField(null=True, blank=True)
    kingdom_fk = models.ForeignKey(
        Kingdom, on_delete=models.SET_NULL, null=True, blank=True, related_name="taxons"
    )
    kingdom_name = models.CharField(max_length=512, null=True, blank=True)
    taxon_rank_id = models.IntegerField(null=True, blank=True)
    taxonomy_rank_fk = models.ForeignKey(
        TaxonomyRank,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="taxons",
    )
    is_current = models.BooleanField(default=True)
    name_authority = models.CharField(max_length=500, null=True, blank=True)
    name_comments = models.CharField(max_length=500, null=True, blank=True)
    # storing the hierarchy id and scientific_names(class,family,genus) at the moment
    family_id = models.IntegerField(null=True, blank=True)
    family_name = models.CharField(max_length=512, null=True, blank=True)
    class_id = models.IntegerField(null=True, blank=True)
    class_name = models.CharField(max_length=512, null=True, blank=True)
    genera_id = models.IntegerField(null=True, blank=True)
    genera_name = models.CharField(max_length=512, null=True, blank=True)

    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)

    archived = models.BooleanField(default=False)

    class Meta:
        app_label = "boranga"
        ordering = ["scientific_name"]
        verbose_name = "Taxonomy"
        verbose_name_plural = "Taxonomies"
        indexes = [
            models.Index(fields=["kingdom_id"], name="idx_taxonomy_kingdom_id"),
            models.Index(fields=["is_current"], name="idx_taxonomy_is_current"),
            models.Index(fields=["archived"], name="idx_taxonomy_archived"),
        ]

    def __str__(self):
        return str(self.scientific_name)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    @property
    def taxon_previous_name(self):
        if hasattr(self, "previous_names") and self.previous_names.exists():
            previous_names_list = TaxonPreviousName.objects.filter(
                taxonomy=self.id
            ).values_list("previous_scientific_name", flat=True)
            return ",".join(previous_names_list)
        else:
            return ""

    @property
    def taxon_previous_queryset(self):
        if hasattr(self, "new_taxon") and self.new_taxon.exists():
            previous_queryset = TaxonPreviousName.objects.filter(
                taxonomy=self.id
            ).order_by("id")
            return previous_queryset
        else:
            return TaxonPreviousName.objects.none()

    @property
    def taxon_vernacular_name(self):
        if hasattr(self, "vernaculars") and self.vernaculars.exists():
            vernacular_names_list = TaxonVernacular.objects.filter(
                taxonomy=self.id
            ).values_list("vernacular_name", flat=True)
            return ",".join(vernacular_names_list)
        else:
            return ""


class TaxonVernacular(BaseModel):
    """
    Common Name for Taxon i.e Species(flora/Fauna)
    Used by:
    -Taxonomy
    """

    vernacular_id = models.IntegerField(null=True, blank=True, unique=True)
    vernacular_name = models.CharField(max_length=512, null=True, blank=True)
    taxonomy = models.ForeignKey(
        Taxonomy, on_delete=models.CASCADE, null=True, related_name="vernaculars"
    )
    taxon_name_id = models.IntegerField(null=True, blank=True)

    class Meta:
        app_label = "boranga"
        ordering = ["vernacular_name"]

    def __str__(self):
        return str(self.vernacular_name)


class TaxonPreviousName(BaseModel):
    """
    Previous Name(old name) of taxon
    """

    taxonomy = models.ForeignKey(
        Taxonomy, on_delete=models.CASCADE, null=True, related_name="previous_names"
    )
    previous_name_id = models.IntegerField(null=True, blank=True, unique=True)
    previous_scientific_name = models.CharField(max_length=512, null=True, blank=True)
    # FK to previous taxonomy record if it exists in Boranga
    previous_taxonomy = models.ForeignKey(
        Taxonomy,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="previous_name_from",
    )

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return str(self.previous_scientific_name)


class ClassificationSystem(BaseModel):
    """
    Classification Suystem for a taxon

    Used by:
    -InformalGroup
    """

    classification_system_id = models.IntegerField(null=True, blank=True, unique=True)
    class_desc = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        app_label = "boranga"
        ordering = ["class_desc"]

    def __str__(self):
        return str(self.class_desc)


class InformalGroup(BaseModel):
    """
    Classification informal group of taxon which is also derived from taxon
    """

    # may need to add the classisfication system id
    classification_system_id = models.IntegerField(null=True, blank=True)
    classification_system_fk = models.ForeignKey(
        ClassificationSystem,
        on_delete=models.CASCADE,
        null=True,
        related_name="informal_groups",
    )
    taxon_name_id = models.IntegerField(null=True, blank=True)
    taxonomy = models.ForeignKey(
        Taxonomy, on_delete=models.CASCADE, null=True, related_name="informal_groups"
    )

    class Meta:
        app_label = "boranga"
        indexes = [
            models.Index(
                fields=["taxonomy_id", "classification_system_fk_id"],
                name="idx_ig_taxonomy_classsys",
            ),
        ]

    def __str__(self):
        return f"{self.classification_system_fk.class_desc} {self.taxonomy.scientific_name} "


class FaunaGroup(ArchivableModel, OrderedModel, BaseModel):
    objects = OrderedArchivableManager()

    name = models.CharField(
        unique=True, max_length=255, validators=[no_commas_validator]
    )

    class Meta:
        app_label = "boranga"
        ordering = ["order"]
        verbose_name = "Fauna Group"
        verbose_name_plural = "Fauna Groups"

    def __str__(self):
        return str(self.name)


class FaunaSubGroup(ArchivableModel, OrderedModel, BaseModel):
    objects = OrderedArchivableManager()

    name = models.CharField(
        unique=True, max_length=255, validators=[no_commas_validator]
    )
    fauna_group = models.ForeignKey(
        FaunaGroup, on_delete=models.CASCADE, related_name="sub_groups"
    )
    order_with_respect_to = "fauna_group"

    class Meta:
        app_label = "boranga"
        ordering = ["fauna_group", "order"]
        verbose_name = "Fauna Sub Group"
        verbose_name_plural = "Fauna Sub Groups"

    def __str__(self):
        return str(self.name)


class Species(RevisionedMixin):
    """
    Forms the basis for a Species and Communities record.

    Has a:
    - ConservationStatus
    - GroupType
    - SpeciesDocument
    - ConservationThreat
    - ConservationPlan
    - Taxonomy
    - Distribution
    - ConservationAttributes
    Used by:
    - Communities
    Is:
    - Table
    """

    PROCESSING_STATUS_DRAFT = "draft"
    PROCESSING_STATUS_DISCARDED = "discarded"
    PROCESSING_STATUS_ACTIVE = "active"
    PROCESSING_STATUS_HISTORICAL = "historical"
    PROCESSING_STATUS_CHOICES = (
        (PROCESSING_STATUS_DRAFT, "Draft"),
        (PROCESSING_STATUS_DISCARDED, "Discarded"),
        (PROCESSING_STATUS_ACTIVE, "Active"),
        (PROCESSING_STATUS_HISTORICAL, "Historical"),
    )
    RELATED_ITEM_CHOICES = [
        ("parent_species", "Species"),
        ("conservation_status", "Conservation Status"),
        ("occurrences", "Occurrence"),
        ("occurrence_report", "Occurrence Report"),
    ]

    SPLIT_SPECIES_ACTION_RETAINED = "Retained"
    SPLIT_SPECIES_ACTION_CREATED = "Created"
    SPLIT_SPECIES_ACTION_ACTIVATED = "Activated"
    SPLIT_SPECIES_ACTION_REACTIVATED = "Reactivated"

    COMBINE_SPECIES_ACTION_RETAINED = "Retained"
    COMBINE_SPECIES_ACTION_CREATED = "Created"
    COMBINE_SPECIES_ACTION_ACTIVATED = "Activated"
    COMBINE_SPECIES_ACTION_REACTIVATED = "Reactivated"
    COMBINE_SPECIES_ACTION_MADE_HISTORICAL = "Made Historical"
    COMBINE_SPECIES_ACTION_DISCARDED = "Discarded"
    COMBINE_SPECIES_ACTION_LEFT_AS_HISTORICAL = "Left as Historical"

    species_number = models.CharField(max_length=9, blank=True, default="")
    group_type = models.ForeignKey(GroupType, on_delete=models.CASCADE)

    taxonomy = models.OneToOneField(
        Taxonomy, on_delete=models.SET_NULL, null=True, blank=True
    )

    image_doc = models.ForeignKey(
        "SpeciesDocument",
        default=None,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="species_image",
    )
    regions = models.ManyToManyField(Region, blank=True, related_name="species_regions")
    districts = models.ManyToManyField(
        District, blank=True, related_name="species_districts"
    )
    last_data_curation_date = models.DateField(blank=True, null=True)
    conservation_plan_exists = models.BooleanField(default=False)
    conservation_plan_reference = models.CharField(
        max_length=500, null=True, blank=True
    )
    processing_status = models.CharField(
        "Processing Status",
        max_length=30,
        choices=PROCESSING_STATUS_CHOICES,
        default=PROCESSING_STATUS_CHOICES[0][0],
        null=True,
        blank=True,
    )
    lodgement_date = models.DateTimeField(blank=True, null=True)
    submitter = models.IntegerField(null=True, blank=True)  # EmailUserRO
    # parents will the original species  from the split/combine functionality
    parent_species = models.ManyToManyField("self", blank=True)
    comment = models.CharField(max_length=500, null=True, blank=True)
    department_file_numbers = models.CharField(max_length=512, null=True, blank=True)
    fauna_group = models.ForeignKey(
        FaunaGroup,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="species",
    )
    fauna_sub_group = models.ForeignKey(
        FaunaSubGroup,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="species",
    )
    # Field to use when importing data from the legacy system
    migrated_from_id = models.CharField(max_length=50, blank=True, default="")
    # Track which data-migration run created/modified this record (nullable)
    migration_run = models.ForeignKey(
        "boranga.MigrationRun",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="migration_run_species",
        db_index=True,
    )

    class Meta:
        app_label = "boranga"
        verbose_name = "Species"
        verbose_name_plural = "Species"

    def __str__(self):
        return f"{self.species_number}"

    def save(self, *args, **kwargs):
        cache.delete("get_species_data")
        self.full_clean()
        # Prefix "S" char to species_number.
        if self.species_number == "":
            force_insert = kwargs.pop("force_insert", False)
            super().save(no_revision=True, force_insert=force_insert)
            new_species_id = f"S{str(self.pk)}"
            self.species_number = new_species_id
            self.save(*args, **kwargs)
        else:
            super().save(*args, **kwargs)
        # Enforce eoo_auto False for fauna species on save (no signals).
        try:
            if (
                getattr(self, "group_type", None)
                and getattr(self.group_type, "name", None) == GroupType.GROUP_TYPE_FAUNA
            ):
                try:
                    dist = self.species_distribution
                except SpeciesDistribution.DoesNotExist:
                    dist = None

                if dist and dist.aoo_actual_auto is not False:
                    dist.aoo_actual_auto = False
                    # Save distribution so frontend-provided values cannot persist for fauna
                    dist.save()
        except Exception:
            # Log but do not raise to avoid breaking save flows
            logger.exception("Failed to enforce eoo_auto for fauna in Species.save()")

    @property
    def reference(self):
        return f"{self.species_number}"

    @property
    def applicant(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return f"{email_user.first_name} {email_user.last_name}"

    @property
    def applicant_email(self):
        if self.submitter:
            return self.email_user.email

    @property
    def applicant_details(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return "{} {}".format(
                email_user.first_name,
                email_user.last_name,
            )

    @property
    def applicant_address(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return email_user.residential_address

    @property
    def applicant_id(self):
        if self.submitter:
            return self.email_user.id

    @property
    def applicant_type(self):
        if self.submitter:
            return "SUB"

    @property
    def applicant_field(self):
        return "submitter"

    @property
    def can_user_edit(self):
        return self.processing_status in [
            Species.PROCESSING_STATUS_DRAFT,
            Species.PROCESSING_STATUS_DISCARDED,
        ]

    @property
    def can_user_rename(self):
        return self.processing_status == Species.PROCESSING_STATUS_ACTIVE

    @property
    def can_user_split(self):
        """
        :return: True if the application is in one of the editable status.
        """
        user_editable_state = [
            Species.PROCESSING_STATUS_ACTIVE,
        ]
        return self.processing_status in user_editable_state

    @property
    def can_user_view(self):
        """
        :return: True if the application is in one of the approved status.
        """
        user_viewable_state = [
            Species.PROCESSING_STATUS_ACTIVE,
            Species.PROCESSING_STATUS_HISTORICAL,
        ]
        return self.processing_status in user_viewable_state

    @property
    def can_user_action(self):
        """
        :return: True if the application is in one of the processable status for Assessor(species) role.
        """
        officer_view_state = [
            Species.PROCESSING_STATUS_DRAFT,
            Species.PROCESSING_STATUS_HISTORICAL,
        ]
        if self.processing_status in officer_view_state:
            return False
        else:
            return True

    @property
    def is_deletable(self):
        return (
            self.processing_status == Species.PROCESSING_STATUS_DRAFT
            and not self.species_number
        )

    @property
    def is_flora_application(self):
        if self.group_type.name == GroupType.GROUP_TYPE_FLORA:
            return True
        return False

    @property
    def is_fauna_application(self):
        if self.group_type.name == GroupType.GROUP_TYPE_FAUNA:
            return True
        return False

    # used in split email template
    @property
    def child_species(self):
        child_species = Species.objects.filter(parent_species=self)
        return child_species

    # used in split/combine email template
    @property
    def parent_species_list(self):
        parent_species = self.parent_species.all()
        return parent_species

    def get_approver_group(self):
        return SystemGroup.objects.get(name=GROUP_NAME_SPECIES_COMMUNITIES_APPROVER)

    @property
    def status_without_assessor(self):
        status_without_assessor = [
            Species.PROCESSING_STATUS_WITH_APPROVER,
            Species.PROCESSING_STATUS_APPROVED,
            Species.PROCESSING_STATUS_CLOSED,
            Species.PROCESSING_STATUS_DECLINED,
            Species.PROCESSING_STATUS_DRAFT,
            Species.PROCESSING_STATUS_WITH_REFERRAL,
        ]
        if self.processing_status in status_without_assessor:
            return True
        return False

    @transaction.atomic
    def remove(self, request):
        # Only used to remove a species such as those that are created automatically
        # When the 'Split' action is taken on a species.
        if not self.processing_status == self.PROCESSING_STATUS_DRAFT:
            raise ValueError("Species must be in draft status to be removed")

        if not is_species_communities_approver(request):
            raise ValueError("User does not have permission to remove species")

        # Log the action
        self.log_user_action(
            SpeciesUserAction.ACTION_DISCARD_SPECIES.format(self.species_number),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            SpeciesUserAction.ACTION_DISCARD_SPECIES.format(self.species_number),
            request,
        )

        self.delete()

    @cached_property
    def approved_conservation_status(self):
        # Careful with this as it is cached for the duration of the life of the object (most likely the request)
        # Using it to reduce queries in the species list view
        from boranga.components.conservation_status.models import ConservationStatus

        return self.conservation_status.filter(
            processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED
        ).first()

    def can_user_save(self, request):
        user_closed_state = [
            Species.PROCESSING_STATUS_HISTORICAL,
            Species.PROCESSING_STATUS_DISCARDED,
        ]

        if self.processing_status in user_closed_state:
            return False

        return is_species_communities_approver(request) or request.user.is_superuser

    def can_user_submit(self, request):
        user_submissable_state = [Species.PROCESSING_STATUS_DRAFT]

        if not self.can_user_save(request):
            return False

        if self.processing_status not in user_submissable_state:
            return False

        return is_species_communities_approver(request) or request.user.is_superuser

    def has_user_edit_mode(self, request):
        officer_view_state = [
            Species.PROCESSING_STATUS_DRAFT,
            Species.PROCESSING_STATUS_HISTORICAL,
        ]
        if self.processing_status in officer_view_state:
            return False

        return is_species_communities_approver(request) or request.user.is_superuser

    def get_related_items(
        self, filter_type, offset=None, limit=None, search_value=None, **kwargs
    ):
        return_list = []
        if filter_type == "all":
            related_field_names = [
                "parent_species",
                "conservation_status",
                "occurrences",
                "occurrence_report",
            ]
        elif filter_type == "all_except_parent_species":
            related_field_names = [
                "conservation_status",
                "occurrences",
                "occurrence_report",
            ]
        elif filter_type == "conservation_status_and_occurrences":
            related_field_names = [
                "conservation_status",
                "occurrences",
            ]
        elif filter_type == "for_occurrence":
            related_field_names = [
                "parent_species",
                "conservation_status",
                "occurrences",
            ]
        else:
            related_field_names = [
                filter_type,
            ]

        total_count = 0

        def get_slice_range(count):
            if offset is None:
                return 0, count

            global_start = total_count
            global_end = total_count + count
            req_start = int(offset)
            req_end = int(offset) + int(limit)

            if global_end <= req_start or global_start >= req_end:
                return 0, 0

            start = max(0, req_start - global_start)
            end = min(count, req_end - global_start)
            return start, end

        all_fields = self._meta.get_fields()
        for a_field in all_fields:
            if a_field.name in related_field_names:
                field_objects = []
                is_queryset = False
                if a_field.is_relation:
                    if a_field.many_to_many:
                        field_objects = a_field.related_model.objects.filter(
                            **{a_field.remote_field.name: self}
                        )
                        is_queryset = True
                    elif a_field.many_to_one:  # foreign key
                        val = getattr(self, a_field.name)
                        if val:
                            field_objects = [val]
                        else:
                            field_objects = []
                    elif a_field.one_to_many:  # reverse foreign key
                        qs = a_field.related_model.objects.filter(
                            **{a_field.remote_field.name: self}
                        )
                        field_objects = qs
                        is_queryset = True
                    elif a_field.one_to_one:
                        if hasattr(self, a_field.name):
                            field_objects = [
                                getattr(self, a_field.name),
                            ]

                count = 0
                if is_queryset:
                    if "exclude_ids" in kwargs:
                        field_objects = field_objects.exclude(
                            id__in=kwargs["exclude_ids"]
                        )
                    count = field_objects.count()
                else:
                    count = len(field_objects)

                start, end = get_slice_range(count)

                if start < end:
                    subset = field_objects[start:end]
                    for field_object in subset:
                        if field_object:
                            related_item = field_object.as_related_item
                            if search_value:
                                if (
                                    search_value.lower()
                                    not in related_item.identifier.lower()
                                    and search_value.lower()
                                    not in related_item.descriptor.lower()
                                ):
                                    continue
                            return_list.append(related_item)

                total_count += count

                # Add parent species related items to the list (limited to one degree of separation)
                if a_field.name == "parent_species":
                    for parent_species in self.parent_species.all():
                        if filter_type == "for_occurrence":
                            items = parent_species.get_related_items(
                                "conservation_status_and_occurrences"
                            )
                        else:
                            items = parent_species.get_related_items(
                                "all_except_parent_species"
                            )

                        count = len(items)
                        start, end = get_slice_range(count)
                        if start < end:
                            return_list.extend(items[start:end])
                        total_count += count

        # Remove duplicates
        if offset is None:
            return_list = list(set(return_list))
            return return_list

        return return_list, total_count

    @property
    def as_related_item(self):
        related_item = RelatedItem(
            identifier=self.related_item_identifier,
            model_name=self._meta.verbose_name.title(),
            descriptor=self.related_item_descriptor,
            status=self.related_item_status,
            action_url=(
                f'<a href="/internal/species-communities/{self.id}'
                f'?group_type_name={self.group_type.name}" target="_blank">View '
                '<i class="bi bi-box-arrow-up-right"></i></a>'
            ),
            related_sc_id=self.species_number,
        )
        return related_item

    @property
    def related_item_identifier(self):
        return self.species_number

    @property
    def related_item_descriptor(self):
        if self.taxonomy and self.taxonomy.scientific_name:
            return abbreviate_species_name(self.taxonomy.scientific_name)
        return "Descriptor not available"

    @property
    def related_item_status(self):
        return self.get_processing_status_display()

    @property
    def submitter_user(self):
        email_user = retrieve_email_user(self.submitter)

        return email_user

    def log_user_action(self, action, request):
        return SpeciesUserAction.log_action(self, action, request.user.id)

    @transaction.atomic
    def upload_image(self, speciesCommunitiesImageFile):
        document = SpeciesDocument(
            name=speciesCommunitiesImageFile.name,
            input_name="speciesCommunitiesImageFile",
            _file=speciesCommunitiesImageFile,
            species=self,
        )
        document.check_file(speciesCommunitiesImageFile)
        document.save()
        self.image_doc = document
        self.save()

    @property
    def image_history(self):
        images_qs = (
            SpeciesDocument.objects.filter(
                species=self, input_name="speciesCommunitiesImageFile"
            )
            .distinct("uploaded_date", "_file")
            .order_by("-uploaded_date")
        )
        return [
            {
                "id": image.id,
                "filename": image.name,
                "url": image._file.url,
                "uploaded_date": image.uploaded_date,
            }
            for image in images_qs
        ]

    def reinstate_image(self, pk):
        try:
            document = SpeciesDocument.objects.get(pk=pk)
        except SpeciesDocument.DoesNotExist:
            raise ValidationError(f"No SpeciesDocument model found with pk {pk}")

        self.image_doc = document
        self.save()

    def image_exists(self) -> bool:
        """Return True if the Species image_doc file exists in storage."""
        if not self.image_doc:
            return False
        return filefield_exists(self.image_doc)

    @transaction.atomic
    def copy_split_documents(
        self: "Species",
        copy_from: "Species",
        request: HttpRequest,
        split_species: object,
        split_species_is_original=False,
    ) -> None:

        document_ids_queryset = copy_from.species_documents.all()
        if not split_species["copy_all_documents"]:
            document_ids_queryset = document_ids_queryset.filter(
                id__in=split_species["document_ids_to_copy"]
            )
        document_ids_to_copy = document_ids_queryset.values_list("id", flat=True)
        if split_species_is_original:
            # If this is the original species, discard any documents that are not in the request data
            documents_to_discard = self.species_documents.exclude(
                id__in=document_ids_to_copy
            )
            for document in documents_to_discard:
                document.active = False
                document.save(version_user=request.user)
                document.species.log_user_action(
                    SpeciesUserAction.ACTION_DISCARD_DOCUMENT.format(
                        document.document_number,
                        document.species.species_number,
                    ),
                    request,
                )
                request.user.log_user_action(
                    SpeciesUserAction.ACTION_DISCARD_DOCUMENT.format(
                        document.document_number,
                        document.species.species_number,
                    ),
                    request,
                )
            return

        for doc_id in document_ids_to_copy:
            new_species_doc = SpeciesDocument.objects.get(id=doc_id)
            new_species_doc.species = self
            new_species_doc.id = None
            new_species_doc.active = True
            new_species_doc.document_number = ""
            new_species_doc.save(version_user=request.user)
            new_species_doc.species.log_user_action(
                SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                    new_species_doc.document_number,
                    new_species_doc.species.species_number,
                ),
                request,
            )
            request.user.log_user_action(
                SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                    new_species_doc.document_number,
                    new_species_doc.species.species_number,
                ),
                request,
            )

    @transaction.atomic
    def copy_split_threats(
        self: "Species",
        copy_from: "Species",
        request: HttpRequest,
        split_species: object,
        split_species_is_original=False,
    ) -> None:
        threat_ids_queryset = copy_from.species_threats.all()
        if not split_species["copy_all_documents"]:
            threat_ids_queryset = threat_ids_queryset.filter(
                id__in=split_species["threat_ids_to_copy"]
            )
        threat_ids_to_copy = threat_ids_queryset.values_list("id", flat=True)

        if split_species_is_original:
            # If this is the original species, discard any threats that are not in the request data
            threats_to_discard = self.species_threats.exclude(id__in=threat_ids_to_copy)
            for threat in threats_to_discard:
                threat.visible = False
                threat.save(version_user=request.user)
                threat.species.log_user_action(
                    SpeciesUserAction.ACTION_DISCARD_THREAT.format(
                        threat.threat_number,
                        threat.species.species_number,
                    ),
                    request,
                )
                request.user.log_user_action(
                    SpeciesUserAction.ACTION_DISCARD_THREAT.format(
                        threat.threat_number,
                        threat.species.species_number,
                    ),
                    request,
                )
            return

        for threat_id in threat_ids_to_copy:
            new_species_threat = ConservationThreat.objects.get(id=threat_id)
            new_species_threat.species = self
            new_species_threat.id = None
            new_species_threat.visible = True
            new_species_threat.threat_number = ""
            new_species_threat.save()
            new_species_threat.species.log_user_action(
                SpeciesUserAction.ACTION_ADD_THREAT.format(
                    new_species_threat.threat_number,
                    new_species_threat.species.species_number,
                ),
                request,
            )
            request.user.log_user_action(
                SpeciesUserAction.ACTION_ADD_THREAT.format(
                    new_species_threat.threat_number,
                    new_species_threat.species.species_number,
                ),
                request,
            )

    @transaction.atomic
    def copy_combine_documents_and_threats(
        self, selection: dict, request: HttpRequest
    ) -> None:
        if not selection:
            return

        documents_map = selection.get("documents") or {}
        threats_map = selection.get("threats") or {}

        def _int(v):
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        for source_key, cfg in documents_map.items():
            source_id = _int(source_key)
            if not source_id or source_id == self.id:
                continue
            mode = (cfg or {}).get("mode")
            ids = (cfg or {}).get("ids") or []

            qs = SpeciesDocument.objects.filter(species_id=source_id)

            if mode == "individual":
                if not ids:
                    continue
                qs = qs.filter(id__in=ids)

            for doc in qs:
                doc.pk = None
                doc.id = None
                doc.species = self
                doc.document_number = ""
                doc.save(version_user=request.user)
                self.log_user_action(
                    SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                        doc.document_number,
                        self.species_number,
                    ),
                    request,
                )
                request.user.log_user_action(
                    SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                        doc.document_number,
                        self.species_number,
                    ),
                    request,
                )

        for source_key, cfg in threats_map.items():
            source_id = _int(source_key)
            if not source_id or source_id == self.id:
                continue
            mode = (cfg or {}).get("mode")
            ids = (cfg or {}).get("ids") or []

            qs = ConservationThreat.objects.filter(species_id=source_id, visible=True)
            if mode == "individual":
                if not ids:
                    continue
                qs = qs.filter(id__in=ids)

            for threat in qs:
                threat.pk = None
                threat.id = None
                threat.species = self
                threat.threat_number = ""
                threat.visible = True
                threat.save(version_user=request.user)
                self.log_user_action(
                    SpeciesUserAction.ACTION_ADD_THREAT.format(
                        threat.threat_number,
                        self.species_number,
                    ),
                    request,
                )
                request.user.log_user_action(
                    SpeciesUserAction.ACTION_ADD_THREAT.format(
                        threat.threat_number,
                        self.species_number,
                    ),
                    request,
                )

    @transaction.atomic
    def discard(self, request):
        if not self.processing_status == Species.PROCESSING_STATUS_DRAFT:
            raise ValidationError("You cannot discard a species that is not a draft")

        if self.lodgement_date:
            raise ValidationError(
                "You cannot discard a species that has been submitted"
            )

        if not is_species_communities_approver(request):
            raise ValidationError(
                "You cannot discard a species unless you are a contributor"
            )

        self.processing_status = Species.PROCESSING_STATUS_DISCARDED
        self.save(version_user=request.user)

        # Log proposal action
        self.log_user_action(
            SpeciesUserAction.ACTION_DISCARD_SPECIES.format(self.species_number),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            SpeciesUserAction.ACTION_DISCARD_SPECIES.format(self.species_number),
            request,
        )

    def reinstate(self, request):
        if not self.processing_status == Species.PROCESSING_STATUS_DISCARDED:
            raise ValidationError(
                "You cannot reinstate a species that is not discarded"
            )

        if not is_species_communities_approver(request):
            raise ValidationError(
                "You cannot reinstate a species unless you are a species communities approver"
            )

        self.processing_status = Species.PROCESSING_STATUS_DRAFT
        self.save(version_user=request.user)

        # Log proposal action
        self.log_user_action(
            SpeciesUserAction.ACTION_REINSTATE_SPECIES.format(self.species_number),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            SpeciesUserAction.ACTION_REINSTATE_SPECIES.format(self.species_number),
            request,
        )

    @property
    def occurrence_count(self):
        from boranga.components.occurrence.models import Occurrence

        return Occurrence.objects.filter(
            species=self,
            processing_status__in=[
                Occurrence.PROCESSING_STATUS_ACTIVE,
            ],
        ).count()

    @property
    def area_of_occupancy_m2(self):
        return _sum_area_of_occupancy_m2(self, "species")

    @property
    def area_of_occupancy_km2(self):
        if not self.area_of_occupancy_m2:
            return 0

        return round(self.area_of_occupancy_m2 / 1000000, 5)

    @property
    def area_occurrence_convex_hull_m2(self):
        return _convex_hull_area_m2(self, "species")

    @property
    def area_occurrence_convex_hull_km2(self):
        if not self.area_occurrence_convex_hull_m2:
            return 0

        return round(self.area_occurrence_convex_hull_m2 / 1000000, 5)


class SpeciesLogDocument(Document):
    log_entry = models.ForeignKey(
        "SpeciesLogEntry", related_name="documents", on_delete=models.CASCADE
    )
    _file = models.FileField(
        upload_to=update_species_comms_log_filename,
        max_length=512,
        storage=private_storage,
    )

    class Meta:
        app_label = "boranga"

    def get_parent_instance(self) -> BaseModel:
        return self.log_entry


class SpeciesLogEntry(CommunicationsLogEntry):
    species = models.ForeignKey(
        Species, related_name="comms_logs", on_delete=models.CASCADE
    )

    def __str__(self):
        return f"{self.reference} - {self.subject}"

    class Meta:
        app_label = "boranga"

    def save(self, **kwargs):
        # save the application reference if the reference not provided
        if not self.reference:
            self.reference = self.species.reference
        super().save(**kwargs)


class SpeciesUserAction(UserAction):

    ACTION_DISCARD_SPECIES = "Discard Species {}"
    ACTION_REINSTATE_SPECIES = "Reinstate Species {}"
    ACTION_EDIT_SPECIES = "Edit Species {}"
    ACTION_CREATE_SPECIES = "Create new species {}"
    ACTION_SAVE_SPECIES = "Save Species {}"
    ACTION_MAKE_HISTORICAL = "Make Species {} historical"
    ACTION_IMAGE_UPDATE = "Species Image document updated for Species {}"
    ACTION_IMAGE_DELETE = "Species Image document deleted for Species {}"
    ACTION_IMAGE_REINSTATE = "Species Image document reinstated for Species {}"

    # Species are copied prior to being renamed so we want to capture this in case the rename
    # Doesn't go through and we end up with orphaned species records we know where they came from
    ACTION_COPY_SPECIES_TO = "Species {} copied to new species {}"
    ACTION_COPY_SPECIES_FROM = "Species {} copied from species {}"

    ACTION_RENAME_SPECIES_TO_NEW = "Species {} renamed to new species {}"
    ACTION_RENAME_SPECIES_TO_EXISTING = "Species {} renamed to existing species {}"

    ACTION_RENAME_SPECIES_FROM_NEW = "Species {} created by renaming species {}"
    ACTION_RENAME_SPECIES_FROM_EXISTING_DRAFT = (
        "Species {} activated by renaming species {}"
    )
    ACTION_RENAME_SPECIES_FROM_EXISTING_HISTORICAL = (
        "Species {} reactivated by renaming species {}"
    )

    ACTION_SPLIT_SPECIES_TO_NEW = "Species {} split into new species {}"
    ACTION_SPLIT_SPECIES_TO_EXISTING = "Species {} split into existing species {}"

    ACTION_SPLIT_SPECIES_FROM_NEW = "Species {} created by splitting species {}"
    ACTION_SPLIT_SPECIES_FROM_EXISTING_DRAFT = (
        "Species {} activated by splitting species {}"
    )
    ACTION_SPLIT_SPECIES_FROM_EXISTING_HISTORICAL = (
        "Species {} reactivated by splitting species {}"
    )

    ACTION_SPLIT_MAKE_ORIGINAL_HISTORICAL = (
        "Species {} made historical as a result of being split"
    )
    ACTION_SPLIT_RETAIN_ORIGINAL = "Species {} retained as part of a split."

    ACTION_COMBINE_ACTIVE_SPECIES_TO_NEW = (
        "Active species {} made historical as a "
        "result of being combined into new species {}"
    )
    ACTION_COMBINE_ACTIVE_SPECIES_TO_EXISTING = (
        "Active species {} made historical as a "
        "result of being combined into existing species {}"
    )
    ACTION_COMBINE_DRAFT_SPECIES_TO_EXISTING = (
        "Draft species {} discarded as a result of "
        "being combined into existing species {}"
    )
    ACTION_COMBINE_DRAFT_SPECIES_TO_NEW = (
        "Draft species {} discarded as a result of "
        "being combined into new species {}"
    )
    ACTION_COMBINE_HISTORICAL_SPECIES_TO_NEW = (
        "Historical species {} was combined into "
        "new species {} and remains historical"
    )
    ACTION_COMBINE_HISTORICAL_SPECIES_TO_EXISTING = (
        "Historical species {} was combined into "
        "existing species {} and remains historical"
    )

    ACTION_COMBINE_SPECIES_FROM_NEW = (
        "Species {} created from a combination of species {}"
    )
    ACTION_COMBINE_SPECIES_FROM_EXISTING_ACTIVE = (
        "Species {} retained from a combination of species {}"
    )
    ACTION_COMBINE_SPECIES_FROM_EXISTING_DRAFT = (
        "Species {} activated from a combination of species {}"
    )
    ACTION_COMBINE_SPECIES_FROM_EXISTING_HISTORICAL = (
        "Species {} reactivated from a combination of species {}"
    )

    # Document
    ACTION_ADD_DOCUMENT = "Document {} added for Species {}"
    ACTION_UPDATE_DOCUMENT = "Document {} updated for Species {}"
    ACTION_DISCARD_DOCUMENT = "Document {} discarded for Species {}"
    ACTION_REINSTATE_DOCUMENT = "Document {} reinstated for Species {}"

    # Threat
    ACTION_ADD_THREAT = "Threat {} added for Species {}"
    ACTION_UPDATE_THREAT = "Threat {} updated for Species {}"
    ACTION_DISCARD_THREAT = "Threat {} discarded for Species {}"
    ACTION_REINSTATE_THREAT = "Threat {} reinstated for Species {}"

    ACTION_DISCARD_PROPOSAL = "Discard species proposal {}"

    class Meta:
        app_label = "boranga"
        ordering = ("-when",)

    @classmethod
    def log_action(cls, species, action, user):
        return cls.objects.create(species=species, who=user, what=str(action))

    species = models.ForeignKey(
        Species, related_name="action_logs", on_delete=models.CASCADE
    )


class SpeciesDistribution(BaseModel):
    """
    All the different locations where this species can be found.

    Used by:
    - Species
    Is:
    - Table
    """

    number_of_occurrences = models.IntegerField(null=True, blank=True)
    noo_auto = models.BooleanField(
        default=True
    )  # to check auto or manual entry of number_of_occurrences
    extent_of_occurrences = models.DecimalField(
        null=True,
        blank=True,
        max_digits=15,
        decimal_places=5,
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    eoo_auto = models.BooleanField(
        default=True
    )  # extra boolean field to check auto or manual entry of extent_of_occurrences
    area_of_occupancy = models.IntegerField(null=True, blank=True)
    area_of_occupancy_actual = models.DecimalField(
        max_digits=15, decimal_places=5, null=True, blank=True
    )
    aoo_actual_auto = models.BooleanField(
        default=True
    )  # to check auto or manual entry of area_of_occupancy_actual
    number_of_iucn_locations = models.IntegerField(null=True, blank=True)
    number_of_iucn_subpopulations = models.IntegerField(null=True, blank=True)
    species = models.OneToOneField(
        Species,
        on_delete=models.CASCADE,
        null=True,
        related_name="species_distribution",
    )
    distribution = models.CharField(max_length=512, null=True, blank=True)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        string = f"Species Distribution {self.id}"
        if self.species:
            string += f" for Species ({self.species})"
        return string

    def save(self, *args, **kwargs):
        """
        Enforce that eoo_auto is only False for fauna species.

        - If this distribution is linked to a Species whose GroupType name is 'fauna',
                - If this distribution is linked to a Species whose GroupType name is 'fauna',
                    set eoo_auto = False.
                - For non-fauna species, do not alter eoo_auto (allow either True/False).
                - If no species is linked, leave eoo_auto unchanged.
        """
        try:
            if self.species and getattr(self.species, "group_type", None):
                if (
                    getattr(self.species.group_type, "name", None)
                    == GroupType.GROUP_TYPE_FAUNA
                ):
                    # Force False for fauna
                    self.aoo_actual_auto = False
                # else: leave eoo_auto unchanged for non-fauna
        except Exception:
            # Be conservative: if anything unexpected happens while checking group_type,
            # don't modify the field and let the save proceed.
            pass

        super().save(*args, **kwargs)


class Community(RevisionedMixin):
    """
    A collection of 2 or more Species within a specific location.

    Has a:
    - GroupType
    - Species
    Used by:
    - N/A
    Is:
    - Table
    """

    PROCESSING_STATUS_DRAFT = "draft"
    PROCESSING_STATUS_DISCARDED = "discarded"
    PROCESSING_STATUS_ACTIVE = "active"
    PROCESSING_STATUS_HISTORICAL = "historical"
    PROCESSING_STATUS_CHOICES = (
        (PROCESSING_STATUS_DRAFT, "Draft"),
        (PROCESSING_STATUS_DISCARDED, "Discarded"),
        (PROCESSING_STATUS_ACTIVE, "Active"),
        (PROCESSING_STATUS_HISTORICAL, "Historical"),
    )
    RELATED_ITEM_CHOICES = [("conservation_status", "Conservation Status")]

    community_number = models.CharField(max_length=9, blank=True, default="")
    renamed_from = models.ForeignKey(
        "self",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="renamed_to",
    )
    group_type = models.ForeignKey(GroupType, on_delete=models.CASCADE)
    species = models.ManyToManyField(Species, blank=True)
    regions = models.ManyToManyField(
        Region, blank=True, related_name="community_regions"
    )
    districts = models.ManyToManyField(
        District, blank=True, related_name="community_districts"
    )
    last_data_curation_date = models.DateField(blank=True, null=True)
    conservation_plan_exists = models.BooleanField(default=False)
    conservation_plan_reference = models.CharField(
        max_length=500, null=True, blank=True
    )
    submitter = models.IntegerField(null=True)  # EmailUserRO
    image_doc = models.ForeignKey(
        "CommunityDocument",
        default=None,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="community_image",
    )
    processing_status = models.CharField(
        "Processing Status",
        max_length=30,
        choices=PROCESSING_STATUS_CHOICES,
        default=PROCESSING_STATUS_CHOICES[0][0],
    )
    lodgement_date = models.DateTimeField(blank=True, null=True)
    comment = models.CharField(max_length=500, null=True, blank=True)
    department_file_numbers = models.CharField(max_length=512, null=True, blank=True)
    # Field to use when importing data from the legacy system
    migrated_from_id = models.CharField(max_length=50, blank=True, default="")

    # Track which data-migration run created/modified this record (nullable)
    migration_run = models.ForeignKey(
        "boranga.MigrationRun",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="migration_run_communities",
        db_index=True,
    )

    class Meta:
        app_label = "boranga"
        verbose_name_plural = "communities"

    def __str__(self):
        return f"{self.community_number}"

    def save(self, *args, **kwargs):
        # Prefix "C" char to community_number.
        if self.community_number == "":
            force_insert = kwargs.pop("force_insert", False)
            super().save(no_revision=True, force_insert=force_insert)
            new_community_id = f"C{str(self.pk)}"
            self.community_number = new_community_id
            self.save(*args, **kwargs)
        else:
            super().save(*args, **kwargs)

    @property
    def applicant(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return f"{email_user.first_name} {email_user.last_name}"

    @property
    def applicant_email(self):
        if self.submitter:
            return self.email_user.email

    @property
    def applicant_details(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return "{} {}".format(
                email_user.first_name,
                email_user.last_name,
            )

    @property
    def applicant_address(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return email_user.residential_address

    @property
    def applicant_id(self):
        if self.submitter:
            return self.email_user.id

    @property
    def applicant_type(self):
        if self.submitter:
            return "SUB"

    @property
    def applicant_field(self):
        return "submitter"

    @property
    def can_user_edit(self):
        return self.processing_status in [
            Community.PROCESSING_STATUS_DRAFT,
            Community.PROCESSING_STATUS_DISCARDED,
        ]

    @property
    def can_user_view(self):
        """
        :return: True if the application is in one of the approved status.
        """
        user_viewable_state = [
            Community.PROCESSING_STATUS_ACTIVE,
            Community.PROCESSING_STATUS_HISTORICAL,
        ]
        return self.processing_status in user_viewable_state

    @property
    def can_user_action(self):
        """
        :return: True if the application is in one of the processable status for Assessor(species) role.
        """
        officer_view_state = [
            Community.PROCESSING_STATUS_DRAFT,
            Community.PROCESSING_STATUS_HISTORICAL,
        ]
        if self.processing_status in officer_view_state:
            return False
        else:
            return True

    def can_user_save(self, request):
        user_closed_state = [
            Community.PROCESSING_STATUS_HISTORICAL,
            Community.PROCESSING_STATUS_DISCARDED,
        ]

        if self.processing_status in user_closed_state:
            return False

        return is_species_communities_approver(request) or request.user.is_superuser

    def can_user_submit(self, request):
        user_submissable_state = [Community.PROCESSING_STATUS_DRAFT]

        if not self.can_user_save(request):
            return False

        if self.processing_status not in user_submissable_state:
            return False

        return is_species_communities_approver(request) or request.user.is_superuser

    @property
    def is_deletable(self):
        """
        An application can be deleted only if it is a draft and it hasn't been lodged yet
        :return:
        """
        return (
            self.processing_status == Community.PROCESSING_STATUS_DRAFT
            and not self.community_number
        )

    @property
    def is_community_application(self):
        if self.group_type.name == GroupType.GROUP_TYPE_COMMUNITY:
            return True
        return False

    def get_approver_group(self):
        return SystemGroup.objects.get(name=GROUP_NAME_SPECIES_COMMUNITIES_APPROVER)

    @property
    def status_without_assessor(self):
        status_without_assessor = [
            Community.PROCESSING_STATUS_WITH_APPROVER,
            Community.PROCESSING_STATUS_APPROVED,
            Community.PROCESSING_STATUS_CLOSED,
            Community.PROCESSING_STATUS_DECLINED,
            Community.PROCESSING_STATUS_DRAFT,
            Community.PROCESSING_STATUS_WITH_REFERRAL,
        ]
        if self.processing_status in status_without_assessor:
            return True
        return False

    def has_user_edit_mode(self, request):
        officer_view_state = [
            Community.PROCESSING_STATUS_DRAFT,
            Community.PROCESSING_STATUS_HISTORICAL,
        ]
        if self.processing_status in officer_view_state:
            return False

        return is_species_communities_approver(request) or request.user.is_superuser

    @property
    def reference(self):
        return f"{self.community_number}-{self.community_number}"

    def get_related_items(
        self, filter_type, offset=None, limit=None, search_value=None, **kwargs
    ):
        return_list = []
        if filter_type == "all":
            related_field_names = [
                "renamed_from",
                "renamed_to",
                "conservation_status",
                "occurrences",
                "occurrence_report",
            ]
        elif filter_type == "all_except_renamed_community":
            related_field_names = [
                "conservation_status",
                "occurrences",
                "occurrence_report",
            ]
        elif filter_type == "conservation_status_and_occurrences":
            related_field_names = [
                "conservation_status",
                "occurrences",
            ]
        elif filter_type == "for_occurrence":
            related_field_names = [
                "renamed_from",
                "renamed_to",
                "conservation_status",
                "occurrences",
            ]
        else:
            related_field_names = [
                filter_type,
            ]

        total_count = 0

        def get_slice_range(count):
            if offset is None:
                return 0, count

            global_start = total_count
            global_end = total_count + count
            req_start = int(offset)
            req_end = int(offset) + int(limit)

            if global_end <= req_start or global_start >= req_end:
                return 0, 0

            start = max(0, req_start - global_start)
            end = min(count, req_end - global_start)
            return start, end

        all_fields = self._meta.get_fields()
        for a_field in all_fields:
            if a_field.name in related_field_names:
                field_objects = []
                is_queryset = False
                if a_field.is_relation:
                    if a_field.many_to_many:
                        field_objects = a_field.related_model.objects.filter(
                            **{a_field.remote_field.name: self}
                        )
                        is_queryset = True
                    elif a_field.many_to_one:  # foreign key
                        val = getattr(self, a_field.name)
                        if val:
                            field_objects = [val]
                        else:
                            field_objects = []
                    elif a_field.one_to_many:  # reverse foreign key
                        qs = a_field.related_model.objects.filter(
                            **{a_field.remote_field.name: self}
                        )
                        field_objects = qs
                        is_queryset = True
                    elif a_field.one_to_one:
                        if hasattr(self, a_field.name):
                            field_objects = [
                                getattr(self, a_field.name),
                            ]

                count = 0
                if is_queryset:
                    if "exclude_ids" in kwargs:
                        field_objects = field_objects.exclude(
                            id__in=kwargs["exclude_ids"]
                        )
                    count = field_objects.count()
                else:
                    count = len(field_objects)

                start, end = get_slice_range(count)

                if start < end:
                    subset = field_objects[start:end]
                    for field_object in subset:
                        if field_object:
                            related_item = field_object.as_related_item
                            if search_value:
                                if (
                                    search_value.lower()
                                    not in related_item.identifier.lower()
                                    and search_value.lower()
                                    not in related_item.descriptor.lower()
                                ):
                                    continue
                            return_list.append(related_item)

                total_count += count

                # Add renamed from related items to the list (limited to one degree of separation)
                if a_field.name == "renamed_from" and self.renamed_from:
                    if filter_type == "for_occurrence":
                        items = self.renamed_from.get_related_items(
                            "conservation_status_and_occurrences"
                        )
                    else:
                        items = self.renamed_from.get_related_items(
                            "all_except_renamed_community"
                        )

                    count = len(items)
                    start, end = get_slice_range(count)
                    if start < end:
                        return_list.extend(items[start:end])
                    total_count += count

                # Add renamed to related items to the list (limited to one degree of separation)
                if a_field.name == "renamed_to" and self.renamed_to.exists():
                    for community in self.renamed_to.select_related("taxonomy").all():
                        if filter_type == "for_occurrence":
                            items = community.get_related_items(
                                "conservation_status_and_occurrences"
                            )
                        else:
                            items = community.get_related_items(
                                "all_except_renamed_community"
                            )

                        count = len(items)
                        start, end = get_slice_range(count)
                        if start < end:
                            return_list.extend(items[start:end])
                        total_count += count

        # Remove duplicates by (model_name, identifier)
        if offset is None:
            seen = set()
            deduped = []
            for it in return_list:
                key = (getattr(it, "model_name", None), getattr(it, "identifier", None))
                if key not in seen:
                    seen.add(key)
                    deduped.append(it)

            return deduped

        return return_list, total_count

    @property
    def as_related_item(self):
        related_item = RelatedItem(
            identifier=self.related_item_identifier,
            model_name=self._meta.verbose_name.title(),
            descriptor=self.related_item_descriptor,
            status=self.related_item_status,
            action_url=(
                f'<a href="/internal/species-communities/{self.id}'
                f'?group_type_name={self.group_type.name}" target="_blank">View '
                '<i class="bi bi-box-arrow-up-right"></i></a>'
            ),
            related_sc_id=self.community_number,
        )
        return related_item

    @property
    def related_item_identifier(self):
        return self.community_number

    @property
    def related_item_descriptor(self):
        if self.taxonomy and self.taxonomy.community_common_id:
            return self.taxonomy.community_common_id
        return "Descriptor not available"

    @property
    def related_item_status(self):
        return self.get_processing_status_display()

    @cached_property
    def approved_conservation_status(self):
        # Careful with this as it is cached for the duration of the life of the object (most likely the request)
        # Using it to reduce queries in the communities list view
        from boranga.components.conservation_status.models import ConservationStatus

        return self.conservation_status.filter(
            processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED
        ).first()

    @transaction.atomic
    def discard(self, request):
        if not self.processing_status == Community.PROCESSING_STATUS_DRAFT:
            raise ValidationError("You cannot discard a community that is not a draft")

        if self.lodgement_date:
            raise ValidationError(
                "You cannot discard a community that has been submitted"
            )

        if not is_species_communities_approver(request):
            raise ValidationError(
                "You cannot discard a community unless you are a contributor"
            )

        self.processing_status = Community.PROCESSING_STATUS_DISCARDED
        self.save(version_user=request.user)

        # Log proposal action
        self.log_user_action(
            CommunityUserAction.ACTION_DISCARD_COMMUNITY.format(self.community_number),
            request,
        )

        # create a log entry for the user
        request.user.log_user_action(
            CommunityUserAction.ACTION_DISCARD_COMMUNITY.format(self.community_number),
            request,
        )

    def reinstate(self, request):
        if not self.processing_status == Community.PROCESSING_STATUS_DISCARDED:
            raise ValidationError(
                "You cannot reinstate a community that is not discarded"
            )

        if not is_species_communities_approver(request):
            raise ValidationError(
                "You cannot reinstate a community unless you are a species communities approver"
            )

        self.processing_status = Community.PROCESSING_STATUS_DRAFT
        self.save(version_user=request.user)

        # Log proposal action
        self.log_user_action(
            CommunityUserAction.ACTION_REINSTATE_COMMUNITY.format(
                self.community_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            CommunityUserAction.ACTION_REINSTATE_COMMUNITY.format(
                self.community_number
            ),
            request,
        )

    def log_user_action(self, action, request):
        return CommunityUserAction.log_action(self, action, request.user.id)

    @transaction.atomic
    def upload_image(self, speciesCommunitiesImageFile):
        document = CommunityDocument(
            input_name="speciesCommunitiesImageFile",
            _file=speciesCommunitiesImageFile,
            community=self,
        )
        document.check_file(speciesCommunitiesImageFile)
        document.save()
        self.image_doc = document
        self.save()

    @property
    def image_history(self):
        images_qs = (
            CommunityDocument.objects.filter(
                community=self, input_name="speciesCommunitiesImageFile"
            )
            .distinct("uploaded_date", "_file")
            .order_by("-uploaded_date")
        )
        return [
            {
                "id": image.id,
                "filename": os.path.basename(image._file.name),
                "url": image._file.url,
                "uploaded_date": image.uploaded_date,
            }
            for image in images_qs
        ]

    def reinstate_image(self, pk):
        try:
            document = CommunityDocument.objects.get(pk=pk)
        except CommunityDocument.DoesNotExist:
            raise ValidationError(f"No CommunityDocument model found with pk {pk}")

        self.image_doc = document
        self.save()

    def image_exists(self) -> bool:
        """Return True if the Community image_doc file exists in storage."""
        if not self.image_doc:
            return False
        return filefield_exists(self.image_doc)

    @property
    def occurrence_count(self):
        from boranga.components.occurrence.models import Occurrence

        return Occurrence.objects.filter(
            community=self,
            processing_status__in=[
                Occurrence.PROCESSING_STATUS_ACTIVE,
            ],
        ).count()

    @property
    def area_of_occupancy_m2(self):
        return _sum_area_of_occupancy_m2(self, "community")

    @property
    def area_of_occupancy_km2(self):
        if not self.area_of_occupancy_m2:
            return 0

        return round(self.area_of_occupancy_m2 / 1000000, 5)

    @property
    def area_occurrence_convex_hull_m2(self):
        return _convex_hull_area_m2(self, "community")

    @property
    def area_occurrence_convex_hull_km2(self):
        if not self.area_occurrence_convex_hull_m2:
            return 0

        return round(self.area_occurrence_convex_hull_m2 / 1000000, 5)

    @transaction.atomic
    def copy_for_rename(
        self, request, existing_community=None, rename_community_serializer_data=None
    ):
        if not self.processing_status == Community.PROCESSING_STATUS_ACTIVE:
            raise ValidationError("You cannot rename a community that is not active")

        if not is_species_communities_approver(request):
            raise ValidationError(
                "You cannot rename a community unless you are a species communities approver"
            )

        if existing_community:
            resulting_community = existing_community
        else:
            # Create a new community with appropriate values overridden
            resulting_community = Community.objects.get(pk=self.pk)
            resulting_community.pk = None
            resulting_community.community_number = ""
            resulting_community.processing_status = Community.PROCESSING_STATUS_ACTIVE

        resulting_community.renamed_from_id = self.id
        resulting_community.save(version_user=request.user)

        if not existing_community:
            from boranga.components.species_and_communities.serializers import (
                SaveCommunityTaxonomySerializer,
            )

            # Apply the taxonomy details to the new community
            community_taxonomy, created = CommunityTaxonomy.objects.get_or_create(
                community=resulting_community
            )
            if created:
                logger.info(
                    f"Created new taxonomy instance for community {resulting_community}"
                )
            serializer = SaveCommunityTaxonomySerializer(
                community_taxonomy, data=rename_community_serializer_data
            )

            serializer.is_valid(raise_exception=True)
            serializer.save()

        if existing_community:
            # Append original community name to existing if there is one
            resulting_community.taxonomy.previous_name = (
                f"{resulting_community.taxonomy.previous_name}, {self.taxonomy.community_name}"
                if resulting_community.taxonomy.previous_name
                else self.taxonomy.community_name
            )
            # Overwrite other taxonomy details
            resulting_community.taxonomy.community_description = (
                self.taxonomy.community_description
            )
            resulting_community.taxonomy.name_authority = self.taxonomy.name_authority
            resulting_community.taxonomy.name_comments = self.taxonomy.name_comments
        else:
            resulting_community.taxonomy.previous_name = self.taxonomy.community_name

        resulting_community.taxonomy.save()

        if not existing_community:
            # Copy the community publishing status and leave all values as is
            try:
                publishing_status = CommunityPublishingStatus.objects.get(
                    id=self.community_publishing_status.id
                )
                publishing_status.pk = None
                publishing_status.community = resulting_community
                publishing_status.save()
            except CommunityPublishingStatus.DoesNotExist:
                CommunityPublishingStatus.objects.get_or_create(community=self)

        resulting_community.species.set(self.species.all())
        resulting_community.regions.set(self.regions.all())
        resulting_community.districts.set(self.districts.all())

        resulting_community_distribution = CommunityDistribution.objects.filter(
            community=self
        ).first()

        if resulting_community_distribution:
            if not existing_community:
                # This will create a new CommunityDistribution instance
                resulting_community_distribution.pk = None
            else:
                # This will overwrite the existing CommunityDistribution instance
                if (
                    hasattr(resulting_community, "community_distribution")
                    and resulting_community.community_distribution
                ):
                    resulting_community_distribution.pk = (
                        resulting_community.community_distribution.pk
                    )
                else:
                    resulting_community_distribution.pk = None

            resulting_community_distribution.community = resulting_community
            resulting_community_distribution.save()
        else:
            # If no distribution exists, create a new one
            CommunityDistribution.objects.create(community=resulting_community)

        for new_document in self.community_documents.all():
            new_doc_instance = new_document
            new_doc_instance.community = resulting_community
            new_doc_instance.id = None
            new_doc_instance.document_number = ""
            new_doc_instance.save(version_user=request.user)
            new_doc_instance.community.log_user_action(
                CommunityUserAction.ACTION_ADD_DOCUMENT.format(
                    new_doc_instance.document_number,
                    new_doc_instance.community.community_number,
                ),
                request,
            )
            request.user.log_user_action(
                CommunityUserAction.ACTION_ADD_DOCUMENT.format(
                    new_doc_instance.document_number,
                    new_doc_instance.community.community_number,
                ),
                request,
            )

        for new_threat in self.community_threats.all():
            new_threat_instance = new_threat
            new_threat_instance.community = resulting_community
            new_threat_instance.id = None
            new_threat_instance.threat_number = ""
            new_threat_instance.save(version_user=request.user)
            new_threat_instance.community.log_user_action(
                CommunityUserAction.ACTION_ADD_THREAT.format(
                    new_threat_instance.threat_number,
                    new_threat_instance.community.community_number,
                ),
                request,
            )
            request.user.log_user_action(
                CommunityUserAction.ACTION_ADD_THREAT.format(
                    new_threat_instance.threat_number,
                    new_threat_instance.community.community_number,
                ),
                request,
            )

        self.save(version_user=request.user)

        return resulting_community


class CommunityTaxonomy(BaseModel):
    """
    Description from wacensus, to get the main name then fill in everything else

    Has a:
    Used by:
    - Community
    Is:
    - Table
    """

    community = models.OneToOneField(
        Community, on_delete=models.CASCADE, null=True, related_name="taxonomy"
    )
    community_common_id = models.CharField(
        max_length=200, null=True, blank=True, unique=True
    )
    community_name = models.CharField(
        max_length=512, null=True, blank=True, unique=True
    )
    community_description = models.CharField(max_length=2048, null=True, blank=True)
    previous_name = models.CharField(max_length=512, null=True, blank=True)
    name_authority = models.CharField(max_length=500, null=True, blank=True)
    name_comments = models.CharField(max_length=500, null=True, blank=True)

    class Meta:
        app_label = "boranga"
        ordering = ["community_name"]

    def __str__(self):
        return str(self.community_name)


class CommunityLogDocument(Document):
    log_entry = models.ForeignKey(
        "CommunityLogEntry", related_name="documents", on_delete=models.CASCADE
    )
    _file = models.FileField(
        upload_to=update_community_comms_log_filename,
        max_length=512,
        storage=private_storage,
    )

    class Meta:
        app_label = "boranga"

    def get_parent_instance(self) -> BaseModel:
        return self.log_entry


class CommunityLogEntry(CommunicationsLogEntry):
    community = models.ForeignKey(
        Community, related_name="comms_logs", on_delete=models.CASCADE
    )

    def __str__(self):
        return f"{self.reference} - {self.subject}"

    class Meta:
        app_label = "boranga"

    def save(self, **kwargs):
        # save the application reference if the reference not provided
        if not self.reference:
            self.reference = self.community.reference
        super().save(**kwargs)


class CommunityUserAction(UserAction):

    ACTION_EDIT_COMMUNITY = "Edit Community {}"
    ACTION_DISCARD_COMMUNITY = "Discard Community {}"
    ACTION_REINSTATE_COMMUNITY = "Reinstate Community {}"
    ACTION_CREATE_COMMUNITY = "Create new community {}"
    ACTION_SAVE_COMMUNITY = "Save Community {}"
    ACTION_RENAME_COMMUNITY_MADE_HISTORICAL = (
        "Community {} renamed to {} and made historical"
    )
    ACTION_RENAME_COMMUNITY_RETAINED = "Community {} renamed to {} but left active"
    ACTION_IMAGE_UPDATE = "Community Image document updated for Community {}"
    ACTION_IMAGE_DELETE = "Community Image document deleted for Community {}"
    ACTION_IMAGE_REINSTATE = "Community Image document reinstated for Community {}"
    ACTION_CREATED_FROM_RENAME_COMMUNITY = (
        "New Community {} created by renaming Community {}"
    )
    ACTION_ACTIVATED_FROM_RENAME_COMMUNITY = (
        "Draft Community {} activated by renaming Community {}"
    )
    ACTION_REACTIVATED_FROM_RENAME_COMMUNITY = (
        "Historical Community {} reactivated by renaming Community {}"
    )
    ACTION_REACTIVATE_COMMUNITY = "Reactivate Community {}"
    ACTION_DEACTIVATE_COMMUNITY = "Deactivate Community {}"

    # Document
    ACTION_ADD_DOCUMENT = "Document {} added for Community {}"
    ACTION_UPDATE_DOCUMENT = "Document {} updated for Community {}"
    ACTION_DISCARD_DOCUMENT = "Document {} discarded for Community {}"
    ACTION_REINSTATE_DOCUMENT = "Document {} reinstated for Community {}"

    # Threat
    ACTION_ADD_THREAT = "Threat {} added for Community {}"
    ACTION_UPDATE_THREAT = "Threat {} updated for Community {}"
    ACTION_DISCARD_THREAT = "Threat {} discarded for Community {}"
    ACTION_REINSTATE_THREAT = "Threat {} reinstated for Community {}"

    class Meta:
        app_label = "boranga"
        ordering = ("-when",)

    @classmethod
    def log_action(cls, community, action, user):
        return cls.objects.create(community=community, who=user, what=str(action))

    community = models.ForeignKey(
        Community, related_name="action_logs", on_delete=models.CASCADE
    )


class CommunityDistribution(BaseModel):
    """
    All the different locations where this community can be found.

    Used by:
    - Communities
    Is:
    - Table
    """

    number_of_occurrences = models.IntegerField(null=True, blank=True)
    noo_auto = models.BooleanField(
        default=True
    )  # to check auto or manual entry of number_of_occurrences
    extent_of_occurrences = models.DecimalField(
        null=True,
        blank=True,
        max_digits=15,
        decimal_places=5,
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    eoo_auto = models.BooleanField(
        default=True
    )  # extra boolean field to check auto or manual entry of extent_of_occurrences
    area_of_occupancy = models.IntegerField(null=True, blank=True)
    area_of_occupancy_actual = models.DecimalField(
        max_digits=15, decimal_places=5, null=True, blank=True
    )
    aoo_actual_auto = models.BooleanField(
        default=True
    )  # to check auto or manual entry of area_of_occupancy_actual
    number_of_iucn_locations = models.IntegerField(null=True, blank=True)
    # Community Ecological Attributes
    community_original_area = models.DecimalField(
        null=True,
        blank=True,
        max_digits=12,
        decimal_places=2,
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    community_original_area_accuracy = models.DecimalField(
        max_digits=15, decimal_places=5, null=True, blank=True
    )
    community_original_area_reference = models.CharField(
        max_length=512, null=True, blank=True
    )
    community = models.OneToOneField(
        Community,
        on_delete=models.CASCADE,
        null=True,
        related_name="community_distribution",
    )
    distribution = models.CharField(max_length=512, null=True, blank=True)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        string = f"Community Distribution {self.id}"
        if self.community:
            string += f" for Community ({self.community})"
        return string


class DocumentCategory(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    This is particularly useful for organisation of documents e.g. preventing inappropriate documents being added
    to certain tables.

    Used by:
    - DocumentSubCategory
    - SpeciesDocument
    - CommunityDocument
    -ConservationStatusDocument
    Is:
    - Table
    """

    document_category_name = models.CharField(
        max_length=128, unique=True, validators=[no_commas_validator]
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Document Category"
        verbose_name_plural = "Document Categories"

    def __str__(self):
        return str(self.document_category_name)


class DocumentSubCategory(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    This is particularly useful for organisation of sub documents e.g. preventing inappropriate documents being added
    to certain tables.

    Used by:
    - SpeciesDocument
    - CommunityDocument
    -ConservationStatusDocument
    Is:
    - Table
    """

    document_category = models.ForeignKey(
        DocumentCategory,
        on_delete=models.CASCADE,
        related_name="document_sub_categories",
    )
    document_sub_category_name = models.CharField(
        max_length=128,
        unique=True,
        validators=[no_commas_validator],
    )
    order_with_respect_to = "document_category"

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Document Sub Category"
        verbose_name_plural = "Document Sub Categories"

    def __str__(self):
        return str(self.document_sub_category_name)


class SpeciesDocument(Document):
    """
    Meta-data associated with a document relevant to a Species.

    Has a:
    - Species
    - DocumentCategory
    - DocumentSubCategoty
    Used for:
    - Species
    Is:
    - Table
    """

    document_number = models.CharField(max_length=9, blank=True, default="")
    _file = models.FileField(
        upload_to=update_species_doc_filename,
        max_length=512,
        default=None,
        storage=private_storage,
    )
    input_name = models.CharField(max_length=255, null=True, blank=True)
    document_category = models.ForeignKey(
        DocumentCategory, null=True, blank=True, on_delete=models.SET_NULL
    )
    document_sub_category = models.ForeignKey(
        DocumentSubCategory, null=True, blank=True, on_delete=models.SET_NULL
    )
    species = models.ForeignKey(
        Species,
        blank=False,
        default=None,
        on_delete=models.CASCADE,
        related_name="species_documents",
    )

    class Meta:
        app_label = "boranga"
        verbose_name = "Species Document"

    def save(self, *args, **kwargs):
        # Prefix "D" char to document_number.
        if self.document_number == "":
            force_insert = kwargs.pop("force_insert", False)
            super().save(no_revision=True, force_insert=force_insert)
            new_document_id = f"D{str(self.pk)}"
            self.document_number = new_document_id
            self.save(*args, **kwargs)
        else:
            super().save(*args, **kwargs)

    def get_parent_instance(self) -> BaseModel:
        return self.species

    @transaction.atomic
    def add_documents(self, request, *args, **kwargs):
        # save the files
        data = json.loads(request.data.get("data"))
        for idx in range(data["num_files"]):
            self.check_file(request.data.get("file-" + str(idx)))
            _file = request.data.get("file-" + str(idx))
            self._file = _file
            self.name = _file.name
            self.input_name = data["input_name"]
            self.save(no_revision=True)  # no need to have multiple revisions
        # end save documents
        self.save(*args, **kwargs)


class CommunityDocument(Document):
    """
    Meta-data associated with a document relevant to a Community.

    Has a:
    - Community
    - DocumentCategory
    - DocumentSubCategory
    Used for:
    - Community:
    Is:
    - Table
    """

    document_number = models.CharField(max_length=9, blank=True, default="")
    _file = models.FileField(
        upload_to=update_community_doc_filename,
        max_length=512,
        default=None,
        storage=private_storage,
    )
    input_name = models.CharField(max_length=255, null=True, blank=True)
    document_category = models.ForeignKey(
        DocumentCategory, null=True, blank=True, on_delete=models.SET_NULL
    )
    document_sub_category = models.ForeignKey(
        DocumentSubCategory, null=True, blank=True, on_delete=models.SET_NULL
    )
    community = models.ForeignKey(
        Community,
        blank=False,
        default=None,
        on_delete=models.CASCADE,
        related_name="community_documents",
    )

    class Meta:
        app_label = "boranga"
        verbose_name = "Community Document"

    def save(self, *args, **kwargs):
        # Prefix "D" char to document_number.
        if self.document_number == "":
            force_insert = kwargs.pop("force_insert", False)
            super().save(no_revision=True, force_insert=force_insert)
            new_document_id = f"D{str(self.pk)}"
            self.document_number = new_document_id
            self.save(*args, **kwargs)
        else:
            super().save(*args, **kwargs)

    def get_parent_instance(self) -> BaseModel:
        return self.community

    @transaction.atomic
    def add_documents(self, request, *args, **kwargs):
        # save the files
        data = json.loads(request.data.get("data"))
        for idx in range(data["num_files"]):
            self.check_file(request.data.get("file-" + str(idx)))
            _file = request.data.get("file-" + str(idx))
            self._file = _file
            self.name = _file.name
            self.input_name = data["input_name"]
            self.save(no_revision=True)
        # end save documents
        self.save(*args, **kwargs)


class ThreatCategory(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    # e.g. mechnical disturbance
    """

    name = models.CharField(
        max_length=128, blank=False, unique=True, validators=[no_commas_validator]
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Threat Category"
        verbose_name_plural = "Threat Categories"

    def __str__(self):
        return str(self.name)


class CurrentImpact(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    # don't know the data yet

    Used by:
    - ConservationThreat

    """

    name = models.CharField(
        max_length=100, blank=False, unique=True, validators=[no_commas_validator]
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Current Impact"
        verbose_name_plural = "Current Impacts"

    def __str__(self):
        return str(self.name)


class PotentialImpact(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    # don't know the data yet

    Used by:
    - ConservationThreat

    """

    name = models.CharField(
        max_length=100, blank=False, unique=True, validators=[no_commas_validator]
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Potential Impact"
        verbose_name_plural = "Potential Impacts"

    def __str__(self):
        return str(self.name)


class PotentialThreatOnset(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    # don't know the data yet

    Used by:
    - ConservationThreat

    """

    name = models.CharField(
        max_length=100, blank=False, unique=True, validators=[no_commas_validator]
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Potential Threat Onset"
        verbose_name_plural = "Potential Threat Onsets"

    def __str__(self):
        return str(self.name)


class ThreatAgent(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    Used by:
    - ConservationThreat

    """

    name = models.CharField(
        max_length=100, blank=False, unique=True, validators=[no_commas_validator]
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Threat Agent"
        verbose_name_plural = "Threat Agents"

    def __str__(self):
        return str(self.name)


class ConservationThreat(RevisionedMixin):
    """
    Threat for a species and community in a particular location.

    NB: Maybe make many to many

    Has a:
    - species
    - community
    Used for:
    - Species
    - Community
    Is:
    - Table
    """

    species = models.ForeignKey(
        Species,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="species_threats",
    )
    community = models.ForeignKey(
        Community,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="community_threats",
    )
    threat_number = models.CharField(max_length=9, blank=True, default="")
    threat_category = models.ForeignKey(
        ThreatCategory, on_delete=models.CASCADE, default=None, null=True, blank=True
    )
    threat_agent = models.ForeignKey(
        ThreatAgent, on_delete=models.SET_NULL, default=None, null=True, blank=True
    )
    current_impact = models.ForeignKey(
        CurrentImpact, on_delete=models.SET_NULL, default=None, null=True, blank=True
    )
    potential_impact = models.ForeignKey(
        PotentialImpact, on_delete=models.SET_NULL, default=None, null=True, blank=True
    )
    potential_threat_onset = models.ForeignKey(
        PotentialThreatOnset,
        on_delete=models.SET_NULL,
        default=None,
        null=True,
        blank=True,
    )
    comment = models.CharField(max_length=512, blank=True, null=True)
    date_observed = models.DateField(blank=True, null=True)
    visible = models.BooleanField(
        default=True
    )  # to prevent deletion, hidden and still be available in history

    class Meta:
        app_label = "boranga"

    def __str__(self):
        string = f"Conservation Threat {self.id}"
        if self.species:
            string += f" for Species ({self.species})"
        elif self.community:
            string += f" for Community ({self.community})"

        return string

    def save(self, *args, **kwargs):
        if self.threat_number == "":
            force_insert = kwargs.pop("force_insert", False)
            super().save(no_revision=True, force_insert=force_insert)
            new_threat_id = f"T{str(self.pk)}"
            self.threat_number = new_threat_id
            self.save(*args, **kwargs)
        else:
            super().save(*args, **kwargs)

    @property
    def source(self):
        if self.species:
            return self.species.species_number
        elif self.community:
            return self.community.community_number


class SpeciesPublishingStatus(BaseModel):
    """
    The public publishing status of a species instance and its sections.

    Has a:
    - species
    Used for:
    - Species
    Is:
    - Table
    """

    species = models.OneToOneField(
        Species,
        on_delete=models.CASCADE,
        null=True,
        related_name="species_publishing_status",
    )

    species_public = models.BooleanField(default=False)

    distribution_public = models.BooleanField(default=False)
    conservation_status_public = models.BooleanField(default=False)
    conservation_attributes_public = models.BooleanField(default=False)
    threats_public = models.BooleanField(default=False)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return str(self.species)


class CommunityPublishingStatus(BaseModel):
    """
    The public publishing status of a community instance and its sections.

    Has a:
    - community
    Used for:
    - Community
    Is:
    - Table
    """

    community = models.OneToOneField(
        Community,
        on_delete=models.CASCADE,
        null=True,
        related_name="community_publishing_status",
    )

    community_public = models.BooleanField(default=False)

    distribution_public = models.BooleanField(default=False)
    conservation_status_public = models.BooleanField(default=False)
    conservation_attributes_public = models.BooleanField(default=False)
    threats_public = models.BooleanField(default=False)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return str(self.community)


class FloraRecruitmentType(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    # list derived from WACensus

    Used by:
    - SpeciesConservationAttributes

    """

    recruitment_type = models.CharField(max_length=200, blank=False, unique=True)

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Flora Recruitment Type"
        verbose_name_plural = "Flora Recruitment Types"

    def __str__(self):
        return str(self.recruitment_type)


class RootMorphology(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    # list derived from WACensus

    Used by:
    - SpeciesConservationAttributes

    """

    name = models.CharField(max_length=200, blank=False, unique=True)

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Root Morphology"
        verbose_name_plural = "Root Morphologies"

    def __str__(self):
        return str(self.name)


class PostFireHabitatInteraction(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    # list derived from WACensus

    Used by:
    - SpeciesConservationAttributes

    """

    name = models.CharField(max_length=200, blank=False, unique=True)

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Post Fire Habitat Interaction"
        verbose_name_plural = "Post Fire Habitat Interactions"

    def __str__(self):
        return str(self.name)


class SpeciesConservationAttributes(BaseModel):
    """
    Species conservation attributes data.

    Used for:
    - Species
    Is:
    - Table
    """

    PERIOD_CHOICES = (
        (1, "January"),
        (2, "February"),
        (3, "March"),
        (4, "April"),
        (5, "May"),
        (6, "June"),
        (7, "July"),
        (8, "August"),
        (9, "September"),
        (10, "October"),
        (11, "November"),
        (12, "December"),
    )
    INTERVAL_CHOICES = ((1, "year/s"), (2, "month/s"))

    species = models.OneToOneField(
        Species,
        on_delete=models.CASCADE,
        null=True,
        related_name="species_conservation_attributes",
    )

    # flora related attributes
    flowering_period = MultiSelectField(
        max_length=250, blank=True, choices=PERIOD_CHOICES, null=True
    )
    fruiting_period = MultiSelectField(
        max_length=250, blank=True, choices=PERIOD_CHOICES, null=True
    )
    flora_recruitment_type = models.ForeignKey(
        FloraRecruitmentType, on_delete=models.SET_NULL, null=True, blank=True
    )
    flora_recruitment_notes = models.CharField(max_length=1000, null=True, blank=True)
    seed_viability_germination_info = models.CharField(
        max_length=1000, null=True, blank=True
    )
    root_morphology = models.ForeignKey(
        RootMorphology, on_delete=models.SET_NULL, null=True, blank=True
    )
    pollinator_information = models.CharField(max_length=1000, null=True, blank=True)
    response_to_dieback = models.CharField(max_length=1500, null=True, blank=True)

    # fauna related attributes
    breeding_period = MultiSelectField(
        max_length=250, blank=True, choices=PERIOD_CHOICES, null=True
    )
    fauna_breeding = models.CharField(max_length=2000, null=True, blank=True)
    fauna_reproductive_capacity = models.CharField(
        max_length=200, null=True, blank=True
    )
    diet_and_food_source = models.CharField(max_length=500, null=True, blank=True)
    home_range = models.CharField(max_length=1000, null=True, blank=True)

    # flora and fauna common attributes
    habitat_growth_form = models.CharField(max_length=200, null=True, blank=True)
    time_to_maturity_from = models.IntegerField(null=True, blank=True)
    time_to_maturity_to = models.IntegerField(null=True, blank=True)
    time_to_maturity_choice = models.CharField(
        max_length=10, choices=INTERVAL_CHOICES, null=True, blank=True
    )
    generation_length_from = models.IntegerField(null=True, blank=True)
    generation_length_to = models.IntegerField(null=True, blank=True)
    generation_length_choice = models.CharField(
        max_length=10, choices=INTERVAL_CHOICES, null=True, blank=True
    )
    average_lifespan_from = models.IntegerField(null=True, blank=True)
    average_lifespan_to = models.IntegerField(null=True, blank=True)
    average_lifespan_choice = models.CharField(
        max_length=10, choices=INTERVAL_CHOICES, null=True, blank=True
    )
    minimum_fire_interval_from = models.IntegerField(null=True, blank=True)
    minimum_fire_interval_to = models.IntegerField(null=True, blank=True)
    minimum_fire_interval_choice = models.CharField(
        max_length=10, choices=INTERVAL_CHOICES, null=True, blank=True
    )
    response_to_fire = models.CharField(max_length=200, null=True, blank=True)
    post_fire_habitat_interaction = models.ForeignKey(
        PostFireHabitatInteraction, on_delete=models.SET_NULL, null=True, blank=True
    )
    habitat = models.CharField(max_length=1000, null=True, blank=True)
    hydrology = models.CharField(max_length=200, null=True, blank=True)
    research_requirements = models.CharField(max_length=1500, null=True, blank=True)
    other_relevant_diseases = models.CharField(max_length=1500, null=True, blank=True)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        string = f"Conservation Attributes: {self.id}"
        if self.species:
            string += f" for Species ({self.species})"
        return string


class CommunityConservationAttributes(BaseModel):
    """
    Community conservation attributes data.

    Used for:
    - Community
    Is:
    - Table
    """

    community = models.OneToOneField(
        Community,
        on_delete=models.CASCADE,
        null=True,
        related_name="community_conservation_attributes",
    )

    pollinator_information = models.CharField(max_length=1000, null=True, blank=True)
    minimum_fire_interval_from = models.IntegerField(null=True, blank=True)
    minimum_fire_interval_to = models.IntegerField(null=True, blank=True)
    minimum_fire_interval_choice = models.CharField(
        max_length=10,
        choices=SpeciesConservationAttributes.INTERVAL_CHOICES,
        null=True,
        blank=True,
    )
    response_to_fire = models.CharField(max_length=200, null=True, blank=True)
    post_fire_habitat_interaction = models.ForeignKey(
        PostFireHabitatInteraction, on_delete=models.SET_NULL, null=True, blank=True
    )
    hydrology = models.CharField(max_length=200, null=True, blank=True)
    ecological_and_biological_information = models.CharField(
        max_length=500, null=True, blank=True
    )
    research_requirements = models.CharField(max_length=500, null=True, blank=True)
    response_to_dieback = models.CharField(max_length=500, null=True, blank=True)
    other_relevant_diseases = models.CharField(max_length=500, null=True, blank=True)

    class Meta:
        app_label = "boranga"

    def __str__(self):
        string = f"Conservation Attributes: {self.id}"
        if self.community:
            string += f" for Community ({self.community})"
        return string


class SystemEmailGroup(BaseModel):
    AREA_CONSERVATION_STATUS = "conservation_status"
    AREA_OCCURRENCE = "occurrence"
    AREA_CHOICES = [
        (AREA_CONSERVATION_STATUS, "Conservation Status"),
        (AREA_OCCURRENCE, "Occurrence"),
    ]
    group_type = models.ForeignKey(
        GroupType, on_delete=models.PROTECT, null=False, blank=False
    )
    area = models.CharField(max_length=50, choices=AREA_CHOICES, blank=True, null=True)

    class Meta:
        app_label = "boranga"
        verbose_name = "System Email Group"

    def __str__(self):
        return self.label

    @property
    def label(self):
        label = self.group_type.name.title()
        if self.area:
            label += f" {self.get_area_display()}"
        label += " Notification Group"
        return label

    @property
    def email_address_list(self):
        return [email.email for email in self.systememail_set.all()]

    @property
    def email_address_list_str(self):
        return ", ".join(self.email_address_list)

    @classmethod
    def emails_by_group_and_area(
        cls, group_type: GroupType, area: str | None = None
    ) -> list[str]:
        if not group_type:
            logger.warning(
                "No group_type provided. Returning value from NOTIFICATION_EMAIL env instead."
            )
            return settings.NOTIFICATION_EMAIL.split(",")
        try:
            group = cls.objects.get(group_type=group_type, area=area)
        except cls.DoesNotExist:
            logger.warning(
                f"No SystemEmailGroup found for group_type {group_type} and area {area}. "
                "Returning value from NOTIFICATION_EMAIL env instead."
            )
            return settings.NOTIFICATION_EMAIL.split(",")

        if len(group.email_address_list) == 0:
            logger.warning(
                f"No SystemEmailGroup email addresses found for group_type {group_type} and area {area}. "
                "Returning value from NOTIFICATION_EMAIL env instead."
            )
            return settings.NOTIFICATION_EMAIL.split(",")

        return group.email_address_list


class SystemEmail(BaseModel):
    system_email_group = models.ForeignKey(
        SystemEmailGroup, on_delete=models.PROTECT, null=False, blank=False
    )
    email = models.EmailField(max_length=255, blank=False, null=False)

    class Meta:
        app_label = "boranga"
        ordering = ["system_email_group", "email"]

    def __str__(self):
        return f"{self.email} - {self.system_email_group}"


# Species Document History
reversion.register(SpeciesDocument)

# Species History
reversion.register(
    Species,
    follow=[
        "taxonomy",
        "species_distribution",
        "species_conservation_attributes",
        "species_publishing_status",
    ],
)
reversion.register(Taxonomy, follow=["taxon_previous_queryset", "vernaculars"])
reversion.register(TaxonPreviousName)
reversion.register(SpeciesDistribution)
reversion.register(SpeciesConservationAttributes)
reversion.register(TaxonVernacular)
reversion.register(SpeciesPublishingStatus)

# Community Document
reversion.register(CommunityDocument)

# Community History
reversion.register(
    Community,
    follow=[
        "taxonomy",
        "community_distribution",
        "community_conservation_attributes",
        "community_publishing_status",
    ],
)
reversion.register(CommunityTaxonomy)
reversion.register(CommunityDistribution)
reversion.register(CommunityConservationAttributes)
reversion.register(CommunityPublishingStatus)

# Conservation Threat
reversion.register(ConservationThreat)
