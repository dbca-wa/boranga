import json
import logging
from datetime import datetime, timedelta

import reversion
from django.apps import apps
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.files.storage import FileSystemStorage
from django.db import models, transaction
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ordered_model.models import OrderedModel

from boranga import exceptions
from boranga.components.conservation_status.email import (
    send_approver_approve_email_notification,
    send_approver_decline_email_notification,
    send_approver_defer_email_notification,
    send_approver_propose_delist_email_notification,
    send_approver_proposed_for_agenda_email_notification,
    send_assessor_ready_for_agenda_email_notification,
    send_conservation_status_amendment_email_notification,
    send_conservation_status_referral_complete_email_notification,
    send_conservation_status_referral_email_notification,
    send_conservation_status_referral_recall_email_notification,
    send_proposal_approver_sendback_email_notification,
)
from boranga.components.main.models import (
    ArchivableModel,
    BaseModel,
    CommunicationsLogEntry,
    Document,
    LockableModel,
    OrderedArchivableManager,
    RevisionedMixin,
    UserAction,
)
from boranga.components.main.related_item import RelatedItem
from boranga.components.species_and_communities.models import (
    Community,
    DocumentCategory,
    DocumentSubCategory,
    GroupType,
    Species,
    Taxonomy,
)
from boranga.components.users.models import (
    SubmitterInformation,
    SubmitterInformationModelMixin,
)
from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_external_contributor,
    is_internal_contributor,
    member_ids,
    no_commas_validator,
)
from boranga.ledger_api_utils import retrieve_email_user
from boranga.settings import (
    GROUP_NAME_CONSERVATION_STATUS_APPROVER,
    GROUP_NAME_CONSERVATION_STATUS_ASSESSOR,
)

logger = logging.getLogger(__name__)

private_storage = FileSystemStorage(
    location=settings.BASE_DIR + "/private-media/", base_url="/private-media/"
)


def update_species_conservation_status_comms_log_filename(instance, filename):
    return (
        f"{settings.MEDIA_APP_DIR}/conservation_status/{instance.log_entry.conservation_status.id}"
        f"/communications/{filename}"
    )


def update_conservation_status_comms_log_filename(instance, filename):
    return (
        f"{settings.MEDIA_APP_DIR}/conservation_status/{instance.log_entry.conservation_status.id}"
        f"/communications/{filename}"
    )


def update_referral_doc_filename(instance, filename):
    return "{}/conservation_status/{}/referral/{}".format(
        settings.MEDIA_APP_DIR, instance.referral.proposal.id, filename
    )


def update_conservation_status_amendment_request_doc_filename(instance, filename):
    return (
        f"conservation_status/{instance.conservation_status_amendment_request.conservation_status.id}"
        f"/amendment_request_documents/{filename}"
    )


def update_conservation_status_doc_filename(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/conservation_status/{instance.conservation_status.id}/documents/{filename}"


class AbstractConservationList(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()

    code = models.CharField(max_length=64)
    label = models.CharField(max_length=512)
    applies_to_flora = models.BooleanField(default=False)
    applies_to_fauna = models.BooleanField(default=False)
    applies_to_communities = models.BooleanField(default=False)

    class Meta(OrderedModel.Meta):
        abstract = True
        app_label = "boranga"

    @classmethod
    def get_lists_dict(
        cls: models.base.ModelBase,
        group_type: str | int | None,
        active_only: bool = False,
    ) -> list:
        try:
            if group_type and isinstance(group_type, int):
                group_type = GroupType.objects.get(id=group_type)
            elif group_type and isinstance(group_type, str):
                group_type = GroupType.objects.get(name=group_type)
        except GroupType.DoesNotExist:
            logger.warning(f"GroupType {group_type} does not exist")
            return []

        lists = cls.objects.all()

        if active_only:
            lists = cls.objects.active()

        lists = lists.values("id", "code", "label")

        if group_type and group_type.name == GroupType.GROUP_TYPE_COMMUNITY:
            lists = lists.filter(applies_to_communities=True)
        elif group_type and group_type.name in [
            GroupType.GROUP_TYPE_FLORA,
        ]:
            lists = lists.filter(applies_to_flora=True)
        elif group_type and group_type.name in [
            GroupType.GROUP_TYPE_FAUNA,
        ]:
            lists = lists.filter(applies_to_fauna=True)
        return list(lists)

    def __str__(self):
        return f"{self.code} - {self.label}"


class AbstractConservationCategory(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()

    code = models.CharField(max_length=64)
    label = models.CharField(max_length=512)

    class Meta(OrderedModel.Meta):
        abstract = True
        app_label = "boranga"

    def __str__(self):
        return str(self.code)


class WAPriorityList(AbstractConservationList):

    class Meta:
        ordering = ["code"]
        app_label = "boranga"
        verbose_name = "WA Priority List"


class WAPriorityCategory(AbstractConservationCategory):
    wa_priority_lists = models.ManyToManyField(
        WAPriorityList, related_name="wa_priority_categories"
    )

    class Meta:
        ordering = ["code"]
        app_label = "boranga"
        verbose_name = "WA Priority Category"
        verbose_name_plural = "WA Priority Categories"

    @classmethod
    def get_categories_dict(
        cls: models.base.ModelBase,
        group_type: str | int | None,
        active_only: bool = False,
    ) -> list:
        try:
            if group_type and isinstance(group_type, int):
                group_type = GroupType.objects.get(id=group_type)
            elif group_type and isinstance(group_type, str):
                group_type = GroupType.objects.get(name=group_type)
        except GroupType.DoesNotExist:
            logger.warning(f"GroupType {group_type} does not exist")
            return []
        wa_priority_categories = []

        wa_priority_categories_qs = cls.objects.all()

        if active_only:
            wa_priority_categories_qs = cls.objects.active()

        wa_priority_categories_qs = wa_priority_categories_qs.only(
            "id", "code", "label"
        )
        if group_type and group_type.name == GroupType.GROUP_TYPE_COMMUNITY:
            wa_priority_categories_qs = wa_priority_categories_qs.filter(
                wa_priority_lists__applies_to_communities=True
            )
        elif group_type and group_type.name in [
            GroupType.GROUP_TYPE_FLORA,
        ]:
            wa_priority_categories_qs = wa_priority_categories_qs.filter(
                wa_priority_lists__applies_to_flora=True
            )
        elif group_type and group_type.name in [
            GroupType.GROUP_TYPE_FAUNA,
        ]:
            wa_priority_categories_qs = wa_priority_categories_qs.filter(
                wa_priority_lists__applies_to_fauna=True
            )
        for wa_priority_category in wa_priority_categories_qs.distinct():
            list_ids = list(
                WAPriorityList.objects.filter(
                    wa_priority_categories=wa_priority_category.id
                ).values_list("id", flat=True)
            )
            wa_priority_categories.append(
                {
                    "id": wa_priority_category.id,
                    "code": wa_priority_category.code,
                    "label": wa_priority_category.label,
                    "list_ids": list_ids,
                }
            )
        return wa_priority_categories


class WALegislativeList(AbstractConservationList):

    class Meta:
        ordering = ["code"]
        app_label = "boranga"
        verbose_name = "WA Legislative List"


class WALegislativeCategory(AbstractConservationCategory):
    wa_legislative_lists = models.ManyToManyField(
        WALegislativeList, related_name="wa_legislative_categories"
    )

    class Meta:
        ordering = ["code"]
        app_label = "boranga"
        verbose_name = "WA Legislative Category"
        verbose_name_plural = "WA Legislative Categories"

    @classmethod
    def get_categories_dict(
        cls: models.base.ModelBase,
        group_type: str | int | None,
        active_only: bool = False,
    ) -> list:
        try:
            if group_type and isinstance(group_type, int):
                group_type = GroupType.objects.get(id=group_type)
            elif group_type and isinstance(group_type, str):
                group_type = GroupType.objects.get(name=group_type)
        except GroupType.DoesNotExist:
            logger.warning(f"GroupType {group_type} does not exist")
            return []
        wa_legislative_categories = []

        wa_legislative_categories_qs = cls.objects.all()

        if active_only:
            wa_legislative_categories_qs = cls.objects.active()

        wa_legislative_categories_qs = wa_legislative_categories_qs.only(
            "id", "code", "label"
        )
        if group_type and group_type.name == GroupType.GROUP_TYPE_COMMUNITY:
            wa_legislative_categories_qs = wa_legislative_categories_qs.filter(
                wa_legislative_lists__applies_to_communities=True
            )
        elif group_type and group_type.name in [
            GroupType.GROUP_TYPE_FLORA,
        ]:
            wa_legislative_categories_qs = wa_legislative_categories_qs.filter(
                wa_legislative_lists__applies_to_flora=True
            )
        elif group_type and group_type.name in [
            GroupType.GROUP_TYPE_FAUNA,
        ]:
            wa_legislative_categories_qs = wa_legislative_categories_qs.filter(
                wa_legislative_lists__applies_to_fauna=True
            )
        for wa_legislative_category in wa_legislative_categories_qs.distinct():
            list_ids = list(
                WALegislativeList.objects.filter(
                    wa_legislative_categories=wa_legislative_category.id
                ).values_list("id", flat=True)
            )
            wa_legislative_categories.append(
                {
                    "id": wa_legislative_category.id,
                    "code": wa_legislative_category.code,
                    "label": wa_legislative_category.label,
                    "list_ids": list_ids,
                }
            )
        return wa_legislative_categories


class IUCNVersion(AbstractConservationList):
    class Meta:
        ordering = ["code"]
        app_label = "boranga"
        verbose_name = "IUCN Version"


class CommonwealthConservationList(AbstractConservationList):

    class Meta:
        ordering = ["code"]
        app_label = "boranga"
        verbose_name = "Commonwealth Conservation Category"
        verbose_name_plural = "Commonwealth Conservation Categories"


class OtherConservationAssessmentList(AbstractConservationList):

    class Meta:
        ordering = ["code"]
        app_label = "boranga"
        verbose_name = "Other Conservation Assessment"


class ConservationChangeCode(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()
    """
    When the conservation status of a species/community is changed, it can be for a number of reasons.
    These reasons are represented by change codes.
    """

    code = models.CharField(max_length=32)
    label = models.CharField(max_length=512)

    class Meta(OrderedModel.Meta):
        app_label = "boranga"

    def __str__(self):
        return str(self.code)

    @classmethod
    def get_closed_change_code(cls):
        return cls.objects.get(code=settings.CONSERVATION_CHANGE_CODE_CLOSE)

    @classmethod
    def get_filter_list(cls):
        return list(cls.objects.values("id", "code"))

    @classmethod
    def get_active_filter_list(cls):
        return list(cls.objects.active().values("id", "code"))


class ConservationStatus(
    LockableModel, SubmitterInformationModelMixin, RevisionedMixin
):
    """
    Several lists with different attributes

    NB: Different lists has different different entries
    mainly interest in wa but must accomodte comm as well
    Has a:
    - ConservationChangeCode
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - SpeciesConservationStatus
    - CommunityConservationStatus
    """

    CUSTOMER_STATUS_DRAFT = "draft"
    CUSTOMER_STATUS_WITH_ASSESSOR = "with_assessor"
    CUSTOMER_STATUS_READY_FOR_AGENDA = "ready_for_agenda"
    CUSTOMER_STATUS_APPROVED = "approved"
    CUSTOMER_STATUS_DECLINED = "declined"
    CUSTOMER_STATUS_DISCARDED = "discarded"
    CUSTOMER_STATUS_CLOSED = "closed"
    CUSTOMER_STATUS_CHOICES = (
        (CUSTOMER_STATUS_DRAFT, "Draft"),
        (CUSTOMER_STATUS_WITH_ASSESSOR, "Under Review"),
        (CUSTOMER_STATUS_READY_FOR_AGENDA, "In Meeting"),
        (CUSTOMER_STATUS_APPROVED, "Approved"),
        (CUSTOMER_STATUS_DECLINED, "Declined"),
        (CUSTOMER_STATUS_DISCARDED, "Discarded"),
        (CUSTOMER_STATUS_CLOSED, "DeListed"),
    )

    # List of statuses from above that allow a customer to edit an application.
    CUSTOMER_EDITABLE_STATE = [
        "draft",
        "amendment_required",
    ]

    # List of statuses from above that allow a customer to view an application (read-only)
    CUSTOMER_VIEWABLE_STATE = [
        "with_assessor",
        "ready_for_agenda",
        "under_review",
        "approved",
        "declined",
        "closed",
    ]

    PROCESSING_STATUS_APPROVED = "approved"
    PROCESSING_STATUS_CLOSED = "closed"
    PROCESSING_STATUS_DECLINED = "declined"
    PROCESSING_STATUS_DEFERRED = "deferred"
    PROCESSING_STATUS_DELISTED = "delisted"
    PROCESSING_STATUS_DISCARDED = "discarded"
    PROCESSING_STATUS_DISCARDED_INTERNALLY = "discarded_internally"
    PROCESSING_STATUS_DRAFT = "draft"
    PROCESSING_STATUS_ON_AGENDA = "on_agenda"
    PROCESSING_STATUS_PROPOSED_FOR_AGENDA = "proposed_for_agenda"
    PROCESSING_STATUS_READY_FOR_AGENDA = "ready_for_agenda"
    PROCESSING_STATUS_WITH_APPROVER = "with_approver"
    PROCESSING_STATUS_WITH_ASSESSOR = "with_assessor"
    PROCESSING_STATUS_WITH_REFERRAL = "with_referral"

    PROCESSING_STATUS_CHOICES = (
        (PROCESSING_STATUS_DRAFT, "Draft"),
        (PROCESSING_STATUS_DISCARDED, "Discarded"),
        (PROCESSING_STATUS_WITH_ASSESSOR, "With Assessor"),
        (PROCESSING_STATUS_WITH_REFERRAL, "With Referral"),
        (PROCESSING_STATUS_DEFERRED, "Deferred"),
        (PROCESSING_STATUS_PROPOSED_FOR_AGENDA, "Proposed For Agenda"),
        (PROCESSING_STATUS_READY_FOR_AGENDA, "Ready For Agenda"),
        (PROCESSING_STATUS_ON_AGENDA, "On Agenda"),
        (PROCESSING_STATUS_WITH_APPROVER, "Proposed DeListed"),
        (PROCESSING_STATUS_APPROVED, "Approved"),
        (PROCESSING_STATUS_DECLINED, "Declined"),
        (PROCESSING_STATUS_DELISTED, "DeListed"),
        (PROCESSING_STATUS_CLOSED, "Closed"),
    )

    # The following tuples of statuses are only user for filtering purposes
    PROCESSING_STATUSES_AWAITING_ASSESSOR_ACTION = (
        PROCESSING_STATUS_WITH_ASSESSOR,
        PROCESSING_STATUS_WITH_REFERRAL,
        PROCESSING_STATUS_DEFERRED,
    )

    PROCESSING_STATUSES_AWAITING_APPROVER_ACTION = (
        PROCESSING_STATUS_PROPOSED_FOR_AGENDA,
        PROCESSING_STATUS_READY_FOR_AGENDA,
        PROCESSING_STATUS_ON_AGENDA,
        PROCESSING_STATUS_DEFERRED,
        PROCESSING_STATUS_WITH_APPROVER,
    )

    PROCESSING_STATUSES_INACTIVE = (
        PROCESSING_STATUS_DECLINED,
        PROCESSING_STATUS_CLOSED,
        PROCESSING_STATUS_DELISTED,
    )

    # These statuses are used as front end filters but shouldn't be used in most cases
    # So it is deliberately not included in PROCESSING_STATUS_CHOICES
    PROCESSING_STATUS_DISCARDED_BY_ME = "discarded_by_me"
    PROCESSING_STATUS_AWAITING_ASSESSOR_ACTION = "awaiting_assessor_action"
    PROCESSING_STATUS_AWAITING_APPROVER_ACTION = "awaiting_approver_action"
    PROCESSING_STATUS_INACTIVE = "inactive"

    customer_status = models.CharField(
        "Customer Status",
        max_length=40,
        choices=CUSTOMER_STATUS_CHOICES,
        default=CUSTOMER_STATUS_CHOICES[0][0],
    )

    change_code = models.ForeignKey(
        ConservationChangeCode, on_delete=models.SET_NULL, blank=True, null=True
    )

    APPLICATION_TYPE_CHOICES = (
        ("new_proposal", "New Application"),
        ("amendment", "Amendment"),
        ("renewal", "Renewal"),
        ("external", "External"),
    )

    RELATED_ITEM_CHOICES = [
        ("species", "Species"),
        ("community", "Community"),
        ("agendaitem", "Meeting Agenda Item"),
    ]

    # group_type of application
    application_type = models.ForeignKey(
        GroupType, on_delete=models.SET_NULL, blank=True, null=True
    )
    #
    proposal_type = models.CharField(
        "Application Status Type",
        max_length=40,
        choices=APPLICATION_TYPE_CHOICES,
        default=APPLICATION_TYPE_CHOICES[0][0],
    )

    species_taxonomy = models.ForeignKey(
        Taxonomy,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="conservation_statuses",
    )

    # species related conservation status
    species = models.ForeignKey(
        Species,
        on_delete=models.CASCADE,
        related_name="conservation_status",
        null=True,
        blank=True,
    )

    # communties related conservation status
    community = models.ForeignKey(
        Community,
        on_delete=models.CASCADE,
        related_name="conservation_status",
        null=True,
        blank=True,
    )

    conservation_status_number = models.CharField(max_length=9, blank=True, default="")

    # Field to use when importing data from the legacy system
    migrated_from_id = models.CharField(max_length=50, blank=True, default="")

    # Conservation Lists and Categories
    wa_priority_list = models.ForeignKey(
        WAPriorityList,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        related_name="curr_wa_priority_list",
    )
    wa_priority_category = models.ForeignKey(
        WAPriorityCategory,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        related_name="curr_wa_priority_category",
    )
    wa_legislative_list = models.ForeignKey(
        WALegislativeList,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        related_name="curr_wa_legislative_list",
    )
    wa_legislative_category = models.ForeignKey(
        WALegislativeCategory,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        related_name="curr_wa_legislative_category",
    )
    iucn_version = models.ForeignKey(
        IUCNVersion,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        related_name="curr_iucn_version",
    )
    # Although this field is a relationship to CommonwealthConservationList
    # the business requirements was that it should be called a "category"
    commonwealth_conservation_category = models.ForeignKey(
        CommonwealthConservationList,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        # Leave the following as _list otherwise django has remove the field and create a new one
        related_name="curr_commonwealth_conservation_list",
    )
    other_conservation_assessment = models.ForeignKey(
        OtherConservationAssessmentList,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )
    conservation_criteria = models.CharField(max_length=100, blank=True, null=True)
    cam_mou = models.BooleanField(null=True, blank=True)
    cam_mou_date_sent = models.DateField(null=True, blank=True)
    public_consultation = models.BooleanField(default=False, blank=True)
    public_consultation_start_date = models.DateField(null=True, blank=True)
    public_consultation_end_date = models.DateField(null=True, blank=True)

    APPROVAL_LEVEL_IMMEDIATE = "immediate"
    APPROVAL_LEVEL_MINISTER = "minister"
    APPROVAL_LEVEL_CHOICES = (
        (APPROVAL_LEVEL_IMMEDIATE, "Immediate"),
        (APPROVAL_LEVEL_MINISTER, "Ministerial"),
    )

    approval_level = models.CharField(
        max_length=20,
        choices=APPROVAL_LEVEL_CHOICES,
        null=True,
    )

    comment = models.TextField(blank=True, null=True)
    review_due_date = models.DateField(null=True, blank=True)
    effective_from = models.DateField(null=True, blank=True)
    effective_to = models.DateField(null=True, blank=True)
    submitter = models.IntegerField(null=True)  # EmailUserRO
    submitter_information = models.OneToOneField(
        SubmitterInformation,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="conservation_status",
    )
    lodgement_date = models.DateTimeField(blank=True, null=True)

    assigned_officer = models.IntegerField(null=True)  # EmailUserRO
    assigned_approver = models.IntegerField(null=True)  # EmailUserRO
    approved_by = models.IntegerField(null=True)  # EmailUserRO
    processing_status = models.CharField(
        "Processing Status",
        max_length=30,
        choices=PROCESSING_STATUS_CHOICES,
        default=PROCESSING_STATUS_CHOICES[0][0],
    )

    proposed_decline_status = models.BooleanField(default=False)
    assessor_data = models.TextField(null=True, blank=True)  # assessor comment

    # When the CS proposal is sent back to the assessor this comment is used
    # in the email
    approver_comment = models.TextField(blank=True)
    internal_application = models.BooleanField(default=False)

    # Date first listed
    listing_date = models.DateField(null=True, blank=True)

    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)

    migration_run = models.ForeignKey(
        "boranga.MigrationRun",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="conservation_statuses",
    )

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return str(self.conservation_status_number)

    def save(self, *args, **kwargs):
        if self.conservation_status_number == "":
            force_insert = kwargs.pop("force_insert", False)
            super().save(no_revision=True, force_insert=force_insert)
            new_conservation_status_id = f"CS{str(self.pk)}"
            self.conservation_status_number = new_conservation_status_id
            self.save(*args, **kwargs)
        else:
            self.assign_or_create_species()
            super().save(*args, **kwargs)

    def assign_or_create_species(self):
        if not self.species_taxonomy or self.species:
            return

        species = Species.objects.filter(
            taxonomy=self.species_taxonomy,
        ).first()
        if not species:
            species = Species.objects.create(
                taxonomy=self.species_taxonomy,
                group_type=self.species_taxonomy.kingdom_fk.grouptype,
            )
        self.species = species

    @property
    def editing_window_minutes(self):
        return settings.UNLOCKED_CONSERVATION_STATUS_EDITING_WINDOW_MINUTES

    @property
    def reference(self):
        return f"{self.conservation_status_number}"

    @property
    def group_type(self):
        if self.species:
            return self.species.group_type.get_name_display()
        elif self.community:
            return self.community.group_type.get_name_display()
        else:
            return (
                self.application_type.get_name_display()
            )  # when the form is incomplete

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
            return f"{email_user.first_name} {email_user.last_name}"

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
            # return self.APPLICANT_TYPE_SUBMITTER
            return "SUB"

    @property
    def applicant_field(self):
        return "submitter"

    def log_user_action(self, action, request):
        return ConservationStatusUserAction.log_action(self, action, request.user.id)

    @property
    def is_assigned(self):
        return self.assigned_officer is not None

    @property
    def can_officer_process(self):
        officer_view_state = [
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_DRAFT,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_DISCARDED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
        ]
        return self.processing_status not in officer_view_state

    @property
    def can_approver_process(self):
        approver_process_state = [
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
        ]
        unlocked_editing_statuses = [
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
        ]
        return (
            self.processing_status in unlocked_editing_statuses and not self.locked
        ) or self.processing_status in approver_process_state

    @property
    def can_officer_edit(self):
        officer_edit_state = [ConservationStatus.PROCESSING_STATUS_APPROVED]
        return self.processing_status in officer_edit_state

    @property
    def can_user_edit(self):
        return self.customer_status in self.CUSTOMER_EDITABLE_STATE

    @property
    def can_user_view(self):
        return self.customer_status in self.CUSTOMER_VIEWABLE_STATE

    @property
    def is_deletable(self):
        return (
            self.customer_status == ConservationStatus.CUSTOMER_STATUS_DRAFT
            and not self.conservation_status_number
        )

    @property
    def latest_referrals(self):
        return self.referrals.all()[: settings.RECENT_REFERRAL_COUNT]

    @property
    def is_flora_application(self):
        return self.application_type.name == GroupType.GROUP_TYPE_FLORA

    @property
    def is_fauna_application(self):
        return self.application_type.name == GroupType.GROUP_TYPE_FAUNA

    @property
    def is_community_application(self):
        return self.application_type.name == GroupType.GROUP_TYPE_COMMUNITY

    @property
    def allowed_assessors(self):
        group_ids = None
        if not self.locked or self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_ON_AGENDA,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
        ]:
            group_ids = member_ids(GROUP_NAME_CONSERVATION_STATUS_APPROVER)
        elif self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
            ConservationStatus.PROCESSING_STATUS_DEFERRED,
        ]:
            group_ids = member_ids(GROUP_NAME_CONSERVATION_STATUS_ASSESSOR)

        users = (
            list(
                map(
                    lambda id: retrieve_email_user(id),
                    group_ids,
                )
            )
            if group_ids
            else []
        )
        return users

    @property
    def current_conservation_status(self):
        if self.species:
            current_conservation_statuses = ConservationStatus.objects.filter(
                species=self.species,
            )
            warning = f"Multiple approved conservation statuses for {self.species}"
        elif self.community:
            current_conservation_statuses = ConservationStatus.objects.filter(
                community=self.community,
            )
            warning = f"Multiple approved conservation statuses for {self.community}"
        else:
            return None

        current_conservation_statuses = current_conservation_statuses.filter(
            processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED,
        )
        current_conservation_statuses = current_conservation_statuses.exclude(
            id=self.id
        )
        if current_conservation_statuses.count() > 1:
            logger.warning(warning)

        return current_conservation_statuses.first()

    @property
    def is_conservation_status_under_review(self):
        # If a CS for the same species or community is under review (i.e. ready for agenda)
        if self.species:
            conservation_statuses = ConservationStatus.objects.filter(
                species=self.species,
            )
        elif self.community:
            conservation_statuses = ConservationStatus.objects.filter(
                community=self.community,
            )
        else:
            return False

        return (
            conservation_statuses.exclude(id=self.id)
            .filter(
                processing_status=ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            )
            .exists()
        )

    @property
    def conservation_status_under_review(self):
        # If a CS for the same species or community is under review (i.e. ready for agenda)
        if self.species:
            conservation_statuses = ConservationStatus.objects.filter(
                species=self.species,
            )
        elif self.community:
            conservation_statuses = ConservationStatus.objects.filter(
                community=self.community,
            )
        else:
            return None

        return conservation_statuses.filter(
            processing_status=ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
        ).first()

    def can_assess(self, request):
        if self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_DEFERRED,
        ]:
            return is_conservation_status_assessor(request)
        elif not self.locked or self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_ON_AGENDA,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_DEFERRED,
        ]:
            return is_conservation_status_approver(request)

        return False

    def assessor_comments_view(self, request):
        if not self.locked or self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
            ConservationStatus.PROCESSING_STATUS_DEFERRED,
        ]:
            if ConservationStatusReferral.objects.filter(
                conservation_status=self, referral=request.user.id
            ).exists():
                return True

            return is_conservation_status_approver(
                request
            ) or is_conservation_status_assessor(request)
        return False

    @property
    def status_without_assessor(self):
        status_without_assessor = [
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_DRAFT,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
        ]

        return self.processing_status in status_without_assessor

    def can_user_assign_to_self(self, request):
        if self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
            ConservationStatus.PROCESSING_STATUS_DEFERRED,
        ]:
            return (
                self.assigned_officer != request.user.id
                and is_conservation_status_assessor(request)
            )

        if self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_ON_AGENDA,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
        ]:
            return (
                self.assigned_approver != request.user.id
                and is_conservation_status_approver(request)
            )

    def has_assessor_mode(self, request):
        status_without_assessor = [
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
            ConservationStatus.PROCESSING_STATUS_DRAFT,
        ]
        if self.processing_status in status_without_assessor:
            return False

        if self.processing_status == ConservationStatus.PROCESSING_STATUS_APPROVED:
            # Edge case that allows assessors to propose to delist without being assigned
            # to the conservation status. This is due to the fact we only show either
            # the assigned to dropdown for the approver or assessor and not both.
            return is_conservation_status_assessor(request)

        elif not self.locked:
            return is_conservation_status_approver(request)
        else:
            if not self.assigned_officer:
                return False

            if not self.assigned_officer == request.user.id:
                return False

            return is_conservation_status_assessor(request)

    @transaction.atomic
    def assign_officer(self, request, officer):
        if not is_conservation_status_assessor(
            request
        ) and not is_conservation_status_approver(request):
            raise ValidationError(
                f"Officer with id {officer} is not authorised to be assigned to this conservation status"
            )

        approver_statuses = [
            ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_ON_AGENDA,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
        ]

        if (
            not self.locked or self.processing_status in approver_statuses
        ) and is_conservation_status_approver(request):
            if officer == self.assigned_approver:
                logger.warning(
                    "Approver is already assigned to this conservation status"
                )
                return

            self.assigned_approver = officer.id
            self.save()

            # Create a log entry for the conservation status
            self.log_user_action(
                ConservationStatusUserAction.ACTION_ASSIGN_TO_APPROVER.format(
                    self.conservation_status_number,
                    f"{officer.get_full_name()}({officer.email})",
                ),
                request,
            )

            # Create a log entry for the user
            request.user.log_user_action(
                ConservationStatusUserAction.ACTION_ASSIGN_TO_APPROVER.format(
                    self.conservation_status_number,
                    f"{officer.get_full_name()}({officer.email})",
                ),
                request,
            )

        assessor_statuses = [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
            ConservationStatus.PROCESSING_STATUS_DEFERRED,
        ]

        if (
            self.processing_status in assessor_statuses
            and is_conservation_status_assessor(request)
        ):
            if officer == self.assigned_officer:
                logger.warning(
                    "Assessor is already assigned to this conservation status"
                )
                return

            self.assigned_officer = officer.id
            self.save()

            # Create a log entry for the conservation status
            self.log_user_action(
                ConservationStatusUserAction.ACTION_ASSIGN_TO_ASSESSOR.format(
                    self.conservation_status_number,
                    f"{officer.get_full_name()}({officer.email})",
                ),
                request,
            )

            # Create a log entry for the user
            request.user.log_user_action(
                ConservationStatusUserAction.ACTION_ASSIGN_TO_ASSESSOR.format(
                    self.conservation_status_number,
                    f"{officer.get_full_name()}({officer.email})",
                ),
                request,
            )

    @transaction.atomic
    def unassign(self, request):
        if not self.locked or self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_ON_AGENDA,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
        ]:
            if self.assigned_approver:
                self.assigned_approver = None
                self.save()

                # Create a log entry for the proposal
                self.log_user_action(
                    ConservationStatusUserAction.ACTION_UNASSIGN_APPROVER.format(
                        self.conservation_status_number
                    ),
                    request,
                )

                # Create a log entry for the user
                request.user.log_user_action(
                    ConservationStatusUserAction.ACTION_UNASSIGN_APPROVER.format(
                        self.conservation_status_number
                    ),
                    request,
                )
        else:
            if self.assigned_officer:
                self.assigned_officer = None
                self.save()

                # Create a log entry for the proposal
                self.log_user_action(
                    ConservationStatusUserAction.ACTION_UNASSIGN_ASSESSOR.format(
                        self.conservation_status_number
                    ),
                    request,
                )

                # Create a log entry for the user
                request.user.log_user_action(
                    ConservationStatusUserAction.ACTION_UNASSIGN_ASSESSOR.format(
                        self.conservation_status_number
                    ),
                    request,
                )

    @transaction.atomic
    def send_referral(self, request, referral_email, referral_text):
        referral_email = referral_email.lower()
        if self.processing_status not in [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
        ]:
            raise exceptions.ConservationStatusReferralCannotBeSent()

        # Check if the user is in ledger
        try:
            referee = EmailUser.objects.get(email__iexact=referral_email.strip())
        except EmailUser.DoesNotExist:
            raise ValidationError(
                f"There is no user with email {referral_email} in the ledger system. "
                "Please check the email and try again."
            )

        # Don't allow users to refer to themselves
        if referee.id == request.user.id:
            raise ValidationError("You cannot refer to yourself")

        # Don't allow users to refer to the submitter
        if referee.id == self.submitter:
            raise ValidationError("You cannot refer to the submitter")

        referral = None
        try:
            ConservationStatusReferral.objects.get(
                referral=referee.id, conservation_status=self
            )
            raise ValidationError("A referral has already been sent to this user")
        except ConservationStatusReferral.DoesNotExist:
            referral = ConservationStatusReferral.objects.create(
                conservation_status=self,
                referral=referee.id,
                sent_by=request.user.id,
                text=referral_text,
                assigned_officer=request.user.id,
            )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
        self.save()

        # Create a log entry for the proposal
        self.log_user_action(
            ConservationStatusUserAction.ACTION_SEND_REFERRAL_TO.format(
                referral.id,
                self.conservation_status_number,
                f"{referee.get_full_name()}({referee.email})",
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_SEND_REFERRAL_TO.format(
                referral.id,
                self.conservation_status_number,
                f"{referee.get_full_name()}({referee.email})",
            ),
            request,
        )

        # send email
        send_conservation_status_referral_email_notification(referral, request)

    @property
    def amendment_requests(self):
        qs = ConservationStatusAmendmentRequest.objects.filter(conservation_status=self)
        return qs

    @property
    def most_recent_meeting(self):
        Meeting = apps.get_model("boranga", "Meeting")
        meetings = Meeting.objects.order_by("-datetime_updated").filter(
            agenda_items__conservation_status_id=self.id
        )
        if not meetings.exists():
            return None

        most_recent_meeting = meetings.first()
        return most_recent_meeting

    @property
    def most_recent_meeting_completed(self):
        Meeting = apps.get_model("boranga", "Meeting")
        most_recent_meeting = self.most_recent_meeting

        if not most_recent_meeting:
            return False

        return (
            most_recent_meeting.processing_status == Meeting.PROCESSING_STATUS_COMPLETED
        )

    def move_to_status(self, request, status, approver_comment):
        if not self.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        if status not in [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            ConservationStatus.PROCESSING_STATUS_APPROVED,
        ]:
            raise ValidationError("The provided status cannot be found.")

        if (
            self.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
            or self.can_user_edit
        ):
            raise ValidationError("You cannot change the current status at this time")

        if self.processing_status == status:
            return

        if (
            self.processing_status
            == ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA
        ):
            self.approver_comment = ""
            if approver_comment:
                self.approver_comment = approver_comment
                self.save()
                send_proposal_approver_sendback_email_notification(request, self)

        previous_status = self.processing_status
        self.processing_status = status
        self.save()

        # Create a log entry for the conservation status
        self.log_user_action(
            ConservationStatusUserAction.ACTION_MOVE_TO_STATUS.format(
                self.conservation_status_number,
                previous_status,
                self.processing_status,
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_MOVE_TO_STATUS.format(
                self.conservation_status_number,
                previous_status,
                self.processing_status,
            ),
            request,
        )

    @transaction.atomic
    def proposed_decline(self, request, details):
        if not self.can_assess(request):
            raise exceptions.ProposalNotAuthorized()
        if self.processing_status != ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR:
            raise ValidationError(
                "You cannot propose to decline if it is not with assessor"
            )

        reason = details.get("reason")
        ConservationStatusDeclinedDetails.objects.update_or_create(
            conservation_status=self,
            defaults={
                "officer": request.user.id,
                "reason": reason,
                "cc_email": details.get("cc_email", None),
            },
        )
        self.proposed_decline_status = True
        approver_comment = ""

        self.move_to_status(
            request,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            approver_comment,
        )

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSED_DECLINE.format(
                self.conservation_status_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSED_DECLINE.format(
                self.conservation_status_number,
            ),
            request,
        )

        send_approver_decline_email_notification(reason, request, self)

    @transaction.atomic
    def final_decline(self, request, details):
        if not self.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        if not self.can_be_declined:
            raise ValidationError(
                "You can only decline a Conservation Status Proposal "
                "if the processing status is With Assessor AND it has immediate approval level or"
                "the processing status is On Agenda AND it has ministerial approval level"
            )

        conservation_status_decline, created = (
            ConservationStatusDeclinedDetails.objects.update_or_create(
                conservation_status=self,
                defaults={
                    "officer": request.user.id,
                    "reason": details.get("reason"),
                    "cc_email": details.get("cc_email", None),
                },
            )
        )
        self.proposed_decline_status = True
        self.processing_status = ConservationStatus.PROCESSING_STATUS_DECLINED
        self.locked = True
        self.customer_status = ConservationStatus.CUSTOMER_STATUS_DECLINED

        self.save()

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_DECLINE.format(
                self.conservation_status_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_DECLINE.format(
                self.conservation_status_number,
            ),
            request,
        )

    @transaction.atomic
    def proposed_approval(self, request, details):
        if not self.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        if self.processing_status != ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR:
            raise ValidationError(
                "You cannot propose for approval if it is not with assessor"
            )

        ConservationStatusIssuanceApprovalDetails.objects.update_or_create(
            conservation_status=self,
            defaults={
                "officer": request.user.id,
                "effective_from_date": details.get("effective_from_date"),
                "effective_to_date": details.get("effective_to_date"),
                "details": details.get("details"),
                "cc_email": details.get("cc_email", None),
            },
        )
        self.proposed_decline_status = False
        approver_comment = ""

        self.move_to_status(
            request,
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
            approver_comment,
        )
        self.assigned_officer = None
        self.save()

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSED_APPROVAL.format(
                self.conservation_status_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSED_APPROVAL.format(
                self.conservation_status_number,
            ),
            request,
        )

        send_approver_approve_email_notification(request, self)

    @property
    def can_be_approved(self):
        return (
            self.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR
            and self.approval_level == ConservationStatus.APPROVAL_LEVEL_IMMEDIATE
        ) or (
            self.processing_status == ConservationStatus.PROCESSING_STATUS_ON_AGENDA
            and self.approval_level == ConservationStatus.APPROVAL_LEVEL_MINISTER
        )

    @property
    def can_be_declined(self):
        return self.can_be_approved

    @transaction.atomic
    def final_approval(self, request, details):
        self.proposed_decline_status = False

        if not self.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        if not self.can_be_approved:
            raise ValidationError(
                "You can only approve a Conservation Status Proposal "
                "if the processing status is With Assessor AND it has immediate approval level or"
                "the processing status is On Agenda AND it has ministerial approval level"
            )

        # For conservation statuses that require ministerial approval
        # check if the meeting is scheduled or completed
        if self.approval_level == ConservationStatus.APPROVAL_LEVEL_MINISTER:
            from boranga.components.meetings.models import Meeting

            added_to_meeting_status = [
                Meeting.PROCESSING_STATUS_SCHEDULED,
                Meeting.PROCESSING_STATUS_COMPLETED,
            ]
            if not self.agendaitem_set.filter(
                meeting__processing_status__in=added_to_meeting_status
            ).exists():
                raise ValidationError(
                    f"Conservation status {self} can not be approved as there are no meetings "
                    f"for ministerial approval that are currently scheduled or completed that "
                    f"include {self} in the agenda."
                )

        # Add the approval document first to to get the reference id in below model
        proposal_approval_document = request.data["proposal_approval_document"]
        d = None
        if proposal_approval_document != "null":
            try:
                document = self.documents.get(name=str(proposal_approval_document))
            except ConservationStatusDocument.DoesNotExist:
                document = self.documents.get_or_create(
                    input_name="conservation_status_approval_doc",
                    name=str(proposal_approval_document),
                )[0]

            document.check_file(proposal_approval_document)
            document.name = str(proposal_approval_document)
            document._file = proposal_approval_document
            document.save()

            d = ConservationStatusDocument.objects.get(id=document.id)

        effective_from = details.get("effective_from_date")
        effective_to = details.get("effective_to_date")
        ConservationStatusIssuanceApprovalDetails.objects.update_or_create(
            conservation_status=self,
            defaults={
                "officer": request.user.id,
                "effective_from_date": effective_from,
                "effective_to_date": effective_to,
                "details": details.get("details"),
                "cc_email": details.get("cc_email", None),
                "conservation_status_approval_document": d,
            },
        )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_APPROVED
        self.customer_status = ConservationStatus.CUSTOMER_STATUS_APPROVED
        self.locked = True
        self.assigned_officer = None
        self.approved_by = request.user.id

        if effective_from:
            self.effective_from = effective_from

        if effective_to:
            self.effective_to = effective_to

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_APPROVE_CONSERVATION_STATUS.format(
                self.conservation_status_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_APPROVE_CONSERVATION_STATUS.format(
                self.conservation_status_number,
            ),
            request,
        )

        # Delist / Close the previous approved version
        previous_approved_version = ConservationStatus.objects.filter(
            processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED
        )

        if self.species:
            previous_approved_version = previous_approved_version.filter(
                species=self.species
            ).first()
        elif self.community:
            previous_approved_version = previous_approved_version.filter(
                community=self.community
            ).first()

        if previous_approved_version:
            # Check if the previous conservation status has a priority list
            if previous_approved_version.wa_priority_list and not self.wa_priority_list:
                self.wa_priority_list = previous_approved_version.wa_priority_list

                # Check if the previous conservation status has a priority category
                if (
                    previous_approved_version.wa_priority_category
                    and not self.wa_priority_category
                ):
                    self.wa_priority_category = (
                        previous_approved_version.wa_priority_category
                    )

            previous_approved_version.customer_status = (
                ConservationStatus.PROCESSING_STATUS_CLOSED
            )
            previous_approved_version.processing_status = (
                ConservationStatus.PROCESSING_STATUS_CLOSED
            )
            previous_approved_version.locked = True
            previous_approved_version.change_code = (
                ConservationChangeCode.get_closed_change_code()
            )
            if self.effective_from:
                previous_approved_version.effective_to = (
                    self.effective_from - timedelta(days=1)
                )
            previous_approved_version.save()

            # Create a log entry for the conservation status
            self.log_user_action(
                ConservationStatusUserAction.ACTION_CLOSE_CONSERVATION_STATUS.format(
                    previous_approved_version.conservation_status_number
                ),
                request,
            )

            # Create a log entry for the user
            request.user.log_user_action(
                ConservationStatusUserAction.ACTION_CLOSE_CONSERVATION_STATUS.format(
                    previous_approved_version.conservation_status_number,
                ),
                request,
            )

        self.save()

    @transaction.atomic
    def proposed_for_agenda(self, request):
        if not self.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        if self.processing_status != ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR:
            raise ValidationError(
                "You cannot propose for ready for agenda if it is not with assessor"
            )

        self.processing_status = (
            ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA
        )

        # Save time by auto assigning the request user if they are in the CS assessor group
        if is_conservation_status_approver(request):
            self.assigned_approver = request.user.id

        self.save()

        assessor_comment = request.data.get("assessor_comment")

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSED_FOR_AGENDA.format(
                self.conservation_status_number, assessor_comment
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSED_FOR_AGENDA.format(
                self.conservation_status_number, assessor_comment
            ),
            request,
        )

        send_approver_proposed_for_agenda_email_notification(
            request, self, assessor_comment
        )

    @transaction.atomic
    def ready_for_agenda(self, request):
        if not self.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        if (
            self.processing_status
            != ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA
        ):
            raise ValidationError(
                "You cannot change the processing status to 'Ready for Agenda'"
                "unless it is currently 'Proposed for Agenda'"
            )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA
        self.save()

        assessor_comment = request.data.get("assessor_comment")

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_READY_FOR_AGENDA.format(
                self.conservation_status_number, assessor_comment
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_READY_FOR_AGENDA.format(
                self.conservation_status_number, assessor_comment
            ),
            request,
        )

        send_assessor_ready_for_agenda_email_notification(
            request, self, assessor_comment
        )

    @transaction.atomic
    def discard(self, request):
        if self.lodgement_date:
            raise ValidationError(
                "You cannot discard a conservation status that has been submitted"
            )

        if not self.processing_status == ConservationStatus.PROCESSING_STATUS_DRAFT:
            raise ValidationError(
                "You cannot discard a conservation status that is not a draft"
            )

        if not request.user.id == self.submitter:
            raise ValidationError(
                "You cannot discard a conservation status that is not yours"
            )

        if not is_external_contributor(request) and not is_internal_contributor(
            request
        ):
            raise ValidationError(
                "You cannot discard a conservation status unless you are a contributor"
            )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_DISCARDED
        self.customer_status = ConservationStatus.CUSTOMER_STATUS_DISCARDED
        self.save()

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_DISCARD_PROPOSAL.format(
                self.conservation_status_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_DISCARD_PROPOSAL.format(
                self.conservation_status_number,
            ),
            request,
        )

    @transaction.atomic
    def reinstate(self, request):
        if self.lodgement_date:
            raise ValidationError(
                "You cannot reinstate a conservation status that has been submitted"
            )

        if not self.processing_status == ConservationStatus.PROCESSING_STATUS_DISCARDED:
            raise ValidationError(
                "You cannot reinstate a conservation status that has not been discarded"
            )

        if (
            not is_conservation_status_assessor(request)
            and request.user.id != self.submitter
        ):
            raise ValidationError(
                "You cannot reinstate a conservation status that is not yours"
            )

        if not is_external_contributor(request) and not is_internal_contributor(
            request
        ):
            raise ValidationError(
                "You cannot reinstate a conservation status unless you are a contributor"
            )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_DRAFT
        self.customer_status = ConservationStatus.CUSTOMER_STATUS_DRAFT
        self.save()

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_REINSTATE_PROPOSAL.format(
                self.conservation_status_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_REINSTATE_PROPOSAL.format(
                self.conservation_status_number,
            ),
            request,
        )

    @transaction.atomic
    def propose_delist(self, request):
        if not self.processing_status == ConservationStatus.PROCESSING_STATUS_APPROVED:
            raise ValidationError(
                "You cannot propose to delist a conservation status that is not approved"
            )

        if not is_conservation_status_assessor(request):
            raise ValidationError(
                "You cannot propose to delist a conservation status unless you "
                "are a member of the conservation status assessor group"
            )

        self.effective_to = datetime.strptime(
            request.data.get("effective_to"), "%Y-%m-%d"
        ).date()
        self.processing_status = ConservationStatus.PROCESSING_STATUS_WITH_APPROVER
        self.save()

        reason = request.data.get("reason")

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSE_DELIST_PROPOSAL.format(
                self.conservation_status_number, reason
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_PROPOSE_DELIST_PROPOSAL.format(
                self.conservation_status_number, reason
            ),
            request,
        )

        send_approver_propose_delist_email_notification(request, self, reason)

    @transaction.atomic
    def delist(self, request):
        if (
            not self.processing_status
            == ConservationStatus.PROCESSING_STATUS_WITH_APPROVER
        ):
            raise ValidationError(
                "You cannot delist a conservation status that is not with approver"
            )

        if not self.assigned_approver == request.user.id:
            raise ValidationError(
                "You cannot delist a conservation status that you are not assigned to"
            )

        if not is_conservation_status_approver(request):
            raise ValidationError(
                "You cannot delist a conservation status unless you are a "
                "member of the conservation status approver group"
            )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_DELISTED
        self.locked = True
        self.save()

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_DELIST_PROPOSAL.format(
                self.conservation_status_number
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_DELIST_PROPOSAL.format(
                self.conservation_status_number,
            ),
            request,
        )

    @transaction.atomic
    def defer(self, request, reason, review_due_date):
        if self.processing_status not in [
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
            ConservationStatus.PROCESSING_STATUS_PROPOSED_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
            ConservationStatus.PROCESSING_STATUS_ON_AGENDA,
        ]:
            raise ValidationError(
                "You cannot defer a conservation status when the "
                f"processing status is {self.get_processing_status_display()}"
            )

        if not is_conservation_status_assessor(
            request
        ) and not is_conservation_status_approver(request):
            raise ValidationError(
                "You cannot defer a conservation status unless you are a member "
                "of the conservation status assessor or approver group"
            )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_DEFERRED
        if review_due_date:
            self.review_due_date = datetime.strptime(review_due_date, "%Y-%m-%d").date()
        self.save()

        # Log proposal action
        self.log_user_action(
            ConservationStatusUserAction.ACTION_DEFER_PROPOSAL.format(
                self.conservation_status_number, reason
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_DEFER_PROPOSAL.format(
                self.conservation_status_number, reason
            ),
            request,
        )

        send_approver_defer_email_notification(request, self, reason)

    def get_related_items(self, filter_type, **kwargs):
        return_list = []
        if filter_type == "all":
            related_field_names = [
                "species",
                "community",
                "agendaitem",
                "occurrences",
                "occurrence_report",
            ]
        else:
            related_field_names = [
                filter_type,
            ]
        all_fields = self._meta.get_fields()
        for a_field in all_fields:
            if a_field.name in related_field_names:
                field_objects = []
                if a_field.is_relation:
                    if a_field.many_to_many:
                        pass
                    elif a_field.many_to_one:  # foreign key
                        field_objects = [
                            getattr(self, a_field.name),
                        ]
                    elif a_field.one_to_many:  # reverse foreign key
                        field_objects = a_field.related_model.objects.filter(
                            **{a_field.remote_field.name: self}
                        )
                    elif a_field.one_to_one:
                        if hasattr(self, a_field.name):
                            field_objects = [getattr(self, a_field.name)]
                for field_object in field_objects:
                    if field_object:
                        related_item = field_object.as_related_item
                        if related_item not in return_list:
                            return_list.append(related_item)

        species_filter = []
        if "occurrences" in related_field_names:
            species_filter.append("occurrences")
        if "occurrence_report" in related_field_names:
            species_filter.append("occurrence_report")
        for occ_filter_type in species_filter:
            if self.species:
                species_occurences = self.species.get_related_items(occ_filter_type)
                if species_occurences:
                    return_list += species_occurences
            if self.community:
                community_occurences = self.community.get_related_items(occ_filter_type)
                if community_occurences:
                    return_list += community_occurences

        # Remove duplicates
        return_list = list(set(return_list))

        return return_list

    @property
    def as_related_item(self):
        related_item = RelatedItem(
            identifier=self.related_item_identifier,
            model_name=self._meta.verbose_name.title(),
            descriptor=self.related_item_descriptor,
            status=self.related_item_status,
            action_url=str(
                f"<a href=/internal/conservation-status/{self.id} "
                f'target="_blank">View <i class="bi bi-box-arrow-up-right"></i></a>'
            ),
        )
        return related_item

    @property
    def related_item_identifier(self):
        return self.conservation_status_number

    @property
    def related_item_descriptor(self):
        descriptor = ""
        if self.wa_legislative_list:
            descriptor = self.wa_legislative_list.code
        if self.wa_legislative_category:
            descriptor = f"{descriptor} - {self.wa_legislative_category.code}"
        if self.wa_priority_list:
            if descriptor:
                descriptor += ", "
            descriptor = f"{descriptor} {self.wa_priority_list.code}"
        if self.wa_priority_category:
            descriptor = f"{descriptor} - {self.wa_priority_category.code}"
        return descriptor

    @property
    def related_item_status(self):
        return self.get_processing_status_display()

    @property
    def is_finalised(self):
        return self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
        ]

    @property
    def show_locked_indicator(self):
        return self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
        ]

    def lock(self, request):
        if self.locked:
            raise ValidationError(
                "You cannot lock a conservation status that is already locked"
            )
        if not is_conservation_status_approver(request):
            raise ValidationError(
                "You cannot lock a conservation status unless you are a member "
                "of the conservation status approver group"
            )

        self.locked = True
        self.save(version_user=request.user)

        self.log_user_action(
            ConservationStatusUserAction.ACTION_LOCK.format(
                self.conservation_status_number
            ),
            request,
        )

        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_LOCK.format(
                self.conservation_status_number
            ),
            request,
        )

    def unlock(self, request):
        if not self.locked:
            raise ValidationError(
                "You cannot unlock a conservation status that is already unlocked"
            )

        if not is_conservation_status_approver(request):
            raise ValidationError(
                "You cannot unlock a conservation status unless you are a member "
                "of the conservation status approver group"
            )

        self.locked = False
        self.save(version_user=request.user)

        self.log_user_action(
            ConservationStatusUserAction.ACTION_UNLOCK.format(
                self.conservation_status_number
            ),
            request,
        )

        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_UNLOCK.format(
                self.conservation_status_number
            ),
            request,
        )

    def has_unlocked_mode(self, request):
        if self.locked:
            return False

        if not self.assigned_approver:
            return False

        if not self.assigned_approver == request.user.id:
            return False

        return not self.locked and is_conservation_status_approver(request)

    @property
    def external_referral_invites(self):
        return self.external_referee_invites.filter(
            archived=False, datetime_first_logged_in__isnull=True
        )


class ConservationStatusLogEntry(CommunicationsLogEntry):
    conservation_status = models.ForeignKey(
        ConservationStatus, related_name="comms_logs", on_delete=models.CASCADE
    )

    def __str__(self):
        return f"{self.reference} - {self.subject}"

    class Meta:
        app_label = "boranga"

    def save(self, **kwargs):
        # save the application reference if the reference not provided
        if not self.reference:
            self.reference = self.conservation_status.reference
        super().save(**kwargs)


class ConservationStatusLogDocument(Document):
    log_entry = models.ForeignKey(
        "ConservationStatusLogEntry", related_name="documents", on_delete=models.CASCADE
    )
    _file = models.FileField(
        upload_to=update_conservation_status_comms_log_filename,
        max_length=512,
        storage=private_storage,
    )

    class Meta:
        app_label = "boranga"

    def get_parent_instance(self) -> BaseModel:
        return self.log_entry


class ConservationStatusUserAction(UserAction):
    # ConservationStatus Proposal
    ACTION_EDIT_CONSERVATION_STATUS = "Edit Conservation Status {}"
    ACTION_LODGE_PROPOSAL = "Lodge proposal for conservation status {}"
    ACTION_SAVE_APPLICATION = "Save proposal {}"
    ACTION_EDIT_APPLICATION = "Edit proposal {}"
    ACTION_ASSIGN_TO_ASSESSOR = (
        "Assign conservation status proposal {} to {} as the assessor"
    )
    ACTION_UNASSIGN_ASSESSOR = "Unassign assessor from conservation status proposal {}"
    ACTION_ASSIGN_TO_APPROVER = (
        "Assign conservation status proposal {} to {} as the approver"
    )
    ACTION_UNASSIGN_APPROVER = "Unassign approver from conservation status proposal {}"
    ACTION_DECLINE = "Decline conservation status application {}"
    ACTION_APPROVE_CONSERVATION_STATUS = "Approve conservation status proposal {}"
    ACTION_CLOSE_CONSERVATION_STATUS = "Conservation status {} has been closed"
    ACTION_CLOSE_CONSERVATION_STATUS_DUE_TO_RENAME = (
        "Conservation status {} has been closed due to a rename"
    )
    ACTION_CLOSE_CONSERVATION_STATUS_DUE_TO_SPLIT = (
        "Conservation status {} has been closed due to a split"
    )
    ACTION_CLOSE_CONSERVATION_STATUS_DUE_TO_COMBINE = (
        "Conservation status {} has been closed due to a combine"
    )
    ACTION_DISCARD_PROPOSAL = "Discard conservation status proposal {}"
    ACTION_PROPOSE_DELIST_PROPOSAL = (
        "Propose discard conservation status proposal {}. Reason: {}"
    )
    ACTION_PROPOSED_FOR_AGENDA = (
        "Conservation status proposal {} "
        "has been proposed for agenda. Assessor Comment: {}"
    )
    ACTION_READY_FOR_AGENDA = (
        "Conservation status proposal {} " "is ready for agenda. Assessor Comment: {}"
    )
    ACTION_DELIST_PROPOSAL = "Delist conservation status proposal {}"
    ACTION_DEFER_PROPOSAL = "Defer conservation status proposal {}. Reason: {}"
    ACTION_DISCARD_PROPOSAL_INTERNALLY = (
        "Discard conservation status proposal internally {}"
    )
    ACTION_REINSTATE_PROPOSAL = "Reinstate conservation status proposal {}"
    ACTION_APPROVAL_LEVEL_DOCUMENT = "Assign Approval level document {}"
    ACTION_UNLOCK = "Unlock conservation status proposal {}"
    ACTION_LOCK = "Lock conservation status proposal {}"

    # Amendment
    ACTION_ID_REQUEST_AMENDMENTS = "Request amendments"

    # Assessors
    ACTION_SAVE_ASSESSMENT_ = "Save assessment {}"
    ACTION_CONCLUDE_ASSESSMENT_ = "Conclude assessment {}"
    ACTION_PROPOSED_APPROVAL = (
        "Conservation status proposal {} has been proposed for approval"
    )
    ACTION_PROPOSED_DECLINE = (
        "Conservation status proposal {} has been proposed for decline"
    )

    # Referrals
    ACTION_SEND_REFERRAL_TO = (
        "Send referral {} for conservation status proposal {} to {}"
    )
    ACTION_RESEND_REFERRAL_TO = (
        "Resend referral {} for conservation status proposal {} to {}"
    )
    ACTION_REMIND_REFERRAL = (
        "Send reminder for referral {} for conservation status proposal {} to {}"
    )
    ACTION_MOVE_TO_STATUS = (
        "Change status for conservation status proposal {} from {} to {}"
    )
    RECALL_REFERRAL = (
        "Referral {} for conservation status proposal {} has been recalled"
    )
    SAVE_REFERRAL = (
        "Referral {} for conservation status proposal {} has been saved by {}"
    )
    CONCLUDE_REFERRAL = (
        "Referral {} for conservation status proposal {} has been concluded by {}"
    )

    class Meta:
        app_label = "boranga"
        ordering = ("-when",)

    @classmethod
    def log_action(cls, conservation_status, action, user):
        return cls.objects.create(
            conservation_status=conservation_status, who=user, what=str(action)
        )

    conservation_status = models.ForeignKey(
        ConservationStatus, related_name="action_logs", on_delete=models.CASCADE
    )


class ConservationStatusDocument(Document):
    document_number = models.CharField(max_length=9, blank=True, default="")
    conservation_status = models.ForeignKey(
        "ConservationStatus", related_name="documents", on_delete=models.CASCADE
    )
    _file = models.FileField(
        upload_to=update_conservation_status_doc_filename,
        max_length=512,
        storage=private_storage,
    )
    input_name = models.CharField(max_length=255, null=True, blank=True)
    can_submitter_access = models.BooleanField(default=False)
    document_category = models.ForeignKey(
        DocumentCategory, null=True, blank=True, on_delete=models.SET_NULL
    )
    document_sub_category = models.ForeignKey(
        DocumentSubCategory, null=True, blank=True, on_delete=models.SET_NULL
    )

    class Meta:
        app_label = "boranga"
        verbose_name = "Conservation Status Document"

    def get_parent_instance(self):
        return self.conservation_status

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


class ConservationStatusDeclinedDetails(BaseModel):
    conservation_status = models.OneToOneField(
        ConservationStatus, on_delete=models.CASCADE
    )
    officer = models.IntegerField()  # EmailUserRO
    reason = models.TextField(blank=True)
    cc_email = models.TextField(null=True)

    class Meta:
        app_label = "boranga"


class ConservationStatusIssuanceApprovalDetails(BaseModel):
    conservation_status = models.OneToOneField(
        ConservationStatus, on_delete=models.CASCADE
    )
    officer = models.IntegerField()  # EmailUserRO
    effective_from_date = models.DateField(null=True, blank=True)
    effective_to_date = models.DateField(null=True, blank=True)
    details = models.TextField(null=True, blank=True)
    cc_email = models.TextField(null=True)
    conservation_status_approval_document = models.ForeignKey(
        ConservationStatusDocument,
        blank=True,
        null=True,
        related_name="conservation_status_approval_document",
        on_delete=models.SET_NULL,
    )

    class Meta:
        app_label = "boranga"


class ConservationStatusReferral(BaseModel):
    PROCESSING_STATUS_WITH_REFERRAL = "with_referral"
    PROCESSING_STATUS_RECALLED = "recalled"
    PROCESSING_STATUS_COMPLETED = "completed"
    PROCESSING_STATUS_CHOICES = (
        (PROCESSING_STATUS_WITH_REFERRAL, "Awaiting"),
        (PROCESSING_STATUS_RECALLED, "Recalled"),
        (PROCESSING_STATUS_COMPLETED, "Completed"),
    )
    lodged_on = models.DateTimeField(auto_now_add=True)
    conservation_status = models.ForeignKey(
        ConservationStatus, related_name="referrals", on_delete=models.CASCADE
    )
    sent_by = models.IntegerField()  # EmailUserRO
    referral = models.IntegerField()  # EmailUserRO
    linked = models.BooleanField(default=False)
    processing_status = models.CharField(
        "Processing Status",
        max_length=30,
        choices=PROCESSING_STATUS_CHOICES,
        default=PROCESSING_STATUS_CHOICES[0][0],
    )
    text = models.TextField(blank=True)  # Assessor text when send_referral
    referral_comment = models.TextField(blank=True, null=True)  # Referral Comment
    assigned_officer = models.IntegerField(null=True)  # EmailUserRO
    is_external = models.BooleanField(default=False)

    class Meta:
        app_label = "boranga"
        ordering = ("-lodged_on",)

    def __str__(self):
        return f"Application {self.conservation_status.id} - Referral {self.id}"

    @property
    def can_be_completed(self):
        # Referral cannot be completed until second level referral sent by referral has been completed/recalled
        return not ConservationStatusReferral.objects.filter(
            sent_by=self.referral,
            conservation_status=self.conservation_status,
            processing_status=ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL,
        ).exists()

    @property
    def referral_as_email_user(self):
        return retrieve_email_user(self.referral)

    @transaction.atomic
    def remind(self, request):
        if not self.conservation_status.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        # Create a log entry for the proposal
        self.conservation_status.log_user_action(
            ConservationStatusUserAction.ACTION_REMIND_REFERRAL.format(
                self.id,
                self.conservation_status.conservation_status_number,
                f"{self.referral_as_email_user.get_full_name()}",
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_REMIND_REFERRAL.format(
                self.id,
                self.conservation_status.conservation_status_number,
                f"{self.referral_as_email_user.get_full_name()}",
            ),
            request,
        )

        # send email
        send_conservation_status_referral_email_notification(
            self,
            request,
            reminder=True,
        )

    @transaction.atomic
    def recall(self, request):
        if not self.conservation_status.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        self.processing_status = ConservationStatusReferral.PROCESSING_STATUS_RECALLED
        self.save()

        outstanding = self.conservation_status.referrals.filter(
            processing_status=ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL
        )
        if len(outstanding) == 0:
            self.conservation_status.processing_status = (
                ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR
            )
            self.conservation_status.save()

        send_conservation_status_referral_recall_email_notification(self, request)

        # Create a log entry for the conservation status
        self.conservation_status.log_user_action(
            ConservationStatusUserAction.RECALL_REFERRAL.format(
                self.id,
                self.conservation_status.conservation_status_number,
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.RECALL_REFERRAL.format(
                self.id,
                self.conservation_status.conservation_status_number,
                f"{self.referral_as_email_user.get_full_name()}",
            ),
            request,
        )

    @transaction.atomic
    def resend(self, request):
        if not self.conservation_status.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        self.processing_status = (
            ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL
        )
        self.conservation_status.processing_status = (
            ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL
        )
        self.conservation_status.save()

        self.save()

        # Create a log entry for the conservation status
        self.conservation_status.log_user_action(
            ConservationStatusUserAction.ACTION_RESEND_REFERRAL_TO.format(
                self.id,
                self.conservation_status.conservation_status_number,
                f"{self.referral_as_email_user.get_full_name()}({self.referral_as_email_user.email})",
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.RECALL_REFERRAL.format(
                self.id,
                self.conservation_status.conservation_status_number,
                f"{self.referral_as_email_user.get_full_name()}({self.referral_as_email_user.email})",
            ),
            request,
        )

        # send email
        send_conservation_status_referral_email_notification(self, request)

    @transaction.atomic
    def send_referral(self, request, referral_email, referral_text):
        referral_email = referral_email.lower()
        if not (
            self.conservation_status.processing_status
            == ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
        ):
            raise exceptions.ConservationStatusReferralCannotBeSent()

        # Don't allow users to refer to themselves
        if request.user.id == self.referral:
            raise ValidationError("You cannot refer to yourself")

        # Don't allow users to refer to the submitter
        if request.user.id == self.conservation_status.submitter:
            raise ValidationError("You cannot refer to the submitter")

        self.conservation_status.processing_status = (
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
        )
        self.conservation_status.save()
        referral = None

        # Check if the user is in ledger
        try:
            referee = EmailUser.objects.get(email__iexact=referral_email.strip())
        except EmailUser.DoesNotExist:
            raise ValidationError(
                f"There is no user with email {referral_email} in the ledger system. "
                "Please check the email and try again."
            )

        qs = ConservationStatusReferral.objects.filter(
            sent_by=referee.id, conservation_status=self.conservation_status
        )
        if qs:
            raise ValidationError("You cannot send referral to this user")
        try:
            ConservationStatusReferral.objects.get(
                referral=referee.id,
                conservation_status=self.conservation_status,
            )
            raise ValidationError("A referral has already been sent to this user")
        except ConservationStatusReferral.DoesNotExist:
            # Create Referral
            referral = ConservationStatusReferral.objects.create(
                conservation_status=self.conservation_status,
                referral=referee.id,
                sent_by=request.user.id,
                text=referral_text,
            )

        # Create a log entry for the conservation status
        self.conservation_status.log_user_action(
            ConservationStatusUserAction.ACTION_SEND_REFERRAL_TO.format(
                referral.id,
                self.conservation_status.conservation_status_number,
                f"{referee.get_full_name()}({referee.email})",
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_SEND_REFERRAL_TO.format(
                referral.id,
                self.conservation_status.conservation_status_number,
                f"{referee.get_full_name()}({referee.email})",
            ),
            request,
        )

        # send email
        send_conservation_status_referral_email_notification(referral, request)

    @transaction.atomic
    def complete(self, request):
        if request.user.id != self.referral:
            raise exceptions.ReferralNotAuthorized()

        self.processing_status = ConservationStatusReferral.PROCESSING_STATUS_COMPLETED
        self.save()

        outstanding = self.conservation_status.referrals.filter(
            processing_status=ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL
        )
        if len(outstanding) == 0:
            self.conservation_status.processing_status = (
                ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR
            )
            self.conservation_status.save()

        # Create a log entry for the conservation status
        self.conservation_status.log_user_action(
            ConservationStatusUserAction.CONCLUDE_REFERRAL.format(
                self.id,
                self.conservation_status.conservation_status_number,
                f"{self.referral_as_email_user.get_full_name()}({self.referral_as_email_user.email})",
            ),
            request,
        )

        # Create a log entry for the user
        request.user.log_user_action(
            ConservationStatusUserAction.CONCLUDE_REFERRAL.format(
                self.id,
                self.conservation_status.conservation_status_number,
                f"{self.referral_as_email_user.get_full_name()}({self.referral_as_email_user.email})",
            ),
            request,
        )

        send_conservation_status_referral_complete_email_notification(self, request)

    def can_assess_referral(self):
        return (
            self.processing_status
            == ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL
        )

    @property
    def can_be_processed(self):
        return (
            self.processing_status
            == ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL
        )


class ConservationStatusProposalRequest(BaseModel):
    conservation_status = models.ForeignKey(
        ConservationStatus, on_delete=models.CASCADE
    )
    text = models.TextField(blank=True)

    class Meta:
        app_label = "boranga"


class ProposalAmendmentReason(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()

    reason = models.CharField(
        "Reason", max_length=125, validators=[no_commas_validator]
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Proposal Amendment Reason"  # display name in Admin
        verbose_name_plural = "Proposal Amendment Reasons"

    def __str__(self):
        return self.reason


class ConservationStatusAmendmentRequest(ConservationStatusProposalRequest):
    STATUS_CHOICE_REQUESTED = "requested"
    STATUS_CHOICE_AMENDED = "amended"

    STATUS_CHOICES = (
        (STATUS_CHOICE_REQUESTED, "Requested"),
        (STATUS_CHOICE_AMENDED, "Amended"),
    )

    status = models.CharField(
        "Status", max_length=30, choices=STATUS_CHOICES, default=STATUS_CHOICES[0][0]
    )
    reason = models.ForeignKey(
        ProposalAmendmentReason, blank=True, null=True, on_delete=models.SET_NULL
    )

    class Meta:
        app_label = "boranga"

    @transaction.atomic
    def generate_amendment(self, request):
        if not self.conservation_status.can_assess(request):
            raise exceptions.ProposalNotAuthorized()

        if self.status == ConservationStatusAmendmentRequest.STATUS_CHOICE_REQUESTED:
            conservation_status = self.conservation_status
            if (
                conservation_status.processing_status
                != ConservationStatus.PROCESSING_STATUS_DRAFT
            ):
                conservation_status.processing_status = (
                    ConservationStatus.PROCESSING_STATUS_DRAFT
                )
                conservation_status.customer_status = (
                    ConservationStatus.PROCESSING_STATUS_DRAFT
                )
                conservation_status.save()

            # Create a log entry for the conservationstatus
            conservation_status.log_user_action(
                ConservationStatusUserAction.ACTION_ID_REQUEST_AMENDMENTS,
                request,
            )

            # Create a log entry for the user
            request.user.log_user_action(
                ConservationStatusUserAction.ACTION_ID_REQUEST_AMENDMENTS,
                request,
            )

            # send email
            send_conservation_status_amendment_email_notification(
                self, request, conservation_status
            )

        self.save()

    def add_documents(self, request):
        with transaction.atomic():
            # save the files
            data = json.loads(request.data.get("data"))
            if not data.get("update"):
                documents_qs = self.cs_amendment_request_documents.filter(
                    input_name="amendment_request_doc", visible=True
                )
                documents_qs.delete()
            for idx in range(data["num_files"]):
                _file = request.data.get("file-" + str(idx))
                document = self.cs_amendment_request_documents.create(
                    _file=_file, name=_file.name
                )
                document.check_file(request.data.get("file-" + str(idx)))
                document.input_name = data["input_name"]
                document.save()
            # end save documents
            self.save()

        return


class ConservationStatusAmendmentRequestDocument(Document):
    conservation_status_amendment_request = models.ForeignKey(
        "ConservationStatusAmendmentRequest",
        related_name="cs_amendment_request_documents",
        on_delete=models.CASCADE,
    )
    _file = models.FileField(
        upload_to=update_conservation_status_amendment_request_doc_filename,
        max_length=500,
        storage=private_storage,
    )
    input_name = models.CharField(max_length=255, null=True, blank=True)

    def get_parent_instance(self) -> BaseModel:
        return self.conservation_status_amendment_request


class CSExternalRefereeInvite(BaseModel):
    email = models.EmailField()
    first_name = models.CharField(max_length=100)
    last_name = models.CharField(max_length=100)
    datetime_sent = models.DateTimeField(null=True, blank=True)
    datetime_first_logged_in = models.DateTimeField(null=True, blank=True)
    conservation_status = models.ForeignKey(
        ConservationStatus,
        related_name="external_referee_invites",
        on_delete=models.CASCADE,
    )
    sent_by = models.IntegerField()
    invite_text = models.TextField(blank=True)
    archived = models.BooleanField(default=False)

    class Meta:
        app_label = "boranga"
        verbose_name = "External Conservation Status Referral Invite"
        verbose_name_plural = "External Conservation Status Referral Invites"

    def __str__(self):
        return_str = f"{self.first_name} {self.last_name} ({self.email})"
        if self.archived:
            return_str += " - Archived"
        return return_str

    @property
    def full_name(self):
        return f"{self.first_name} {self.last_name}"


# Species Document History
reversion.register(ConservationStatusDocument)

# Conservation Status History
reversion.register(ConservationStatus, follow=["species", "community"])
