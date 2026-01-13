import json
import logging

import reversion
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.files.storage import FileSystemStorage
from django.db import models, transaction
from django.utils import timezone
from ordered_model.models import OrderedModel

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.main.models import (
    ArchivableModel,
    BaseModel,
    CommunicationsLogEntry,
    Document,
    OrderedArchivableManager,
    UserAction,
)
from boranga.components.main.related_item import RelatedItem
from boranga.components.species_and_communities.models import (
    DocumentCategory,
    DocumentSubCategory,
)
from boranga.helpers import is_conservation_status_approver

logger = logging.getLogger(__name__)


def update_meeting_comms_log_filename(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/meeting/{instance.log_entry.meeting.id}/communications/{filename}"


private_storage = FileSystemStorage(
    location=settings.BASE_DIR + "/private-media/", base_url="/private-media/"
)


def update_meeting_doc_filename(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/meeting/{instance.meeting.id}/meeting_minutes_document/{filename}"


class MeetingRoom(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()

    room_name = models.CharField(max_length=128, blank=True, null=True)

    class Meta:
        app_label = "boranga"
        verbose_name = "Meeting Room"
        verbose_name_plural = "Meeting Rooms"
        ordering = ["room_name"]

    def __str__(self):
        return str(self.room_name)


class Committee(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()

    name = models.CharField(max_length=328, blank=True, null=True)

    class Meta(OrderedModel.Meta):
        app_label = "boranga"

    def __str__(self):
        return str(self.name)


class CommitteeMembers(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()

    first_name = models.CharField(max_length=128, blank=True, null=True)
    last_name = models.CharField(max_length=128, blank=True, null=True)
    email = models.CharField(max_length=328, blank=True, null=True)
    committee = models.ForeignKey(
        "Committee", on_delete=models.CASCADE, null=True, blank=True
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name = "Committee Member"
        verbose_name_plural = "Committee Members"

    def __str__(self):
        return str(self.email)


class Meeting(BaseModel):
    """
    A list of conservation status for a species is executed during Meetings or Committee Meetings.
    It is necessary to capture these changes and the meetings that caused the change.

    Has a:
    - Contact
    """

    MEETING = "meeting"
    COMMITTEE_MEETING = "committee_meeting"
    PROCESSING_STATUS_DRAFT = "draft"
    PROCESSING_STATUS_SCHEDULED = "scheduled"
    PROCESSING_STATUS_COMPLETED = "completed"
    PROCESSING_STATUS_DISCARDED = "discarded"

    MEETING_TYPE_CHOICES = (
        (MEETING, "Meeting"),
        (COMMITTEE_MEETING, "Committee Meeting"),
    )
    PROCESSING_STATUS_CHOICES = (
        (PROCESSING_STATUS_DRAFT, "Draft"),
        (PROCESSING_STATUS_SCHEDULED, "Scheduled"),
        (PROCESSING_STATUS_COMPLETED, "Completed"),
        (PROCESSING_STATUS_DISCARDED, "Discarded"),
    )

    # List of statuses from above that allow a customer to view an application (read-only)
    CUSTOMER_VIEWABLE_STATE = ["completed"]

    meeting_number = models.CharField(max_length=9, blank=True, default="")
    meeting_type = models.CharField(
        "Meeting Type",
        max_length=30,
        choices=MEETING_TYPE_CHOICES,
        default=MEETING_TYPE_CHOICES[0][0],
    )
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    location = models.ForeignKey(
        MeetingRoom,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="meetings",
    )
    title = models.CharField(max_length=128, blank=True, null=True)
    attendees = models.CharField(max_length=1208, blank=True, null=True)
    # if commitee meeting
    committee = models.ForeignKey(
        Committee,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="committee",
    )
    selected_committee_members = models.ManyToManyField(CommitteeMembers, blank=True)
    processing_status = models.CharField(
        "Processing Status",
        max_length=30,
        choices=PROCESSING_STATUS_CHOICES,
        default=PROCESSING_STATUS_CHOICES[0][0],
    )
    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)
    datetime_scheduled = models.DateTimeField(blank=True, null=True)
    datetime_completed = models.DateTimeField(blank=True, null=True)
    submitter = models.IntegerField(null=True)  # EmailUserRO

    class Meta:
        app_label = "boranga"

    def __str__(self):
        return str(self.title)

    def save(self, *args, **kwargs):
        # Prefix "M" char to meeting_number.
        super().save(*args, **kwargs)
        if self.meeting_number == "":
            new_meeting_id = f"M{str(self.pk)}"
            self.meeting_number = new_meeting_id
            self.save()

    @property
    def reference(self):
        return f"{self.meeting_number}-{self.meeting_number}"

    def log_user_action(self, action, request):
        return MeetingUserAction.log_action(self, action, request.user.id)

    @property
    def can_user_edit(self):
        return self.processing_status in [
            Meeting.PROCESSING_STATUS_DRAFT,
            Meeting.PROCESSING_STATUS_SCHEDULED,
        ]

    @property
    def can_user_schedule(self):
        return self.processing_status in [Meeting.PROCESSING_STATUS_DRAFT]

    @property
    def can_user_complete(self):
        return self.processing_status in [Meeting.PROCESSING_STATUS_SCHEDULED]

    @property
    def can_user_reinstate(self):
        return self.processing_status in [Meeting.PROCESSING_STATUS_DISCARDED]

    @property
    def can_user_view(self):
        user_viewable_state = [
            Meeting.PROCESSING_STATUS_COMPLETED,
        ]
        return self.processing_status in user_viewable_state

    def has_user_edit_mode(self, request):
        officer_view_state = [
            Meeting.PROCESSING_STATUS_DRAFT,
            Meeting.PROCESSING_STATUS_COMPLETED,
        ]
        if self.processing_status in officer_view_state:
            return False

        return is_conservation_status_approver(request)

    @transaction.atomic
    def discard(self, request):
        if not self.can_user_edit:
            raise ValidationError("You can't edit this meeting at this moment")

        self.processing_status = self.PROCESSING_STATUS_DISCARDED
        self.save()

        # Create a log entry for the meeting
        self.log_user_action(
            MeetingUserAction.ACTION_DISCARD_MEETING.format(self.meeting_number),
            request,
        )

        # Create a log entry for the submitter
        request.user.log_user_action(
            MeetingUserAction.ACTION_DISCARD_MEETING.format(
                self.meeting_number,
            ),
            request,
        )

    @transaction.atomic
    def reinstate(self, request):
        if not self.processing_status == Meeting.PROCESSING_STATUS_DISCARDED:
            raise ValidationError(
                "You cannot reinstate a meeting that has not been discarded"
            )

        if not request.user.is_superuser and not request.user.id == self.submitter:
            raise ValidationError("You cannot reinstate a meeting that is not yours")

        if not is_conservation_status_approver(request):
            raise ValidationError(
                "You cannot reinstate a meeting unless you are in the "
                "meeting approver group"
            )

        self.processing_status = ConservationStatus.PROCESSING_STATUS_DRAFT
        self.save()

        # Create a log entry for the meeting
        self.log_user_action(
            MeetingUserAction.ACTION_REINSTATE_MEETING.format(self.meeting_number),
            request,
        )

        # Create a log entry for the submitter
        request.user.log_user_action(
            MeetingUserAction.ACTION_REINSTATE_MEETING.format(
                self.meeting_number,
            ),
            request,
        )

    @transaction.atomic
    def schedule_meeting(self, request, viewset):
        if not self.processing_status == self.PROCESSING_STATUS_DRAFT:
            raise ValidationError("You can't schedule this meeting at this moment")

        self.processing_status = self.PROCESSING_STATUS_SCHEDULED
        self.submitter = request.user.id
        self.datetime_scheduled = timezone.now()

        # Create a log entry for the meeting
        self.log_user_action(
            MeetingUserAction.ACTION_SCHEDULE_MEETING.format(self.meeting_number),
            request,
        )

        # Create a log entry for the submitter
        request.user.log_user_action(
            MeetingUserAction.ACTION_SCHEDULE_MEETING.format(
                self.meeting_number,
            ),
            request,
        )

        self.save()

    @transaction.atomic
    def complete_meeting(self, request, viewset):
        if not self.processing_status == self.PROCESSING_STATUS_SCHEDULED:
            raise ValidationError("You can't complete this meeting at this moment")

        self.processing_status = self.PROCESSING_STATUS_COMPLETED
        self.submitter = request.user.id
        self.datetime_completed = timezone.now()

        # Create a log entry for the meeting
        self.log_user_action(
            MeetingUserAction.ACTION_COMPLETE_MEETING.format(self.meeting_number),
            request,
        )

        # Create a log entry for the submitter
        request.user.log_user_action(
            MeetingUserAction.ACTION_COMPLETE_MEETING.format(
                self.meeting_number,
            ),
            request,
        )

        self.save()


class MeetingLogDocument(Document):
    log_entry = models.ForeignKey(
        "MeetingLogEntry", related_name="documents", on_delete=models.CASCADE
    )
    _file = models.FileField(
        upload_to=update_meeting_comms_log_filename,
        max_length=512,
        storage=private_storage,
    )

    class Meta:
        app_label = "boranga"

    def get_parent_instance(self) -> BaseModel:
        return self.log_entry


class MeetingLogEntry(CommunicationsLogEntry):
    meeting = models.ForeignKey(
        Meeting, related_name="comms_logs", on_delete=models.CASCADE
    )

    def __str__(self):
        return f"{self.reference} - {self.subject}"

    class Meta:
        app_label = "boranga"

    def save(self, **kwargs):
        # save the application reference if the reference not provided
        if not self.reference:
            self.reference = self.meeting.reference
        super().save(**kwargs)


class MeetingUserAction(UserAction):
    ACTION_CREATE_MEETING = "Create meeting {}"
    ACTION_SAVE_MEETING = "Save Meeting {}"
    ACTION_SCHEDULE_MEETING = "Schedule Meeting {}"
    ACTION_COMPLETE_MEETING = "Complete Meeting {}"
    ACTION_DISCARD_MEETING = "Discard Meeting {}"
    ACTION_REINSTATE_MEETING = "Reinstate Meeting {}"

    # Minutes Document
    ACTION_ADD_MINUTE = "Minutes {} added for Meeting {}"
    ACTION_UPDATE_MINUTE = "Minutes {} updated for Meeting {}"
    ACTION_DISCARD_MINUTE = "Minutes {} discarded for Meeting {}"
    ACTION_REINSTATE_MINUTE = "Minutes {} reinstated for Meeting {}"

    ACTION_DISCARD_MEETING = "Discard Meeting {}"

    class Meta:
        app_label = "boranga"
        ordering = ("-when",)

    @classmethod
    def log_action(cls, meeting, action, user):
        return cls.objects.create(meeting=meeting, who=user, what=str(action))

    meeting = models.ForeignKey(
        Meeting, related_name="action_logs", on_delete=models.CASCADE
    )


class Minutes(Document):
    """
    Meta-data associated with a document relevant to a Meeting.

    Has a:
    - Meeting
    - DocumentCategory
    - DocumentSubCategoty
    Used for:
    - Meeting
    Is:
    - Table
    """

    minutes_number = models.CharField(max_length=9, blank=True, default="")
    _file = models.FileField(
        upload_to=update_meeting_doc_filename,
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
    meeting = models.ForeignKey(
        Meeting,
        blank=False,
        default=None,
        on_delete=models.CASCADE,
        related_name="meeting_minutes",
    )

    class Meta:
        app_label = "boranga"
        verbose_name = "Meeting Minutes"

    def save(self, *args, **kwargs):
        # Prefix "MN" char to minutes_number.
        if self.minutes_number == "":
            force_insert = kwargs.pop("force_insert", False)
            super().save(no_revision=True, force_insert=force_insert)
            new_minute_id = f"MN{str(self.pk)}"
            self.minutes_number = new_minute_id
            self.save(*args, **kwargs)
        else:
            super().save(*args, **kwargs)

    def get_parent_instance(self) -> BaseModel:
        return self.meeting

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


class AgendaItem(OrderedModel):
    meeting = models.ForeignKey(
        Meeting, on_delete=models.CASCADE, related_name="agenda_items"
    )
    conservation_status = models.ForeignKey(
        ConservationStatus, on_delete=models.CASCADE
    )

    class Meta:
        app_label = "boranga"
        # the verbose name should be meeting as used in the Related items tab of CS
        verbose_name = "Meeting Agenda Item"
        unique_together = ("meeting", "conservation_status")
        ordering = ["meeting", "order"]

    def __str__(self):
        return str(self.meeting)

    @property
    def as_related_item(self):
        related_item = RelatedItem(
            identifier=self.related_item_identifier,
            model_name=self._meta.verbose_name.title(),
            descriptor=self.related_item_descriptor,
            status=self.related_item_status,
            action_url=str(
                f"<a href=/internal/meetings/{self.meeting.id} "
                f'target="_blank">View <i class="bi bi-box-arrow-up-right"></i></a>'
            ),
        )
        return related_item

    @property
    def related_item_identifier(self):
        return self.meeting.meeting_number

    @property
    def related_item_descriptor(self):
        return self.meeting.title

    @property
    def related_item_status(self):
        return self.meeting.get_processing_status_display()


# Minutes
reversion.register(Minutes)
