import logging

from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.db import models
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ledger_api_client.managed_models import SystemGroup, SystemGroupPermission
from ordered_model.models import OrderedModel

from boranga.components.main.models import (
    ArchivableModel,
    BaseModel,
    CommunicationsLogEntry,
    Document,
    OrderedArchivableManager,
    UserAction,
)
from boranga.helpers import no_commas_validator
from boranga.ledger_api_utils import retrieve_email_user

private_storage = FileSystemStorage(location=settings.BASE_DIR + "/private-media/", base_url="/private-media/")


logger = logging.getLogger(__name__)


class SubmitterCategory(OrderedModel, ArchivableModel):
    objects = OrderedArchivableManager()

    name = models.CharField(max_length=100, validators=[no_commas_validator])
    USER_TYPE_CHOICE_EXTERNAL = "external"
    USER_TYPE_CHOICE_INTERNAL = "internal"
    USER_TYPE_CHOICES = [
        (USER_TYPE_CHOICE_EXTERNAL, "External Users"),
        (USER_TYPE_CHOICE_INTERNAL, "Internal Users"),
    ]
    visible_to = models.CharField(
        choices=USER_TYPE_CHOICES,
        max_length=10,
        blank=False,
        null=False,
        default=USER_TYPE_CHOICE_EXTERNAL,
    )

    class Meta(OrderedModel.Meta):
        app_label = "boranga"
        verbose_name_plural = "Submitter Categories"

    def __str__(self) -> str:
        return self.name

    @classmethod
    def get_filter_list(cls):
        return list(cls.objects.all().values("id", "name"))


class SubmitterInformation(BaseModel):
    BULK_IMPORT_ABBREVIATION = "orfsub"

    email_user = models.IntegerField(blank=True, null=True)
    name = models.CharField(max_length=100, blank=True, null=True)
    contact_details = models.TextField(blank=True, null=True)
    organisation = models.CharField(max_length=100, blank=True, null=True)
    position = models.CharField(max_length=100, blank=True, null=True)
    submitter_category = models.ForeignKey(SubmitterCategory, on_delete=models.PROTECT, blank=True, null=True)

    class Meta:
        app_label = "boranga"


class SubmitterInformationModelMixin:
    def save(self, *args, **kwargs):
        if not self.pk or self.submitter_information:
            super().save(*args, **kwargs)
            return

        self.create_submitter_information()
        if not kwargs.get("no_revision", False):
            kwargs["no_revision"] = True

        super().save(*args, **kwargs)

    def create_submitter_information(self):
        if not hasattr(self, "submitter") or not hasattr(self, "submitter_information"):
            raise AttributeError(
                "To use SubmitterInformationModelMixin, the model must have a submitter and submitter_information field"
            )

        if not self.submitter or self.submitter_information:
            return

        emailuser = retrieve_email_user(self.submitter)
        contact_details = ""
        if emailuser.mobile_number:
            contact_details += f"Mobile: {emailuser.mobile_number}\r\n"
        if emailuser.phone_number:
            contact_details += f"Phone: {emailuser.phone_number}\r\n"
        if emailuser.email:
            contact_details += f"Email: {emailuser.email}\r\n"
        submitter_information = SubmitterInformation.objects.create(
            email_user=self.submitter,
            name=emailuser.get_full_name(),
            contact_details=contact_details,
        )
        self.submitter_information = submitter_information


class ExternalContributorBlacklist(BaseModel):
    email = models.EmailField(unique=True)
    reason = models.TextField()
    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "boranga"
        verbose_name_plural = "External Contributor Blacklist"

    def __str__(self):
        return self.email

    def save(self, *args, **kwargs):
        # Remove the user from the external contributors group
        external_contributor_group = SystemGroup.objects.get(name=settings.GROUP_NAME_EXTERNAL_CONTRIBUTOR)

        user = EmailUser.objects.get(email=self.email)

        SystemGroupPermission.objects.filter(system_group=external_contributor_group, emailuser=user).delete()
        # Have to save the group to flush the member cache
        external_contributor_group.save()

        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        try:
            user = EmailUser.objects.get(email=self.email)
        except EmailUser.DoesNotExist:
            logger.warning(
                f"ExternalContributorBlacklist: User with email {self.email} does not exist. "
                "Deleting blacklist entry anyway."
            )
            super().delete(*args, **kwargs)
            return

        # If user is already in the external contributors group, don't add them again
        external_contributor_group = SystemGroup.objects.get(name=settings.GROUP_NAME_EXTERNAL_CONTRIBUTOR)
        if not SystemGroupPermission.objects.filter(system_group=external_contributor_group, emailuser=user).exists():
            # Add user back into the external contributors group
            SystemGroupPermission.objects.create(system_group=external_contributor_group, emailuser=user)

        # Have to save the group to flush the member cache
        external_contributor_group.save()

        super().delete(*args, **kwargs)


class EmailUserLogEntry(CommunicationsLogEntry):
    email_user = models.IntegerField()

    def __str__(self):
        return f"Email User ID: {self.email_user} - {self.subject}"

    class Meta:
        app_label = "boranga"


def email_user_comms_log_document_upload_location(instance, filename):
    return f"{settings.MEDIA_APP_DIR}/email_user/{instance.log_entry.email_user}/communications/{filename}"


class EmailUserLogDocument(Document):
    log_entry = models.ForeignKey(EmailUserLogEntry, related_name="documents", on_delete=models.CASCADE)
    _file = models.FileField(
        upload_to=email_user_comms_log_document_upload_location,
        max_length=512,
        storage=private_storage,
    )

    class Meta:
        app_label = "boranga"

    def get_parent_instance(self) -> BaseModel:
        return self.log_entry


class EmailUserAction(UserAction):
    email_user = models.IntegerField()

    @classmethod
    def log_action(cls, email_user, action, request_user):
        return cls.objects.create(
            email_user=email_user.id,
            who=request_user.id,
            what=str(action),
        )

    class Meta:
        app_label = "boranga"


def log_user_action(self, action, request):
    """As EmailUserRO is outside the boranga app,
    we need to monkey patch it to add the log_user_action method"""
    return EmailUserAction.log_action(self, action, request.user)


setattr(EmailUser, "log_user_action", log_user_action)
