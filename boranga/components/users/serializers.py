import logging

from django.conf import settings
from django.urls import reverse
from ledger_api_client.ledger_models import Address
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ledger_api_client.managed_models import SystemGroup
from rest_framework import serializers

from boranga.components.conservation_status.models import ConservationStatusReferral
from boranga.components.main.models import (
    CommunicationsLogEntry,
    Document,
    UserSystemSettings,
)
from boranga.components.main.serializers import (
    BaseModelSerializer,
    BaseSerializer,
    get_relative_url,
)
from boranga.components.occurrence.models import (
    OccurrenceReport,
    OccurrenceReportReferral,
)
from boranga.components.users.models import (
    EmailUserAction,
    EmailUserLogEntry,
    SubmitterCategory,
    SubmitterInformation,
)
from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_contributor,
    is_internal,
    is_occurrence_approver,
    is_occurrence_assessor,
)

logger = logging.getLogger(__name__)


class DocumentSerializer(BaseModelSerializer):

    class Meta:
        model = Document
        fields = ("id", "description", "file", "name", "uploaded_date")


class UserAddressSerializer(BaseModelSerializer):
    class Meta:
        model = Address
        fields = ("id", "line1", "locality", "state", "country", "postcode")


class UserSystemSettingsSerializer(BaseModelSerializer):
    class Meta:
        model = UserSystemSettings
        fields = ["area_of_interest"]


class UserFilterSerializer(BaseModelSerializer):
    name = serializers.SerializerMethodField()

    class Meta:
        model = EmailUser
        fields = ("id", "last_name", "first_name", "email", "name")

    def get_name(self, obj):
        return obj.get_full_name()


class UserSerializer(BaseModelSerializer):
    residential_address = UserAddressSerializer()
    personal_details = serializers.SerializerMethodField()
    address_details = serializers.SerializerMethodField()
    contact_details = serializers.SerializerMethodField()
    full_name = serializers.SerializerMethodField()
    area_of_interest = serializers.SerializerMethodField()
    groups = serializers.SerializerMethodField()

    cs_referral_count = serializers.SerializerMethodField()
    ocr_referral_count = serializers.SerializerMethodField()

    is_internal = serializers.SerializerMethodField()
    is_superuser = serializers.SerializerMethodField()
    last_login = serializers.DateTimeField(read_only=True)

    class Meta:
        model = EmailUser
        fields = (
            "id",
            "last_name",
            "first_name",
            "email",
            "residential_address",
            "phone_number",
            "mobile_number",
            "personal_details",
            "address_details",
            "contact_details",
            "full_name",
            "area_of_interest",
            "groups",
            "is_internal",
            "is_superuser",
            "last_login",
            "cs_referral_count",
            "ocr_referral_count",
        )

    def get_personal_details(self, obj):
        return True if obj.last_name and obj.first_name else False

    def get_address_details(self, obj):
        return True if obj.residential_address else False

    def get_contact_details(self, obj):
        if obj.mobile_number and obj.email:
            return True
        elif obj.phone_number and obj.email:
            return True
        elif obj.mobile_number and obj.phone_number:
            return True
        else:
            return False

    def get_full_name(self, obj):
        return obj.get_full_name()

    def get_area_of_interest(self, obj):
        user_system_settings, created = UserSystemSettings.objects.get_or_create(
            user=obj.id
        )
        if created:
            logger.info(f"Created UserSystemSettings: {user_system_settings}")
        group_type_name = (
            user_system_settings.area_of_interest.name
            if user_system_settings.area_of_interest
            else None
        )
        return group_type_name if group_type_name else None

    def get_groups(self, obj):
        groups = SystemGroup.objects.all().values_list("name", flat=True)
        request = self.context["request"] if self.context else None
        if request.user.is_superuser:
            return groups
        return groups.filter(
            systemgrouppermission__emailuser=request.user.id
        ).values_list("name", flat=True)

    def get_is_internal(self, obj):
        request = self.context["request"]
        return is_internal(request)

    def get_is_superuser(self, obj):
        return obj.is_superuser

    def get_cs_referral_count(self, obj):
        return ConservationStatusReferral.objects.filter(referral=obj.id).count()

    def get_ocr_referral_count(self, obj):
        return OccurrenceReportReferral.objects.filter(referral=obj.id).count()


class PersonalSerializer(BaseModelSerializer):
    class Meta:
        model = EmailUser
        fields = (
            "id",
            "last_name",
            "first_name",
        )


class ContactSerializer(BaseModelSerializer):
    class Meta:
        model = EmailUser
        fields = (
            "id",
            "email",
            "phone_number",
            "mobile_number",
        )

    def validate(self, obj):
        # Mobile and phone number for dbca user are updated from active directory
        # so need to skip these users from validation.
        domain = None
        if obj["email"]:
            domain = obj["email"].split("@")[1]
        if domain in settings.DEPT_DOMAINS:
            return obj
        else:
            if not obj.get("phone_number") and not obj.get("mobile_number"):
                raise serializers.ValidationError(
                    "You must provide a mobile/phone number"
                )
        return obj


class EmailUserActionSerializer(BaseModelSerializer):
    who = serializers.CharField(source="who.get_full_name")

    class Meta:
        model = EmailUserAction
        fields = "__all__"


class EmailUserCommsSerializer(BaseModelSerializer):
    class Meta:
        model = EmailUserLogEntry
        fields = "__all__"


class CommunicationLogEntrySerializer(BaseModelSerializer):
    customer = serializers.PrimaryKeyRelatedField(
        queryset=EmailUser.objects.all(), required=False
    )
    documents = serializers.SerializerMethodField()

    class Meta:
        model = CommunicationsLogEntry
        fields = (
            "id",
            "customer",
            "to",
            "fromm",
            "cc",
            "log_type",
            "reference",
            "subject" "text",
            "created",
            "staff",
            "emailuser",
            "documents",
        )

    def get_documents(self, obj):
        return [[d.name, get_relative_url(d._file.url)] for d in obj.documents.all()]


class EmailUserLogEntrySerializer(CommunicationLogEntrySerializer):
    documents = serializers.SerializerMethodField()

    class Meta:
        model = EmailUserLogEntry
        fields = "__all__"
        read_only_fields = ("customer",)

    def get_documents(self, obj):
        return [[d.name, get_relative_url(d._file.url)] for d in obj.documents.all()]


class SubmitterCategorySerializer(BaseModelSerializer):
    class Meta:
        model = SubmitterCategory
        fields = ("id", "name")


class SubmitterInformationSerializer(BaseModelSerializer):
    submitter_category_name = serializers.CharField(
        source="submitter_category.name", read_only=True, allow_null=True
    )

    class Meta:
        model = SubmitterInformation
        fields = "__all__"

    def to_representation(self, instance):
        # Submitter information is shown to users that are referees but they should
        # not see the contact details of the submitter
        ret = super().to_representation(instance)
        request = self.context.get("request")
        if instance.email_user == request.user.id and is_contributor(request):
            return ret

        if (
            hasattr(instance, "occurrence_report")
            and (
                not is_occurrence_assessor(request)
                and not is_occurrence_approver(request)
            )
            or hasattr(instance, "conservation_status")
            and (
                not is_conservation_status_assessor(request)
                and not is_conservation_status_approver(request)
            )
        ):
            ret.pop("contact_details")

        return ret


class OutstandingReferralSerializer(BaseSerializer):
    id = serializers.IntegerField()
    parent_id = serializers.IntegerField()
    number = serializers.CharField()
    group_type = serializers.CharField()
    type = serializers.SerializerMethodField()
    name = serializers.CharField()
    lodged_on = serializers.DateTimeField()
    processing_status = serializers.SerializerMethodField()
    link = serializers.SerializerMethodField()

    class Meta:
        fields = (
            "id",
            "parent_id",
            "number",
            "group_type",
            "type",
            "processing_status",
        )

    def get_processing_status(self, obj):
        if "CS" in obj["number"]:
            for choice in ConservationStatusReferral.PROCESSING_STATUS_CHOICES:
                if choice[0] == obj["processing_status"]:
                    return choice[1]
        elif OccurrenceReport.MODEL_PREFIX in obj["number"]:
            for choice in OccurrenceReportReferral.PROCESSING_STATUS_CHOICES:
                if choice[0] == obj["processing_status"]:
                    return choice[1]

        raise serializers.ValidationError(
            "Processing status not found for this referral"
        )

    def get_type(self, obj):
        group_type = obj["group_type"]
        if "CS" in obj["number"]:
            return f"{group_type} Conservation Status".title()
        else:
            return f"{group_type} Occurrence Report".title()

    def get_link(self, obj):
        link = None
        if is_internal(self.context["request"]):
            if "CS" in obj["number"]:
                link = reverse(
                    "internal-conservation-status-referral-detail",
                    kwargs={
                        "cs_proposal_pk": obj["parent_id"],
                        "referral_pk": obj["id"],
                    },
                )
            elif OccurrenceReport.MODEL_PREFIX in obj["number"]:
                link = reverse(
                    "internal-occurrence-report-referral-detail",
                    kwargs={
                        "occurrence_report_pk": obj["parent_id"],
                        "referral_pk": obj["id"],
                    },
                )
        else:
            if "CS" in obj["number"]:
                link = reverse(
                    "external-conservation-status-referral-detail",
                    kwargs={
                        "cs_proposal_pk": obj["parent_id"],
                        "referral_pk": obj["id"],
                    },
                )
            elif OccurrenceReport.MODEL_PREFIX in obj["number"]:
                link = reverse(
                    "external-occurrence-report-referral-detail",
                    kwargs={
                        "occurrence_report_pk": obj["parent_id"],
                        "referral_pk": obj["id"],
                    },
                )

        if link:
            return link

        logger.warning(
            f"Link not found for referral {obj['number']} with id {obj['id']}"
        )

        return None
