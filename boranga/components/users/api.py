import logging

from django.db import transaction
from django.db.models import CharField, F, Value
from django.db.models.functions import Coalesce, Concat
from django.shortcuts import get_object_or_404
from django_countries import countries
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ledger_api_client.managed_models import SystemGroupPermission
from rest_framework import mixins, views, viewsets
from rest_framework.decorators import action as detail_route
from rest_framework.decorators import action as list_route
from rest_framework.decorators import renderer_classes
from rest_framework.exceptions import PermissionDenied
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from boranga.components.conservation_status.models import ConservationStatusReferral
from boranga.components.main.models import UserSystemSettings
from boranga.components.main.permissions import CommsLogPermission
from boranga.components.occurrence.models import OccurrenceReportReferral
from boranga.components.species_and_communities.models import GroupType
from boranga.components.users.models import SubmitterCategory, SubmitterInformation
from boranga.components.users.serializers import (
    EmailUserActionSerializer,
    EmailUserCommsSerializer,
    EmailUserLogEntrySerializer,
    OutstandingReferralSerializer,
    SubmitterCategorySerializer,
    SubmitterInformationSerializer,
    UserSerializer,
)
from boranga.helpers import is_internal, is_internal_contributor
from boranga.permissions import IsApprover, IsAssessor

logger = logging.getLogger(__name__)


class GetCountries(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        country_list = []
        for country in list(countries):
            country_list.append({"name": country.name, "code": country.code})
        return Response(country_list)


class GetProfile(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [AllowAny]

    def get(self, request, format=None):
        if not request.user.is_authenticated:
            return Response(
                {
                    "authenticated": False,
                    "user": None,
                }
            )
        serializer = UserSerializer(request.user, context={"request": request})
        return Response({"authenticated": True, "user": serializer.data})


class GetSubmitterCategories(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        qs = SubmitterCategory.objects.active()
        if not request.user.is_superuser:
            if is_internal(request) or is_internal_contributor(request):
                qs = qs.filter(visible_to=SubmitterCategory.USER_TYPE_CHOICE_INTERNAL)
            else:
                qs = qs.filter(visible_to=SubmitterCategory.USER_TYPE_CHOICE_EXTERNAL)
        serializer = SubmitterCategorySerializer(qs, many=True)
        return Response(serializer.data)


class SaveSubmitterInformation(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def put(self, request, format=None):
        instance = get_object_or_404(SubmitterInformation, pk=request.data["id"])
        if not instance.email_user == request.user.id:
            raise PermissionDenied("You do not have permission to perform this action.")

        serializer = SubmitterInformationSerializer(
            instance=instance, data=request.data, context={"request": request}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data)


class SaveAreaOfInterest(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def patch(self, request, format=None):
        user_system_settings, created = UserSystemSettings.objects.get_or_create(
            user=request.user.id
        )
        if created:
            logger.info(f"Created UserSystemSettings: {user_system_settings}")

        area_of_interest = request.data.get("area_of_interest", None)

        if not area_of_interest:
            user_system_settings.area_of_interest = None
            user_system_settings.save()
            return Response({"area_of_interest": None})

        try:
            group_type = GroupType.objects.get(name=area_of_interest)
            user_system_settings.area_of_interest_id = group_type.id
        except GroupType.DoesNotExist:
            return Response(
                {"error": "GroupType does not exist"},
                status=400,
            )

        user_system_settings.save()
        return Response({"area_of_interest": group_type.name})


class UserViewSet(viewsets.GenericViewSet, mixins.RetrieveModelMixin):
    queryset = EmailUser.objects.all()
    serializer_class = UserSerializer
    permission_classes = [IsAssessor | IsApprover]

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_users(self, request, *args, **kwargs):
        search_term = request.GET.get("term", "")

        # Allow for search of first name, last name and concatenation of both
        users = EmailUser.objects.annotate(
            search_term=Concat(
                "first_name",
                Value(" "),
                "last_name",
                Value(" "),
                "email",
                output_field=CharField(),
            )
        )
        if kwargs.get("is_staff", False):
            users = users.filter(is_staff=True)

        id_field = "email"
        if kwargs.get("id_field", False):
            id_field = kwargs.get("id_field")

        users = users.filter(search_term__icontains=search_term).values(
            "id", "email", "first_name", "last_name"
        )[:10]

        data_transform = [
            {
                "id": person[id_field],
                "text": f"{person['first_name']} {person['last_name']} ({person['email']})",
            }
            for person in users
        ]
        return Response({"results": data_transform})

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_users_ledger_id(self, request, *args, **kwargs):
        return self.get_users(request, id_field="id")

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_department_users(self, request):
        return self.get_users(request, is_staff=True)

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_department_users_ledger_id(self, request, *args, **kwargs):
        return self.get_users(request, is_staff=True, id_field="id")

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_referees(self, request, *args, **kwargs):
        search_term = request.GET.get("term", "")

        if not search_term:
            return Response({"results": []})

        # Allow for search of first name, last name and concatenation of both
        department_users = EmailUser.objects.annotate(
            search_term=Concat(
                "first_name",
                Value(" "),
                "last_name",
                Value(" "),
                "email",
                output_field=CharField(),
            )
        ).filter(is_staff=True)

        department_users = department_users.filter(
            search_term__icontains=search_term
        ).values("id", "email", "first_name", "last_name")[:10]
        external_cs_referrals = ConservationStatusReferral.objects.filter(
            is_external=True
        ).values_list("referral", flat=True)
        external_ocr_referrals = OccurrenceReportReferral.objects.filter(
            is_external=True
        ).values_list("referral", flat=True)
        external_referee_ids = list(
            set(list(external_cs_referrals) + list(external_ocr_referrals))
        )

        external_referees = EmailUser.objects.filter(
            id__in=external_referee_ids
        ).annotate(
            search_term=Concat(
                "first_name",
                Value(" "),
                "last_name",
                Value(" "),
                "email",
                output_field=CharField(),
            )
        )
        external_referees = external_referees.filter(
            search_term__icontains=search_term
        ).values("id", "email", "first_name", "last_name")[:10]

        internal = {
            "text": "Internal",
            "children": [
                {
                    "id": person["email"],
                    "text": f"{person['first_name']} {person['last_name']} ({person['email']})",
                }
                for person in department_users
            ],
        }
        external = {
            "text": "External ",
            "children": [
                {
                    "id": person["email"],
                    "text": f"{person['first_name']} {person['last_name']} ({person['email']})",
                }
                for person in external_referees
            ],
        }

        data_transform = []
        if department_users.exists():
            data_transform.append(internal)
        if external_referees.exists():
            data_transform.append(external)

        return Response({"results": data_transform})

    @detail_route(
        methods=[
            "GET",
        ],
        detail=True,
    )
    def action_log(self, request, *args, **kwargs):
        instance = self.get_object()
        qs = instance.action_logs.all()
        serializer = EmailUserActionSerializer(qs, many=True)
        return Response(serializer.data)

    @detail_route(
        methods=[
            "GET",
        ],
        detail=True,
    )
    def comms_log(self, request, *args, **kwargs):
        instance = self.get_object()
        qs = instance.comms_logs.all()
        serializer = EmailUserCommsSerializer(qs, many=True)
        return Response(serializer.data)

    @detail_route(
        methods=[
            "POST",
        ],
        detail=True,
        permission_classes=[CommsLogPermission],
    )
    @renderer_classes((JSONRenderer,))
    @transaction.atomic
    def add_comms_log(self, request, *args, **kwargs):
        instance = self.get_object()
        mutable = request.data._mutable
        request.data._mutable = True
        request.data["emailuser"] = f"{instance.id}"
        request.data["staff"] = f"{request.user.id}"
        request.data._mutable = mutable
        serializer = EmailUserLogEntrySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        comms = serializer.save()

        # Save the files
        for f in request.FILES:
            document = comms.documents.create()
            document.check_file(request.FILES[f])
            document.name = str(request.FILES[f])
            document._file = request.FILES[f]
            document.save()

        return Response(serializer.data)

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_contributors(self, request, *args, **kwargs):
        search_term = request.GET.get("term", "")
        contributor_ids = list(
            SystemGroupPermission.objects.filter(
                system_group__name__icontains="contributor"
            ).values_list("emailuser", flat=True)
        )

        # Allow for search of first name, last name and concatenation of both
        users = EmailUser.objects.filter(id__in=contributor_ids).annotate(
            search_term=Concat(
                "first_name",
                Value(" "),
                "last_name",
                Value(" "),
                "email",
                output_field=CharField(),
            )
        )

        users = users.filter(search_term__icontains=search_term).values(
            "id", "email", "first_name", "last_name"
        )[:10]

        data_transform = [
            {
                "id": person["id"],
                "text": f"{person['first_name']} {person['last_name']} ({person['email']})",
            }
            for person in users
        ]
        return Response({"results": data_transform})


class OutstandingReferrals(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        outstanding_referrals_qs = (
            ConservationStatusReferral.objects.filter(
                referral=self.request.user.id,
                processing_status=ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL,
            )
            .annotate(
                number=F("conservation_status__conservation_status_number"),
                group_type=F("conservation_status__application_type__name"),
                parent_id=F("conservation_status__id"),
                name=Coalesce(
                    "conservation_status__species__taxonomy__scientific_name",
                    "conservation_status__community__taxonomy__community_name",
                    output_field=CharField(),
                ),
            )
            .values(
                "id",
                "parent_id",
                "number",
                "group_type",
                "name",
                "processing_status",
                "lodged_on",
            )
        )
        ocr_referrals_qs = (
            OccurrenceReportReferral.objects.filter(
                referral=self.request.user.id,
                processing_status=OccurrenceReportReferral.PROCESSING_STATUS_WITH_REFERRAL,
            )
            .annotate(
                name=Coalesce(
                    "occurrence_report__species__taxonomy__scientific_name",
                    "occurrence_report__community__taxonomy__community_name",
                    output_field=CharField(),
                ),
                parent_id=F("occurrence_report__id"),
            )
            .values(
                "id",
                "parent_id",
                "occurrence_report__occurrence_report_number",
                "occurrence_report__group_type__name",
                "name",
                "processing_status",
                "lodged_on",
            )
        )
        outstanding_referrals_qs = outstanding_referrals_qs.union(ocr_referrals_qs)
        ordered_outstanding_referrals_qs = outstanding_referrals_qs.order_by(
            "lodged_on"
        )

        serializer = OutstandingReferralSerializer(
            ordered_outstanding_referrals_qs,
            many=True,
            context={"request": request},
        )
        return Response(serializer.data)
