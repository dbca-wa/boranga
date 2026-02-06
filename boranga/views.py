import logging
import mimetypes
import os

from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.management import call_command
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.views.generic import DetailView
from django.views.generic.base import TemplateView

from boranga.components.conservation_status.mixins import ReferralOwnerMixin
from boranga.components.conservation_status.models import (
    ConservationStatus,
    ConservationStatusAmendmentRequestDocument,
    ConservationStatusDocument,
    ConservationStatusReferral,
)
from boranga.components.meetings.models import Meeting
from boranga.components.occurrence.models import (
    Occurrence,
    OccurrenceReport,
    OccurrenceReportAmendmentRequestDocument,
    OccurrenceReportDocument,
)
from boranga.components.species_and_communities.models import Community, Species
from boranga.forms import LoginForm
from boranga.helpers import (
    is_conservation_status_referee,
    is_contributor,
    is_django_admin,
    is_internal,
    is_occurrence_report_referee,
)

logger = logging.getLogger(__name__)


class InternalView(UserPassesTestMixin, TemplateView):
    template_name = "boranga/dash/index.html"

    def test_func(self):
        return is_internal(self.request)


class PublicView(TemplateView):
    template_name = "boranga/dash/index.html"


class ExternalView(LoginRequiredMixin, TemplateView):
    template_name = "boranga/dash/index.html"


class SpeciesView(TemplateView):
    template_name = "boranga/dash/index.html"


class InternalSpeciesView(DetailView):
    model = Species
    template_name = "boranga/dash/index.html"

    def get(self, *args, **kwargs):
        if self.request.user.is_authenticated():
            if is_internal(self.request):
                return super().get(*args, **kwargs)
        kwargs["form"] = LoginForm
        return super(BorangaRoutingView, self).get(*args, **kwargs)


class InternalCommunityView(DetailView):
    model = Community
    template_name = "boranga/dash/index.html"

    def get(self, *args, **kwargs):
        if self.request.user.is_authenticated():
            if is_internal(self.request):
                return super().get(*args, **kwargs)
        kwargs["form"] = LoginForm
        return super(BorangaRoutingView, self).get(*args, **kwargs)


class ExternalConservationStatusView(DetailView):
    model = ConservationStatus
    template_name = "boranga/dash/index.html"


class InternalConservationStatusView(DetailView):
    model = ConservationStatus
    template_name = "boranga/dash/index.html"

    def get(self, *args, **kwargs):
        if self.request.user.is_authenticated():
            if is_internal(self.request):
                return super().get(*args, **kwargs)
            return redirect("external-conservation-status-detail")
        kwargs["form"] = LoginForm
        return super(BorangaRoutingView, self).get(*args, **kwargs)


class InternalConservationStatusDashboardView(DetailView):
    model = ConservationStatus
    template_name = "boranga/dash/index.html"


class ConservationStatusReferralView(ReferralOwnerMixin, DetailView):
    model = ConservationStatusReferral
    template_name = "boranga/dash/index.html"


class InternalMeetingDashboardView(DetailView):
    model = Meeting
    template_name = "boranga/dash/index.html"


class ExternalOccurrenceReportView(DetailView):
    model = OccurrenceReport
    template_name = "boranga/dash/index.html"


class InternalOccurrenceView(DetailView):
    model = Occurrence
    template_name = "boranga/dash/index.html"


class InternalOccurrenceReportView(DetailView):
    model = OccurrenceReport
    template_name = "boranga/dash/index.html"

    def get(self, *args, **kwargs):
        if self.request.user.is_authenticated():
            if is_internal(self.request):
                return super().get(*args, **kwargs)
            return redirect("external-occurrence-report-detail")
        kwargs["form"] = LoginForm
        return super(BorangaRoutingView, self).get(*args, **kwargs)


class InternalOccurrenceReportReferralView(TemplateView):
    template_name = "boranga/dash/index.html"


class BorangaRoutingView(TemplateView):
    template_name = "boranga/index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["settings"] = settings
        return context


class BorangaContactView(TemplateView):
    template_name = "boranga/contact.html"


class BorangaFurtherInformationView(TemplateView):
    template_name = "boranga/further_info.html"


class ManagementCommandsView(LoginRequiredMixin, UserPassesTestMixin, TemplateView):
    template_name = "boranga/mgt-commands.html"

    def test_func(self):
        return self.request.user.is_superuser or (is_internal(self.request) and is_django_admin(self.request))

    def post(self, request):
        data = {}
        command_script = request.POST.get("script", None)
        if command_script:
            call_command(command_script)
            data.update({command_script: "true"})

        return render(request, self.template_name, data)


def is_authorised_to_access_community_document(request, document_id):
    if is_internal(request):
        # check auth
        return request.user.is_superuser or is_internal(request)
    else:
        return False


def is_authorised_to_access_species_document(request, document_id):
    if is_internal(request):
        # check auth
        return request.user.is_superuser or is_internal(request)
    else:
        return False


def is_authorised_to_access_meeting_document(request, document_id):
    if is_internal(request):
        # check auth
        return request.user.is_superuser or is_internal(request)
    else:
        return False


def check_allowed_path(document_id, path, allowed_paths):
    try:
        file_name_path_split = path.split("/")
        id_index = file_name_path_split.index(str(document_id))
        # take all after the id_index, except the last (the file name) - join and check if in allowed_paths
        check_str = "/".join(file_name_path_split[id_index + 1 : -1])
        return check_str in allowed_paths
    except Exception as e:
        logger.exception(f"Error checking allowed path: {e}")
        return False


def is_authorised_to_access_occurrence_report_document(request, document_id):
    referee_allowed_paths = ["documents"]
    contributor_allowed_paths = ["documents", "amendment_request_documents"]

    if is_internal(request):
        # check auth
        return request.user.is_superuser or is_internal(request)

    if is_occurrence_report_referee(request) and is_contributor(request):
        file_name = get_file_name_from_path(request.path)
        qs = OccurrenceReportDocument.objects.filter(
            active=True,
            occurrence_report_id=document_id,
            _file=file_name,
        )

        return (
            qs.filter(occurrence_report__referrals__referral=request.user.id).exists()
            and check_allowed_path(document_id, request.path, referee_allowed_paths)
            or qs.filter(
                can_submitter_access=True,
                occurrence_report__submitter=request.user.id,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
            or OccurrenceReportAmendmentRequestDocument.objects.filter(
                active=True,
                occurrence_report_amendment_request__occurrence_report_id=document_id,
                _file=file_name,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
        )

    if is_occurrence_report_referee(request):
        file_name = get_file_name_from_path(request.path)
        return OccurrenceReportDocument.objects.filter(
            active=True,
            occurrence_report__referrals__referral=request.user.id,
            occurrence_report_id=document_id,
            _file=file_name,
        ).exists() and check_allowed_path(document_id, request.path, referee_allowed_paths)

    if is_contributor(request):
        file_name = get_file_name_from_path(request.path)
        return (
            OccurrenceReportDocument.objects.filter(
                active=True,
                can_submitter_access=True,
                occurrence_report__submitter=request.user.id,
                occurrence_report_id=document_id,
                _file=file_name,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
            or OccurrenceReportAmendmentRequestDocument.objects.filter(
                active=True,
                occurrence_report_amendment_request__occurrence_report_id=document_id,
                _file=file_name,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
        )

    return False


def is_authorised_to_access_occurrence_document(request, document_id):
    if is_internal(request):
        # check auth
        return request.user.is_superuser or is_internal(request)
    else:
        return False


def is_authorised_to_access_conservation_status_document(request, document_id):
    referee_allowed_paths = ["documents"]
    contributor_allowed_paths = ["documents", "amendment_request_documents"]

    if is_internal(request):
        # check auth
        return request.user.is_superuser or is_internal(request)

    if is_conservation_status_referee(request) and is_contributor(request):
        file_name = get_file_name_from_path(request.path)
        qs = ConservationStatusDocument.objects.filter(
            active=True,
            conservation_status_id=document_id,
            _file=file_name,
        )

        return (
            qs.filter(
                conservation_status__referrals__referral=request.user.id,
            )
            and check_allowed_path(document_id, request.path, referee_allowed_paths)
            or qs.filter(
                can_submitter_access=True,
                conservation_status__submitter=request.user.id,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
            or ConservationStatusAmendmentRequestDocument.objects.filter(
                active=True,
                conservation_status_amendment_request__conservation_status_id=document_id,
                _file=file_name,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
        )

    if is_conservation_status_referee(request):
        file_name = get_file_name_from_path(request.path)
        return ConservationStatusDocument.objects.filter(
            active=True,
            conservation_status__referrals__referral=request.user.id,
            conservation_status_id=document_id,
            _file=file_name,
        ).exists() and check_allowed_path(document_id, request.path, referee_allowed_paths)

    if is_contributor(request):
        contributor_allowed_paths = ["documents", "amendment_request_documents"]
        file_name = get_file_name_from_path(request.path)
        return (
            ConservationStatusDocument.objects.filter(
                active=True,
                can_submitter_access=True,
                conservation_status__submitter=request.user.id,
                conservation_status_id=document_id,
                _file=file_name,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
            or ConservationStatusAmendmentRequestDocument.objects.filter(
                active=True,
                conservation_status_amendment_request__conservation_status_id=document_id,
                _file=file_name,
            ).exists()
            and check_allowed_path(document_id, request.path, contributor_allowed_paths)
        )

    return False


def get_file_path_id(check_str, file_path):
    file_name_path_split = file_path.split("/")
    # if the check_str is in the file path, the next value should be the id
    if check_str in file_name_path_split:
        id_index = file_name_path_split.index(check_str) + 1
        if len(file_name_path_split) > id_index and file_name_path_split[id_index].isnumeric():
            return int(file_name_path_split[id_index])
        else:
            return False
    else:
        return False


def get_file_name_from_path(file_path):
    file_name_path_split = file_path.split("/private-media/")
    return file_name_path_split[-1]


def is_authorised_to_access_document(request):
    # occurrence reports
    document_or_id = get_file_path_id("occurrence_report", request.path)
    if document_or_id:
        return is_authorised_to_access_occurrence_report_document(request, document_or_id)

    # occurrence
    document_o_id = get_file_path_id("occurrence", request.path)
    if document_o_id:
        return is_authorised_to_access_occurrence_document(request, document_o_id)

    # conservation status
    document_cs_id = get_file_path_id("conservation_status", request.path)
    if document_cs_id:
        return is_authorised_to_access_conservation_status_document(request, document_cs_id)

    # meeting
    document_m_id = get_file_path_id("meeting", request.path)
    if document_m_id:
        return is_authorised_to_access_meeting_document(request, document_m_id)

    # species
    document_s_id = get_file_path_id("species", request.path)
    if document_s_id:
        return is_authorised_to_access_species_document(request, document_s_id)

    # community
    document_c_id = get_file_path_id("community", request.path)
    if document_c_id:
        return is_authorised_to_access_community_document(request, document_c_id)

    return False


def getPrivateFile(request):
    file_name_path = request.path
    # norm path will convert any traversal or repeat / in to its normalised form
    full_file_path = os.path.normpath(settings.BASE_DIR + file_name_path)

    if not full_file_path.startswith(settings.BASE_DIR):
        return HttpResponse("Unauthorized", status=401)

    if not os.path.isfile(full_file_path):
        return HttpResponse("Not Found", status=404)

    if is_authorised_to_access_document(request):
        # we then ensure the normalised path is within the BASE_DIR (and the file exists)
        extension = file_name_path.split(".")[-1].lower()
        the_file = open(full_file_path, "rb")
        the_data = the_file.read()
        the_file.close()

        content_type = None
        if extension in ["msg", "eml"]:
            content_type = "application/vnd.ms-outlook"
        else:
            try:
                content_type = mimetypes.types_map["." + str(extension)]
            except KeyError:
                raise ValueError(f"Extension {extension} not found in mimetypes.types_map")

        response = HttpResponse(the_data, content_type=content_type)

        if "image/" in content_type or "application/pdf" == content_type:
            return response

        response["Content-Disposition"] = f'attachment; filename="{os.path.basename(full_file_path)}"'
        response["Content-Length"] = os.path.getsize(full_file_path)
        response["X-Sendfile"] = full_file_path
        return response

    return HttpResponse("Unauthorized", status=401)
