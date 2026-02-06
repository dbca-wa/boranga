import logging
from datetime import datetime, time

from rest_framework_datatables.filters import DatatablesFilterBackend

logger = logging.getLogger(__name__)


class OccurrenceReportReferralFilterBackend(DatatablesFilterBackend):
    def filter_queryset(self, request, queryset, view):
        total_count = queryset.count()

        filter_group_type = request.GET.get("filter_group_type")
        if filter_group_type and not filter_group_type.lower() == "all":
            queryset = queryset.filter(occurrence_report__group_type__name=filter_group_type)

        filter_occurrence = request.GET.get("filter_occurrence")
        if filter_occurrence and not filter_occurrence.lower() == "all":
            queryset = queryset.filter(occurrence_report__occurrence_id=filter_occurrence)

        if filter_group_type == "community":
            filter_community_name = request.GET.get("filter_community_name")
            if filter_community_name and not filter_community_name.lower() == "all":
                queryset = queryset.filter(occurrence_report__community__taxonomy__id=filter_community_name)
        else:
            filter_scientific_name = request.GET.get("filter_scientific_name")
            if filter_scientific_name and not filter_scientific_name.lower() == "all":
                queryset = queryset.filter(occurrence_report__species__taxonomy__id=filter_scientific_name)

        filter_status = request.GET.get("filter_status")
        if filter_status and not filter_status.lower() == "all":
            queryset = queryset.filter(processing_status=filter_status)

        def get_date(filter_date):
            date = request.GET.get(filter_date)
            if date:
                date = datetime.strptime(date, "%Y-%m-%d")
            return date

        filter_submitted_from_date = get_date("filter_submitted_from_date")
        filter_submitted_to_date = get_date("filter_submitted_to_date")
        if filter_submitted_to_date:
            filter_submitted_to_date = datetime.combine(filter_submitted_to_date, time.max)

        if filter_submitted_from_date and not filter_submitted_to_date:
            queryset = queryset.filter(occurrence_report__reported_date__gte=filter_submitted_from_date)

        if filter_submitted_from_date and filter_submitted_to_date:
            queryset = queryset.filter(
                reported_date__range=[
                    filter_submitted_from_date,
                    filter_submitted_to_date,
                ]
            )

        if filter_submitted_to_date and not filter_submitted_from_date:
            queryset = queryset.filter(occurrence_report__reported_date__lte=filter_submitted_to_date)

        filter_from_review_due_date = request.GET.get("filter_from_review_due_date")
        filter_to_review_due_date = request.GET.get("filter_to_review_due_date")

        if filter_from_review_due_date:
            queryset = queryset.filter(occurrence_report__review_due_date__gte=filter_from_review_due_date)
        if filter_to_review_due_date:
            queryset = queryset.filter(occurrence_report__review_due_date__lte=filter_to_review_due_date)

        fields = self.get_fields(request)
        ordering = self.get_ordering(request, view, fields)
        queryset = queryset.order_by(*ordering)
        if len(ordering):
            queryset = queryset.order_by(*ordering)

        queryset = super().filter_queryset(request, queryset, view)

        setattr(view, "_datatables_total_count", total_count)
        return queryset
