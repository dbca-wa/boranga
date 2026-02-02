import logging

from django.conf import settings
from ledger_api_client.managed_models import SystemGroup

from boranga.components.conservation_status.models import (
    CommonwealthConservationList,
    ConservationChangeCode,
    ConservationStatus,
    WALegislativeCategory,
    WALegislativeList,
    WAPriorityCategory,
    WAPriorityList,
)
from boranga.components.occurrence.models import OccurrenceReport

logger = logging.getLogger(__name__)


class DefaultDataManager:
    def __init__(self):
        # Make sure all conservation status records have a submitter information
        for conservation_status in ConservationStatus.objects.filter(submitter_information__isnull=True):
            conservation_status.save(no_revision=True)
        # Make sure all occurrence report records have a submitter information
        for occurrence_report in OccurrenceReport.objects.filter(submitter_information__isnull=True):
            occurrence_report.save(no_revision=True)

        for group_name in settings.GROUP_NAME_CHOICES:
            try:
                group, created = SystemGroup.objects.get_or_create(name=group_name)
                if created:
                    logger.info(f"Created SystemGroup: {group}")
            except Exception as e:
                logger.error(f"{e}, SystemGroup: {group_name}")

        # Delete any groups that are not in the settings.GROUP_NAME_CHOICES
        for sg in SystemGroup.objects.exclude(name__in=[group_name for group_name in settings.GROUP_NAME_CHOICES]):
            sg.systemgrouppermission_set.all().delete()
            sg.delete()

        for conservation_change_code in settings.CONSERVATION_CHANGE_CODES:
            try:
                conservation_change_code_obj, created = ConservationChangeCode.objects.get_or_create(
                    code=conservation_change_code["code"],
                    defaults={"label": conservation_change_code["label"]},
                )
                if created:
                    logger.info(f"Created ConservationChangeCode: {conservation_change_code_obj}")
            except Exception as e:
                logger.error(f"{e}, ConservationChangeCode: {conservation_change_code}")

        for ll in settings.WA_LEGISLATIVE_LISTS:
            try:
                legislative_list_obj, created = WALegislativeList.objects.get_or_create(
                    code=ll["code"],
                    defaults={"label": ll["label"]},
                )
                if created:
                    logger.info(f"Created LegislativeList: {legislative_list_obj}")
            except Exception as e:
                logger.error(f"{e}, LegislativeList: {ll}")

        for lc in settings.WA_LEGISLATIVE_CATEGORIES:
            try:
                legislative_category_obj, created = WALegislativeCategory.objects.get_or_create(
                    code=lc["code"],
                    defaults={"label": lc["label"]},
                )
                if created:
                    logger.info(f"Created LegislativeCategory: {legislative_category_obj}")
            except Exception as e:
                logger.error(f"{e}, LegislativeCategory: {lc}")

        for pl in settings.WA_PRIORITY_LISTS:
            try:
                priority_list_obj, created = WAPriorityList.objects.get_or_create(
                    code=pl["code"],
                    defaults={"label": pl["label"]},
                )
                if created:
                    logger.info(f"Created PriorityList: {priority_list_obj}")
            except Exception as e:
                logger.error(f"{e}, PriorityList: {pl}")

        for pc in settings.WA_PRIORITY_CATEGORIES:
            try:
                priority_category_obj, created = WAPriorityCategory.objects.get_or_create(
                    code=pc["code"],
                    defaults={"label": pc["label"]},
                )
                if created:
                    logger.info(f"Created PriorityCategory: {priority_category_obj}")
            except Exception as e:
                logger.error(f"{e}, PriorityCategory: {pc}")

        for ccl in settings.COMMONWEALTH_CONSERVATION_LISTS:
            try:
                conservation_list_obj, created = CommonwealthConservationList.objects.get_or_create(
                    code=ccl["code"],
                    defaults={"label": ccl["label"]},
                )
                if created:
                    logger.info(f"Created ConservationList: {conservation_list_obj}")
            except Exception as e:
                logger.error(f"{e}, ConservationList: {ccl}")
