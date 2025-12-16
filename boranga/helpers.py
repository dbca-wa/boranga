import logging
import os
import tarfile
import zipfile
from urllib.parse import urlparse

import py7zr
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.db import models
from django.db.models import Q
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ledger_api_client.managed_models import SystemGroup
from multiselectfield import MultiSelectField

from boranga.settings import (
    DJANGO_ADMIN_GROUP,
    GROUP_NAME_CONSERVATION_STATUS_APPROVER,
    GROUP_NAME_CONSERVATION_STATUS_ASSESSOR,
    GROUP_NAME_EXTERNAL_CONTRIBUTOR,
    GROUP_NAME_INTERNAL_CONTRIBUTOR,
    GROUP_NAME_OCCURRENCE_APPROVER,
    GROUP_NAME_OCCURRENCE_ASSESSOR,
    GROUP_NAME_READONLY_USER,
    GROUP_NAME_SPECIES_COMMUNITIES_APPROVER,
)

logger = logging.getLogger(__name__)


def check_file(file, model_name):
    from boranga.components.main.models import FileExtensionWhitelist

    # check if extension in whitelist
    cache_key = settings.CACHE_KEY_FILE_EXTENSION_WHITELIST
    whitelist = cache.get(cache_key)
    if whitelist is None:
        whitelist = FileExtensionWhitelist.objects.all()
        cache.set(cache_key, whitelist, settings.CACHE_TIMEOUT_2_HOURS)

    valid, compression = file_extension_valid(str(file), whitelist, model_name)

    if not valid:
        raise ValidationError("File type/extension not supported")

    if compression:
        # supported compression check
        valid = compressed_content_valid(file, whitelist, model_name)
        if not valid:
            raise ValidationError("Unsupported type/extension in compressed file")


def file_extension_valid(file, whitelist, model):

    logger.info("Uploaded File: " + file + " For Model: " + model)

    filename, extension = os.path.splitext(file)
    extension = extension.replace(".", "").lower()

    check = whitelist.filter(name=extension).filter(
        Q(model="all") | Q(model__iexact=model)
    )
    valid = check.exists()
    compression = False

    if valid:
        compression = check.first().compressed
    else:
        logger.warning(
            "Uploaded File: " + file + " For Model: " + model + " to be Rejected"
        )

    return valid, compression


def tar_content_valid(file, whitelist, model):
    file = file.open()
    tarFile = tarfile.TarFile(fileobj=file.file)
    for i in tarFile.getnames():
        valid, compression = file_extension_valid(i, whitelist, model)
        if compression:
            logger.warning(
                "Uploaded File: "
                + str(file)
                + " For Model: "
                + model
                + " to be Rejected"
            )
            raise ValidationError(
                "Compressed files not supported within compressed files"
            )
        if not valid:
            return False

    return True


def sevenz_content_valid(file, whitelist, model):
    sevenZipFile = py7zr.SevenZipFile(file.file)
    for i in sevenZipFile.getnames():
        valid, compression = file_extension_valid(i, whitelist, model)
        if compression:
            logger.warning(
                "Uploaded File: "
                + str(file)
                + " For Model: "
                + model
                + " to be Rejected"
            )
            raise ValidationError(
                "Compressed files not supported within compressed files"
            )
        if not valid:
            return False

    return True


def zip_content_valid(file, whitelist, model):
    zipFile = zipfile.ZipFile(file)
    for i in zipFile.filelist:
        valid, compression = file_extension_valid(i.filename, whitelist, model)
        if compression:
            if not i.filename.endswith(".zip"):
                logger.warning(
                    "Uploaded File: "
                    + str(file)
                    + " For Model: "
                    + model
                    + " to be Rejected"
                )
                raise ValidationError(
                    "The only compressed format allowed in a .zip file is .zip"
                )
            valid = zip_content_valid(zipFile.open(i.filename), whitelist, model)

        if not valid:
            return False

    return True


def compressed_content_valid(file, whitelist, model):

    file = file.open()

    if zipfile.is_zipfile(file):
        return zip_content_valid(file, whitelist, model)

    file = file.open()

    if py7zr.is_7zfile(file.file):
        return sevenz_content_valid(file, whitelist, model)

    file = file.open()

    if tarfile.is_tarfile(file):
        return tar_content_valid(file, whitelist, model)

    raise ValidationError("Compression/Archive format type not supported")


def filefield_exists(fieldfile_or_document) -> bool:
    """
    Return True if the supplied FieldFile (or Document instance exposing an
    attribute named `_file`) points at an existing file in the configured
    storage backend.

    - Accepts either a Django FieldFile (e.g. `document._file`) or a model
      instance that contains a `_file` attribute (the project's Document models
      use `_file` for the FileField).
    - For filesystem-backed storages this uses `os.path.exists(fieldfile.path)`
      which is fast. For non-filesystem storages it falls back to
      `storage.exists(name)` which works for S3/GCS/etc.
    """
    if not fieldfile_or_document:
        return False

    # Normalize to a FieldFile-like object
    fieldfile = getattr(fieldfile_or_document, "_file", fieldfile_or_document)

    name = getattr(fieldfile, "name", None)
    if not name:
        return False

    try:
        # Fast path for filesystem storages
        try:
            return os.path.exists(fieldfile.path)
        except (ValueError, NotImplementedError, AttributeError):
            # Storage doesn't expose a local path — use storage.exists()
            return fieldfile.storage.exists(name)
    except Exception:
        logger.exception("Failed to check file existence for %s", name)
        return False


def superuser_ids_list():
    cache_key = settings.CACHE_KEY_SUPERUSER_IDS
    superuser_ids = cache.get(cache_key)
    if superuser_ids is None:
        superuser_ids = list(
            EmailUser.objects.filter(is_superuser=True).values_list("id", flat=True)
        )
        cache.set(cache_key, superuser_ids, settings.CACHE_TIMEOUT_5_SECONDS)
    return superuser_ids


def belongs_to_by_user_id(user_id, group_name):
    superuser_ids = superuser_ids_list()
    if superuser_ids and user_id in superuser_ids:
        return True
    cache_key = settings.CACHE_KEY_USER_BELONGS_TO_GROUP.format(
        **{"user_id": user_id, "group_name": group_name}
    )
    belongs_to = cache.get(cache_key)
    if belongs_to is None:
        system_group = SystemGroup.objects.filter(name=group_name).first()
        belongs_to = (
            system_group and user_id in system_group.get_system_group_member_ids()
        )
        cache.set(cache_key, belongs_to, settings.CACHE_TIMEOUT_5_SECONDS)
    return belongs_to


def get_relative_url(url):
    if url and url.startswith("http"):
        return urlparse(url).path
    return url


def belongs_to(request, group_name, internal_only=False, external_only=False):
    if not request.user.is_authenticated:
        return False
    if request.user.is_superuser:
        return True
    if internal_only and not is_internal(request):
        return False
    if external_only and is_internal(request):
        return False

    return belongs_to_by_user_id(request.user.id, group_name)


def belongs_to_groups(request, group_names: list) -> bool:
    if not request.user.is_authenticated:
        return False
    if request.user.is_superuser:
        return True

    for group_name in group_names:
        if belongs_to_by_user_id(request.user.id, group_name):
            return True

    return False


def belongs_to_groups_by_user_id(user_id: int, group_names: list) -> bool:
    superuser_ids = superuser_ids_list()
    if superuser_ids and user_id in superuser_ids:
        return True

    for group_name in group_names:
        if belongs_to_by_user_id(user_id, group_name):
            return True

    return False


def member_ids(group_name, include_superusers=True):
    # Centralised member_ids method that includes all superusers (not totally sure we want this yet)
    system_group = SystemGroup.objects.filter(name=group_name).first()
    if not system_group:
        logger.warning(f"SystemGroup {group_name} not found")
        return []
    member_ids = system_group.get_system_group_member_ids()
    if include_superusers:
        member_ids += superuser_ids_list()
    return member_ids


def is_django_admin(request):
    return belongs_to(request, DJANGO_ADMIN_GROUP, internal_only=True)


def is_readonly_user(request):
    return belongs_to(request, GROUP_NAME_READONLY_USER, internal_only=True)


def is_species_communities_approver(request):
    return belongs_to(
        request, GROUP_NAME_SPECIES_COMMUNITIES_APPROVER, internal_only=True
    )


def is_conservation_status_assessor(request):
    return belongs_to(
        request, GROUP_NAME_CONSERVATION_STATUS_ASSESSOR, internal_only=True
    )


def is_conservation_status_approver(request):
    return belongs_to(
        request, GROUP_NAME_CONSERVATION_STATUS_APPROVER, internal_only=True
    )


def is_internal_contributor(request):
    return belongs_to(request, GROUP_NAME_INTERNAL_CONTRIBUTOR, internal_only=False)


def is_occurrence_assessor(request):
    return belongs_to(request, GROUP_NAME_OCCURRENCE_ASSESSOR, internal_only=True)


def is_occurrence_approver(request):
    return belongs_to(request, GROUP_NAME_OCCURRENCE_APPROVER, internal_only=True)


def is_external_contributor(request):
    return belongs_to(request, GROUP_NAME_EXTERNAL_CONTRIBUTOR, external_only=True)


def is_contributor(request):
    return is_internal_contributor(request) or is_external_contributor(request)


def is_new_external_contributor(user_id):
    from boranga.components.conservation_status.models import ConservationStatus
    from boranga.components.occurrence.models import OccurrenceReport

    if not belongs_to_by_user_id(user_id, GROUP_NAME_EXTERNAL_CONTRIBUTOR):
        return False

    finalised_cs = ConservationStatus.objects.filter(
        submitter=user_id,
        processing_status__in=[
            ConservationStatus.PROCESSING_STATUS_APPROVED,
            ConservationStatus.PROCESSING_STATUS_DECLINED,
            ConservationStatus.PROCESSING_STATUS_DELISTED,
            ConservationStatus.PROCESSING_STATUS_CLOSED,
        ],
    ).exists()
    finalised_ocr = OccurrenceReport.objects.filter(
        submitter=user_id,
        processing_status__in=[
            OccurrenceReport.PROCESSING_STATUS_APPROVED,
            OccurrenceReport.PROCESSING_STATUS_DECLINED,
        ],
    ).exists()

    return not finalised_cs and not finalised_ocr


def is_conservation_status_referee(request, conservation_status=None):
    if not request.user.is_authenticated:
        return False

    if request.user.is_superuser:
        return True

    cache_key = settings.CACHE_KEY_USER_IS_REFEREE.format(
        **{
            "user_id": request.user.id,
            "model": "conservation_status",
            "pk": conservation_status,  # If None, that cache key will be used as a general check
        }
    )
    belongs_to = cache.get(cache_key)
    if belongs_to is None:
        from boranga.components.conservation_status.models import (
            ConservationStatusReferral,
        )

        qs = ConservationStatusReferral.objects.filter(referral=request.user.id)
        if conservation_status:
            qs = qs.filter(conservation_status=conservation_status)

        belongs_to = qs.exists()
        cache.set(cache_key, belongs_to, settings.CACHE_TIMEOUT_5_SECONDS)
    return belongs_to


def is_occurrence_report_referee(request, occurrence_report=None):
    if not request.user.is_authenticated:
        return False

    if request.user.is_superuser:
        return True

    cache_key = settings.CACHE_KEY_USER_IS_REFEREE.format(
        **{
            "user_id": request.user.id,
            "model": "occurrence_report",
            "pk": occurrence_report,  # If None, that cache key will be used as a general check
        }
    )
    belongs_to = cache.get(cache_key)
    if belongs_to is None:
        from boranga.components.occurrence.models import OccurrenceReportReferral

        qs = OccurrenceReportReferral.objects.filter(referral=request.user.id)
        if occurrence_report:
            qs = qs.filter(occurrence_report=occurrence_report)

        belongs_to = qs.exists()
        cache.set(cache_key, belongs_to, settings.CACHE_TIMEOUT_5_SECONDS)
    return belongs_to


def is_referee(request):
    return is_conservation_status_referee(request) or is_occurrence_report_referee(
        request
    )


def in_dbca_domain(request):
    user = request.user
    if not email_in_dbca_domain(user.email):
        return False

    if not user.is_staff:
        # hack to reset department user to is_staff==True, if the user logged in externally
        # (external departmentUser login defaults to is_staff=False)
        user.is_staff = True
        user.save()

    return True


def email_in_dbca_domain(email: str) -> bool:
    return email.split("@")[1] in settings.DEPT_DOMAINS


def is_in_organisation_contacts(request, organisation):
    return request.user.email in organisation.contacts.all().values_list(
        "email", flat=True
    )


def is_departmentUser(request):
    return request.user.is_authenticated and in_dbca_domain(request)


def is_customer(request):
    return request.user.is_authenticated and not request.user.is_staff


def is_internal(request):
    return is_departmentUser(request) and (
        belongs_to_groups(request, settings.INTERNAL_GROUPS)
    )


def is_internal_by_user_id(user_id):
    return belongs_to_groups_by_user_id(user_id, settings.INTERNAL_GROUPS)


def get_all_officers():
    return EmailUser.objects.filter(groups__name=settings.ADMIN_GROUP)


def get_instance_identifier(instance):
    """Checks the instance for the attributes specified in settings"""
    for field in settings.ACTION_LOGGING_IDENTIFIER_FIELDS:
        if hasattr(instance, field):
            return getattr(instance, field)
    raise AttributeError(
        f"Model instance has no valid identifier to use for logging. Tried: {settings.ACTION_LOGGING_IDENTIFIER_FIELDS}"
    )


def get_openpyxl_data_validation_type_for_django_field(field, column=None):
    from openpyxl.worksheet.datavalidation import DataValidation

    if field in ["species", "community"]:
        # There are always lookup tables (never embedded validation)
        return None

    dv_types = dict(zip(DataValidation.type.values, DataValidation.type.values))

    field_type_map = {
        models.CharField: "textLength",
        models.IntegerField: "whole",
        models.DecimalField: "decimal",
        models.BooleanField: "list",
        models.ForeignKey: "list",
        models.DateField: "date",
        models.DateTimeField: "date",
    }

    if isinstance(field, MultiSelectField) or (
        isinstance(field, models.CharField) and field.choices
    ):
        return dv_types["list"]

    if column:
        if isinstance(field, models.IntegerField) and column.is_emailuser_column:
            # No embedded validation for email user columns because
            # validating emails in Excel is a pain
            return None

    for django_field, dv_type in field_type_map.items():
        if isinstance(field, django_field):
            return dv_types[dv_type]

    # Return textLength as a default so that we can still use the allow
    # blank validation in the Excel sheet for things such as TextField with
    # no max_length and other fields such as ForeignKey or M2M fields
    return dv_types["textLength"]


def clone_model(
    source_model_class: models.base.ModelBase,
    target_model_class: models.base.ModelBase,
    source_model: models.Model,
    save: bool = False,
) -> models.Model:
    """
    Copies field values from source_model to a new instance of target_model_class.

    Will complain if:
        - source_model is not an instance of source_model_class.
        - the new instance of target_model_class does not contain a field that is in source_model.

    Returns None if source_model is None so caller must check for existence of return value.

    Pass save=True to save the new instance to the database automatically after copying the field values.
    """
    if source_model is None:
        return None

    if not isinstance(source_model, source_model_class):
        raise ValueError(
            f"source_model is not an instance of {source_model_class.__name__}"
        )

    target_model = target_model_class()

    try:
        for field in source_model._meta.fields:
            if field.primary_key:
                continue

            setattr(target_model, field.name, getattr(source_model, field.name))
    except AttributeError as e:
        logger.error(
            f"Error copying field values from {source_model} to {target_model}: {e}"
        )
    if save:
        target_model.save()

    return target_model


def convert_external_url_to_internal_url(url):
    if not settings.SITE_SUBDOMAIN_INTERNAL_SUFFIX:
        return url

    if settings.SITE_SUBDOMAIN_INTERNAL_SUFFIX not in url:
        # Add the internal subdomain suffix to the url
        url = f"{settings.SITE_SUBDOMAIN_INTERNAL_SUFFIX}.{settings.SITE_DOMAIN}".join(
            url.split("." + settings.SITE_DOMAIN)
        )
    return url


def convert_internal_url_to_external_url(url):
    if not settings.SITE_SUBDOMAIN_INTERNAL_SUFFIX:
        return url

    if settings.SITE_SUBDOMAIN_INTERNAL_SUFFIX in url:
        # remove '-internal'. This email is for external submitters
        url = "".join(url.split(settings.SITE_SUBDOMAIN_INTERNAL_SUFFIX))
    return url


def get_display_field_for_model(model: models.Model) -> str:
    """
    Returns the field name to display for a model in the admin list display.
    """
    # Find the best field to use for a display value
    field_names = [field.name for field in model._meta.get_fields()]
    for field_name in settings.OCR_BULK_IMPORT_LOOKUP_TABLE_DISPLAY_FIELDS:
        if field_name in field_names:
            return field_name

    # If we can't find a display field, we'll just use the first CharField we find
    for field_name in field_names:
        if isinstance(model._meta.get_field(field_name), models.fields.CharField):
            return field_name

    return "id"


def get_choices_for_field(
    model_class: models.base.ModelBase, field: models.Field
) -> list | None:
    from boranga.components.main.models import ArchivableModel
    from boranga.components.occurrence.models import OccurrenceReport

    if model_class is OccurrenceReport and field.name == "processing_status":
        # Only certain statuses are valid for OCR bulk import processing
        return OccurrenceReport.VALID_BULK_IMPORT_PROCESSING_STATUSES

    choices = field.choices if hasattr(field, "choices") else None

    if isinstance(field, MultiSelectField):
        # Have to create an instance for the choices to be populated :-(
        # as for some reason they are populated in the __init__ method
        instance = model_class()
        multi_select_field = instance._meta.get_field(field.name)
        choices = multi_select_field.choices
    elif isinstance(field, (models.ForeignKey, models.ManyToManyField)):
        related_model = field.related_model
        related_model_qs = related_model.objects.all()

        if issubclass(related_model, ArchivableModel):
            related_model_qs = related_model_qs.filter(archived=False)

        related_model_count = related_model_qs.count()

        if (
            related_model_count == 0
            or field.name in ["species", "community"]
            or related_model_count > settings.OCR_BULK_IMPORT_LOOKUP_TABLE_RECORD_LIMIT
        ):
            choices = None
        else:
            display_field = get_display_field_for_model(related_model)
            choices = list(related_model_qs.values_list("id", display_field))

    return choices


def get_lookup_field_options_for_field(field: models.Field) -> list | None:
    lookup_field_options = None

    if isinstance(field, (models.ForeignKey, models.ManyToManyField)):
        related_model = field.related_model
        lookup_field_options = [
            field.verbose_name.lower()
            for field in related_model._meta.get_fields()
            if not field.related_model
            and (hasattr(field, "unique") and field.unique)
            and not field.name.endswith("_number")
        ]

    return lookup_field_options


def get_filter_field_options_for_field(field: models.Field) -> list:
    if not isinstance(field, (models.ForeignKey, models.ManyToManyField)):
        return []

    return [
        field.name
        for field in field.related_model._meta.get_fields()
        if not field.related_model
    ]


def get_mock_request(emailuser: EmailUser):
    request = type("Request", (), {})()
    request.user = type("User", (), {})()
    request.user.id = emailuser.id
    return request


# Because the OCR Bulk Importer uses commas to embed list validation in the .xlsx
# import files, we need to ensure that the display field for the lookup table
# does not contain commas.
no_commas_validator = RegexValidator(
    "[,]",
    code="comma_not_allowed",
    message="Commas not allowed in the display field for a django lookup.",
    inverse_match=True,
)
