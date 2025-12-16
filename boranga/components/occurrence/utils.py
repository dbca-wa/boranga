import logging
import os
from zipfile import ZipFile

import geopandas as gpd
from django.apps import apps
from django.conf import settings
from django.contrib.gis.gdal import SpatialReference
from django.contrib.gis.geos import GEOSGeometry
from django.core.exceptions import ValidationError
from django.core.files import File
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from boranga.components.occurrence.email import (
    send_submit_email_notification,
    send_submitter_submit_email_notification,
)
from boranga.components.occurrence.models import (
    OccurrenceReport,
    OccurrenceReportAmendmentRequest,
    OccurrenceReportUserAction,
)
from boranga.components.species_and_communities.models import Species
from boranga.helpers import is_internal

logger = logging.getLogger(__name__)


@transaction.atomic
def ocr_proposal_submit(ocr_proposal, request):

    ocr_proposal.validate_submit()

    if not ocr_proposal.can_user_edit(request):
        raise ValidationError(
            "You can't submit this report at the moment due to the status or a permission issue"
        )

    ocr_proposal.submitter = request.user.id

    if is_internal(request):
        ocr_proposal.internal_application = True
    else:
        ocr_proposal.internal_application = False

    ocr_proposal.lodgement_date = timezone.now()

    # Set the status of any pending amendment requests to 'amended'
    ocr_proposal.amendment_requests.filter(
        status=OccurrenceReportAmendmentRequest.STATUS_CHOICE_REQUESTED
    ).update(status=OccurrenceReportAmendmentRequest.STATUS_CHOICE_AMENDED)

    # Create a log entry for the proposal
    ocr_proposal.log_user_action(
        OccurrenceReportUserAction.ACTION_LODGE_PROPOSAL.format(ocr_proposal.id),
        request,
    )

    # Create a log entry for the user
    request.user.log_user_action(
        OccurrenceReportUserAction.ACTION_LODGE_PROPOSAL.format(ocr_proposal.id),
        request,
    )

    ret1 = send_submit_email_notification(request, ocr_proposal)
    ret2 = send_submitter_submit_email_notification(request, ocr_proposal)

    if (settings.WORKING_FROM_HOME and settings.DEBUG) or ret1 and ret2:
        ocr_proposal.processing_status = (
            OccurrenceReport.PROCESSING_STATUS_WITH_ASSESSOR
        )
        ocr_proposal.customer_status = OccurrenceReport.PROCESSING_STATUS_WITH_ASSESSOR
        ocr_proposal.save()
    else:
        raise ValidationError(
            "An error occurred while submitting occurrence report (Submit email notifications failed)"
        )

    return ocr_proposal


def save_document(request, instance, comms_instance, document_type, input_name=None):
    if "filename" in request.data and input_name:
        filename = request.data.get("filename")
        _file = request.data.get("_file")

        if document_type == "shapefile_document":
            document = instance.shapefile_documents.get_or_create(
                input_name=input_name, name=filename
            )[0]
        else:
            raise ValidationError(f"Invalid document type {document_type}")

        document.check_file(request.data.get("_file"))
        document._file = _file
        document.save()


@transaction.atomic
def delete_document(request, instance, comms_instance, document_type, input_name=None):
    document_id = request.data.get("document_id", None)
    if document_id:
        if document_type == "shapefile_document":
            document = instance.shapefile_documents.get(id=document_id)
        else:
            raise ValidationError(f"Invalid document type {document_type}")

        if not document:
            raise ValidationError(f"Document id {document_id} not found")

        if document._file and os.path.isfile(document._file.path):
            os.remove(document._file.path)

        document.delete()


@transaction.atomic
def process_shapefile_document(request, instance, *args, **kwargs):
    action = request.data.get("action")
    input_name = request.data.get("input_name")
    document_type = "shapefile_document"
    request.data.get("document_id")
    comms_instance = None

    if action == "list":
        pass
    elif action == "delete":
        delete_document(request, instance, comms_instance, document_type, input_name)
    elif action == "save":
        save_document(request, instance, comms_instance, document_type, input_name)
    else:
        raise ValidationError(f"Invalid action {action} for shapefile document")

    documents_qs = instance.shapefile_documents

    returned_file_data = [
        dict(
            url=d.path,
            id=d.id,
            name=d.name,
        )
        for d in documents_qs.filter(input_name=input_name)
        if d._file
    ]
    return {"filedata": returned_file_data}


def extract_attached_archives(instance, foreign_key_field=None):
    """Extracts shapefiles from attached zip archives and saves them as shapefile documents."""

    archive_files_qs = instance.shapefile_documents.filter(Q(name__endswith=".zip"))
    instance_name = instance._meta.model.__name__
    shapefile_archives = [qs._file for qs in archive_files_qs]

    for archive in shapefile_archives:
        archive_path = os.path.dirname(archive.path)
        z = ZipFile(archive.path, "r")
        z.extractall(archive_path)

        for zipped_file in z.filelist:
            extracted_file_path = os.path.join(archive_path, zipped_file.filename)
            shapefile_model = apps.get_model(
                "boranga", f"{instance_name}ShapefileDocument"
            )

            # Open the extracted file and create a Django File object
            with open(extracted_file_path, "rb") as f:
                django_file = File(f, name=zipped_file.filename)
                shapefile_model.objects.create(
                    **{
                        foreign_key_field: instance,
                        "name": zipped_file.filename,
                        "input_name": "shapefile_document",
                        "_file": django_file,
                    }
                )

            # Remove the temporary extracted file since it's now in Django's storage
            if os.path.exists(extracted_file_path):
                os.remove(extracted_file_path)

    return archive_files_qs


def validate_map_files(request, instance, foreign_key_field=None):
    # Validates shapefiles uploaded with via the proposal map or the competitive process map.
    # Shapefiles are valid when the shp, shx, and dbf extensions are provided
    # and when they intersect with DBCA legislated land or water polygons

    valid_geometry_saved = False

    if not instance.shapefile_documents.exists():
        raise ValidationError(
            "Please attach at least a .shp, .shx, and .dbf file (the .prj file is optional but recommended)"
        )

    archive_files_qs = extract_attached_archives(instance, foreign_key_field)

    # Shapefile extensions shp (geometry), shx (index between shp and dbf), dbf (data) are essential
    shp_file_qs = instance.shapefile_documents.filter(
        Q(name__endswith=".shp")
        | Q(name__endswith=".shx")
        | Q(name__endswith=".dbf")
        | Q(name__endswith=".prj")
    )

    # Validate shapefile and all the other related files are present
    if not shp_file_qs and not archive_files_qs:
        raise ValidationError(
            "You can only attach files with the following extensions: .shp, .shx, and .dbf or .zip"
        )

    shp_files = shp_file_qs.filter(name__endswith=".shp").distinct()
    shp_file_basenames = [s[:-4] for s in shp_files.values_list("name", flat=True)]

    shx_files = shp_file_qs.filter(name__in=[f"{b}.shx" for b in shp_file_basenames])
    dbf_files = shp_file_qs.filter(name__in=[f"{b}.dbf" for b in shp_file_basenames])

    # Check if no required files are missing
    if any(f == 0 for f in [shp_files.count(), shx_files.count(), dbf_files.count()]):
        raise ValidationError(
            "Please attach at least a .shp, .shx, and .dbf file (the .prj file is optional but recommended)"
        )
    # Check if all files have the same count
    if not (shp_files.count() == shx_files.count() == dbf_files.count()):
        raise ValidationError(
            "Please attach at least a .shp, .shx, and .dbf file "
            "(the .prj file is optional but recommended) for every shapefile"
        )

    # Add the shapefiles to a zip file for archiving purposes
    # (as they are deleted after being converted to proposal geometry)
    shapefile_archive_name = (
        os.path.splitext(instance.shapefile_documents.first().path)[0]
        + "-"
        + timezone.now().strftime("%Y%m%d%H%M%S")
        + ".zip"
    )
    shapefile_archive = ZipFile(shapefile_archive_name, "w")
    for shp_file_obj in shp_file_qs:
        shapefile_archive.write(shp_file_obj.path, shp_file_obj.name)
    shapefile_archive.close()

    # A list of all uploaded shapefiles
    shp_file_objs = shp_file_qs.filter(Q(name__endswith=".shp"))

    for shp_file_obj in shp_file_objs:
        gdf = gpd.read_file(shp_file_obj.path)  # Shapefile to GeoDataFrame

        if gdf.empty:
            if archive_files_qs:
                instance.shapefile_documents.exclude(name__endswith=".zip").delete()
            raise ValidationError(f"Geometry is empty in {shp_file_obj.name}")

        if gdf.geometry.crs is None:
            if archive_files_qs:
                instance.shapefile_documents.exclude(name__endswith=".zip").delete()
            raise ValidationError(
                f"Geometry in {shp_file_obj.name} has no coordinate reference system (CRS)"
            )

        # spatial reference identifier of the original uploaded geometry
        original_srid = SpatialReference(gdf.geometry.crs.srs).srid

        # If no prj file assume WGS-84 datum
        if not gdf.crs:
            gdf_transform = gdf.set_crs("epsg:4326", inplace=True)
        else:
            gdf_transform = gdf.to_crs("epsg:4326")

        geometries = gdf_transform.geometry  # GeoSeries

        # Only accept points or polygons
        geom_type = geometries.geom_type.values[0]
        if geom_type not in ("Point", "MultiPoint", "Polygon", "MultiPolygon"):
            raise ValidationError(f"Geometry of type {geom_type} not allowed")

        # Check for intersection with DBCA geometries
        gdf_transform["valid"] = False
        for idx, row in gdf_transform.iterrows():
            srid = 4326  # We transformed to 4326 above

            geometry = GEOSGeometry(row.geometry.wkt, srid=srid)
            original_geometry = GEOSGeometry(
                gdf.loc[idx, "geometry"].wkt, srid=original_srid
            )

            # Add the file name as identifier to the geojson for use in the frontend
            if "source_" not in gdf_transform:
                gdf_transform["source_"] = shp_file_obj.name

            gdf_transform["valid"] = True

            # Some generic code to save the geometry to the database
            # That will work for both a proposal instance and a competitive process instance
            instance_name = instance._meta.model.__name__
            if not foreign_key_field:
                foreign_key_field = instance_name.lower()

            geometry_model = apps.get_model("boranga", f"{instance_name}Geometry")
            geometry_model.objects.create(
                **{
                    foreign_key_field: instance,
                    "geometry": geometry,
                    "original_geometry_ewkb": original_geometry.ewkb,
                    "drawn_by": request.user.id,
                }
            )

        instance.save(no_revision=True)
        valid_geometry_saved = True

    # Delete all shapefile documents so the user can upload another one if they wish.
    instance.shapefile_documents.all().delete()

    return valid_geometry_saved


# gets all species that are related to the occurrence's species - parents, children, parent's parents, etc
def get_all_related_species(species_id, exclude=[]):

    species_ids = []
    # add species id to list
    species_ids.append(species_id)
    species = Species.objects.get(id=species_id)

    # iterate through many to many relationship - this should contain both "parents" and "children"
    for i in species.parent_species.all():
        # only process what has not already been processed
        if i.id not in exclude:
            # update list here (temporarily) to prevent infinite loop caused by a circular relation
            # (unlikely but possible)
            temp_exclude = exclude + species_ids
            species_ids = species_ids + get_all_related_species(
                i.id, exclude=temp_exclude
            )
            # update the exclude list with the newly added species_ids so they are not processed again
            exclude = exclude + species_ids
    return species_ids
