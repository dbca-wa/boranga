from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from boranga.components.data_migration import utils
from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.species_and_communities.models import GroupType

# Header → canonical key
COLUMN_MAP = {
    "SHEETNO": "migrated_from_id",
    "SPNAME": "species_id",
    "COMM_NAME": "community_id",  # TODO replace with real column name later
    # approved_by - has no column as is derived from two other columns: See synthetic fields in handler
    # assessor_data - is pre-populated by concatenating two other columns: see tpfl adapter
    # comments - synthetic field, see pipelines
    "CREATED_DATE": "lodgement_date",
    "OBSERVATION_DATE": "observation_date",
    # ocr_for_occ_name - is pre-populated by concatenating two other columns: see tpfl adapter
    # ocr_for_occ_number - synthetic field, see pipelines
    "FORM_STATUS_CODE": "processing_status",
    "RECORD_SRC_CODE": "record_source",
    # reported_date: just a copy of lodgement_date, so will be applied in handler
    "CREATED_BY": "submitter",
    "MODIFIED_BY": "modified_by",  # used in tpfl adapter to derive submitter
    # OCRObserverDetail fields
    "OBS_ROLE_CODE": "OCRObserverDetail__role",
    # OCRObserverDetail__main_observer - is pre-populated in tpfl adapter
    "OBS_NAME": "OCRObserverDetail__observer_name",
    "OBSERVER_CODE": "OBSERVER_CODE",
    # SHEET_* fields for ocr_for_occ_name composition in tpfl adapter
    "SHEET_POP_NUMBER": "SHEET_POP_NUMBER",
    "SHEET_SUBPOP_CODE": "SHEET_SUBPOP_CODE",
    # OCRHabitatComposition fields
    "ROCK_TYPE": "OCRHabitatComposition__rock_type",
    "GRAVEL": "OCRHabitatComposition__loose_rock_percent",
    "DRAINAGE": "OCRHabitatComposition__drainage",
    # Habitat composition extras
    "HABITAT_NOTES": "OCRHabitatComposition__habitat_notes",
    "ASPECT": "ASPECT",
    "HABITAT_CONDITION": "HABITAT_CONDITION",
    "SV_VEGETATION_CONDITION": "OCRHabitatComposition__vegetation_condition",
    "SOIL_COLOR": "OCRHabitatComposition__soil_colour",
    "SOIL_CONDITION": "OCRHabitatComposition__soil_condition",
    "LANDFORM": "OCRHabitatComposition__land_form",
    "SOIL_TYPE": "OCRHabitatComposition__soil_type",
    # Identification fields
    "BARCODE": "OCRIdentification__barcode_number",
    "COLLECTOR_NO": "OCRIdentification__collector_number",
    "LICENCE": "OCRIdentification__permit_id",
    # Identification comment composition parts (preserve raw so adapter can map+prefix)
    "VCHR_STATUS_CODE": "VCHR_STATUS_CODE",
    "DUPVOUCH_LOCATION": "DUPVOUCH_LOCATION",
    # Voucher location (sample destination) closed-list mapping
    "VOUCHER_LOCATION": "OCRIdentification__sample_destination",
    # OCRLocation fields
    "CO_ORD_SOURCE_CODE": "OCRLocation__coordinate_source",
    "DISTRICT": "OCRLocation__district",
    "LANDDISTRICT": "OCRLocation__locality",
    "RESOLUTION": "OCRLocation__location_accuracy",
    # location_description: composed from LOCATION + LGA_CODE
    "LOCATION": "LOCATION",
    "LGA_CODE": "LGA_CODE",
    # OCRObservationDetail fields (Task 11380, 11382, 11383, 11385)
    "SVY_EXTENT": "OCRObservationDetail__area_assessment",
    "SVY_EFFORT_AREA": "OCRObservationDetail__area_surveyed",
    "SVY_EFFORT_TIME": "OCRObservationDetail__survey_duration",
    # OCRAssociatedSpecies fields (Task 11456)
    "ASSOCIATED_SPECIES": "OCRAssociatedSpecies__comment",
    # OccurrenceReportGeometry fields (Task 11359, 11364, 11366)
    "GDA94LAT": "GDA94LAT",
    "GDA94LONG": "GDA94LONG",
    # TPFL raw fields (preserve these so TPFL-specific transforms can read them)
    "PURPOSE1": "PURPOSE1",
    "PURPOSE2": "PURPOSE2",
    "VESTING": "VESTING",
    "FENCING_STATUS": "FENCING_STATUS",
    "FENCING_COMMENTS": "FENCING_COMMENTS",
    "ROADSIDE_MARKER_STATUS": "ROADSIDE_MARKER_STATUS",
    "RDSIDE_MKR_COMMENTS": "RDSIDE_MKR_COMMENTS",
    # OCRPlantCount fields
    "CNT_PLANT_TYPE_CODE": "OCRPlantCount__counted_subject",
    "POPULATION_CONDITION": "OCRPlantCount__plant_condition",
    "COUNT_MTHD_CODE": "OCRPlantCount__plant_count_method",
    "CLONAL": "OCRPlantCount__clonal_reproduction_present",
    # comment composition parts
    "POPULATION_NOTES": "POPULATION_NOTES",
    "AREA_OCCUPIED_METHOD": "AREA_OCCUPIED_METHOD",
    "QUAD_SIZE": "QUAD_SIZE",
    "QUAD_NUM_TOTAL": "QUAD_NUM_TOTAL",
    "QUAD_NUM_MATURE": "QUAD_NUM_MATURE",
    "QUAD_NUM_JUVENILE": "QUAD_NUM_JUVENILE",
    "QUAD_NUM_SEEDLINGS": "QUAD_NUM_SEEDLINGS",
    # count_status derived from other fields
    "DEHISCED_FRUIT": "OCRPlantCount__dehisced_fruit_present",
    "JUVENILE_PLANTS": "OCRPlantCount__detailed_alive_juvenile",
    "MATURE_PLANTS": "OCRPlantCount__detailed_alive_mature",
    "SEEDLING_PLANTS": "OCRPlantCount__detailed_alive_seedling",
    "JUVENILE_DEAD": "OCRPlantCount__detailed_dead_juvenile",
    "MATURE_DEAD": "OCRPlantCount__detailed_dead_mature",
    "SEEDLING_DEAD": "OCRPlantCount__detailed_dead_seedling",
    "AREA_OCCUPIED": "OCRPlantCount__estimated_population_area",
    "IN_BUDS": "OCRPlantCount__flower_bud_present",
    "IN_FLOWER": "OCRPlantCount__flower_present",
    "FLOWER_PERCENTAGE": "OCRPlantCount__flowering_plants_per",
    "IMMATURE_FRUIT": "OCRPlantCount__immature_fruit_present",
    "POLLINATOR": "OCRPlantCount__pollinator_observation",
    "QUAD_NUM": "OCRPlantCount__quadrats_surveyed",
    "FRUIT": "OCRPlantCount__ripe_fruit_present",
    "SIMPLE_LIVE_TOT": "OCRPlantCount__simple_alive",
    "SIMPLE_DEAD_TOT": "OCRPlantCount__simple_dead",
    "QUAD_TOT_SQ_M": "OCRPlantCount__total_quadrat_area",
    "VEGETATIVE": "OCRPlantCount__vegetative_state_present",
}

REQUIRED_COLUMNS = [
    "migrated_from_id",
    "processing_status",
]

PIPELINES: dict[str, list[str]] = {}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    source_choices=None,
)

# Convenience exports
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class OccurrenceReportRow:
    """
    Canonical (post-transform) occurrence report data for persistence.
    """

    migrated_from_id: str
    Occurrence__migrated_from_id: str
    group_type_id: int
    species_id: int | None = None  # FK id (Species) after transform
    community_id: int | None = None  # FK id (Community) after transform
    processing_status: str | None = None
    customer_status: str | None = None
    observation_date: date | None = None
    record_source: str | None = None
    comments: str | None = None
    ocr_for_occ_number: str | None = None
    ocr_for_occ_name: str | None = None
    assessor_data: str | None = None
    reported_date: date | None = None  # copy of lodgement_date
    lodgement_date: date | None = None
    approved_by: int | None = None  # FK id (EmailUser) after transform
    submitter: int | None = None  # FK id (EmailUser) after transform

    # OCRObserverDetail fields
    OCRObserverDetail__role: str | None = None

    # SubmitterInformation fields
    SubmitterInformation__submitter_category: int | None = (
        None  # FK id (SubmitterCategory)
    )
    SubmitterInformation__email_user: int | None = None  # EmailUser id
    SubmitterInformation__name: str | None = None
    SubmitterInformation__organisation: str | None = None

    # OCRHabitatComposition fields
    OCRHabitatComposition__rock_type: str | None = None
    OCRHabitatComposition__loose_rock_percent: int | None = None
    OCRHabitatComposition__drainage: str | None = None
    OCRHabitatComposition__habitat_notes: str | None = None
    OCRHabitatComposition__vegetation_condition: str | None = None
    OCRHabitatComposition__soil_colour: str | None = None
    OCRHabitatComposition__soil_condition: str | None = None
    OCRHabitatComposition__land_form: str | None = None
    OCRHabitatComposition__soil_type: str | None = None

    # OCRIdentification fields
    OCRIdentification__barcode_number: str | None = None
    OCRIdentification__collector_number: str | None = None
    OCRIdentification__permit_id: str | None = None
    OCRIdentification__identification_comment: str | None = None
    OCRIdentification__identification_certainty: int | None = None
    OCRIdentification__sample_destination: int | None = None

    # OCRLocation fields
    OCRLocation__coordinate_source: int | None = None  # FK id (CoordinateSource)
    OCRLocation__location_accuracy: int | None = None  # FK id (LocationAccuracy)
    OCRLocation__district: int | None = None  # FK id (District)
    OCRLocation__region: int | None = None  # FK id (Region)
    OCRLocation__locality: str | None = None
    OCRLocation__location_description: str | None = None
    OCRLocation__boundary_description: str | None = None
    OCRLocation__epsg_code: int | None = None

    # OCRObservationDetail fields
    OCRObservationDetail__area_assessment: int | None = None  # FK id (AreaAssessment)
    OCRObservationDetail__area_surveyed: str | None = (
        None  # Decimal with 4 decimal places
    )
    OCRObservationDetail__survey_duration: int | None = None  # Integer hours

    # OCRAssociatedSpecies fields
    OCRAssociatedSpecies__comment: str | None = None

    # OCRPlantCount fields
    OCRPlantCount__counted_subject: int | None = None  # FK id (CountedSubject)
    OCRPlantCount__plant_condition: int | None = None  # FK id (PlantCondition)
    OCRPlantCount__plant_count_method: int | None = None  # FK id (PlantCountMethod)
    OCRPlantCount__clonal_reproduction_present: bool | None = None
    OCRPlantCount__comment: str | None = None
    OCRPlantCount__count_status: str | None = None
    OCRPlantCount__dehisced_fruit_present: bool | None = None
    OCRPlantCount__detailed_alive_juvenile: int | None = None
    OCRPlantCount__detailed_alive_mature: int | None = None
    OCRPlantCount__detailed_alive_seedling: int | None = None
    OCRPlantCount__detailed_dead_juvenile: int | None = None
    OCRPlantCount__detailed_dead_mature: int | None = None
    OCRPlantCount__detailed_dead_seedling: int | None = None
    OCRPlantCount__estimated_population_area: Decimal | None = None
    OCRPlantCount__flower_bud_present: bool | None = None
    OCRPlantCount__flower_present: bool | None = None
    OCRPlantCount__flowering_plants_per: Decimal | None = None
    OCRPlantCount__immature_fruit_present: bool | None = None
    OCRPlantCount__pollinator_observation: str | None = None
    OCRPlantCount__quadrats_surveyed: int | None = None
    OCRPlantCount__ripe_fruit_present: bool | None = None
    OCRPlantCount__simple_alive: int | None = None
    OCRPlantCount__simple_dead: int | None = None
    OCRPlantCount__total_quadrat_area: Decimal | None = None
    OCRPlantCount__vegetative_state_present: bool | None = None
    OCRPlantCount__obs_date: date | None = None

    # OCRVegetationStructure fields
    OCRVegetationStructure__vegetation_structure_layer_one: str | None = None
    OCRVegetationStructure__vegetation_structure_layer_two: str | None = None
    OCRVegetationStructure__vegetation_structure_layer_three: str | None = None
    OCRVegetationStructure__vegetation_structure_layer_four: str | None = None

    @classmethod
    def from_dict(cls, d: dict) -> OccurrenceReportRow:
        """
        Build OccurrenceReportRow from pipeline output. Coerce simple types.
        """
        # lodgement_date and reported_date are datetimes; observation_date is date
        lodgement_dt = utils.parse_date_iso(d.get("lodgement_date"))
        reported_dt = utils.parse_date_iso(d.get("reported_date"))
        obs_dt = utils.parse_date_iso(d.get("observation_date"))
        obs_date = obs_dt.date() if obs_dt is not None else None

        return cls(
            migrated_from_id=str(d["migrated_from_id"]),
            Occurrence__migrated_from_id=d.get("Occurrence__migrated_from_id"),
            group_type_id=utils.to_int_maybe(d.get("group_type_id")),
            species_id=utils.to_int_maybe(d.get("species_id")),
            community_id=utils.to_int_maybe(d.get("community_id")),
            processing_status=utils.safe_strip(d.get("processing_status")),
            customer_status=utils.safe_strip(d.get("customer_status")),
            observation_date=obs_date,
            record_source=utils.safe_strip(d.get("record_source")),
            comments=utils.safe_strip(d.get("comments")),
            ocr_for_occ_number=utils.safe_strip(d.get("ocr_for_occ_number")),
            ocr_for_occ_name=utils.safe_strip(d.get("ocr_for_occ_name")),
            assessor_data=utils.safe_strip(d.get("assessor_data")),
            reported_date=reported_dt,
            lodgement_date=lodgement_dt,
            approved_by=utils.to_int_maybe(d.get("approved_by")),
            submitter=utils.to_int_maybe(d.get("submitter")),
            OCRObserverDetail__role=utils.safe_strip(d.get("OCRObserverDetail__role")),
            SubmitterInformation__submitter_category=utils.to_int_maybe(
                d.get("SubmitterInformation__submitter_category")
            ),
            SubmitterInformation__email_user=utils.to_int_maybe(
                d.get("SubmitterInformation__email_user")
            ),
            SubmitterInformation__name=utils.safe_strip(
                d.get("SubmitterInformation__name")
            ),
            SubmitterInformation__organisation=utils.safe_strip(
                d.get("SubmitterInformation__organisation")
            ),
            OCRHabitatComposition__loose_rock_percent=utils.to_int_maybe(
                d.get("OCRHabitatComposition__loose_rock_percent")
            ),
            OCRHabitatComposition__habitat_notes=utils.safe_strip(
                d.get("OCRHabitatComposition__habitat_notes")
            ),
            OCRHabitatComposition__vegetation_condition=utils.safe_strip(
                d.get("OCRHabitatComposition__vegetation_condition")
            ),
            OCRHabitatComposition__soil_colour=utils.safe_strip(
                d.get("OCRHabitatComposition__soil_colour")
            ),
            OCRHabitatComposition__soil_condition=utils.safe_strip(
                d.get("OCRHabitatComposition__soil_condition")
            ),
            OCRHabitatComposition__land_form=utils.safe_strip(
                d.get("OCRHabitatComposition__land_form")
            ),
            OCRHabitatComposition__soil_type=utils.safe_strip(
                d.get("OCRHabitatComposition__soil_type")
            ),
            OCRIdentification__barcode_number=utils.safe_strip(
                d.get("OCRIdentification__barcode_number")
            ),
            OCRIdentification__collector_number=utils.safe_strip(
                d.get("OCRIdentification__collector_number")
            ),
            OCRIdentification__permit_id=utils.safe_strip(
                d.get("OCRIdentification__permit_id")
            ),
            OCRIdentification__identification_comment=utils.safe_strip(
                d.get("OCRIdentification__identification_comment")
            ),
            OCRIdentification__identification_certainty=utils.to_int_maybe(
                d.get("OCRIdentification__identification_certainty")
            ),
            OCRIdentification__sample_destination=utils.to_int_maybe(
                d.get("OCRIdentification__sample_destination")
            ),
            OCRLocation__coordinate_source=utils.to_int_maybe(
                d.get("OCRLocation__coordinate_source")
            ),
            OCRLocation__location_accuracy=utils.to_int_maybe(
                d.get("OCRLocation__location_accuracy")
            ),
            OCRLocation__district=utils.to_int_maybe(d.get("OCRLocation__district")),
            OCRLocation__region=utils.to_int_maybe(d.get("OCRLocation__region")),
            OCRLocation__locality=utils.safe_strip(d.get("OCRLocation__locality")),
            OCRLocation__location_description=utils.safe_strip(
                d.get("OCRLocation__location_description")
            ),
            OCRLocation__boundary_description=utils.safe_strip(
                d.get("OCRLocation__boundary_description")
            ),
            OCRLocation__epsg_code=utils.to_int_maybe(d.get("OCRLocation__epsg_code")),
            OCRObservationDetail__area_assessment=utils.to_int_maybe(
                d.get("OCRObservationDetail__area_assessment")
            ),
            OCRObservationDetail__area_surveyed=utils.safe_strip(
                d.get("OCRObservationDetail__area_surveyed")
            ),
            OCRObservationDetail__survey_duration=utils.to_int_maybe(
                d.get("OCRObservationDetail__survey_duration")
            ),
            OCRAssociatedSpecies__comment=utils.safe_strip(
                d.get("OCRAssociatedSpecies__comment")
            ),
            OCRPlantCount__counted_subject=utils.to_int_maybe(
                d.get("OCRPlantCount__counted_subject")
            ),
            OCRPlantCount__plant_condition=utils.to_int_maybe(
                d.get("OCRPlantCount__plant_condition")
            ),
            OCRPlantCount__plant_count_method=utils.to_int_maybe(
                d.get("OCRPlantCount__plant_count_method")
            ),
            OCRPlantCount__clonal_reproduction_present=d.get(
                "OCRPlantCount__clonal_reproduction_present"
            ),
            OCRPlantCount__comment=utils.safe_strip(d.get("OCRPlantCount__comment")),
            OCRPlantCount__count_status=utils.safe_strip(
                d.get("OCRPlantCount__count_status")
            ),
            OCRPlantCount__dehisced_fruit_present=d.get(
                "OCRPlantCount__dehisced_fruit_present"
            ),
            OCRPlantCount__detailed_alive_juvenile=utils.to_int_maybe(
                d.get("OCRPlantCount__detailed_alive_juvenile")
            ),
            OCRPlantCount__detailed_alive_mature=utils.to_int_maybe(
                d.get("OCRPlantCount__detailed_alive_mature")
            ),
            OCRPlantCount__detailed_alive_seedling=utils.to_int_maybe(
                d.get("OCRPlantCount__detailed_alive_seedling")
            ),
            OCRPlantCount__detailed_dead_juvenile=utils.to_int_maybe(
                d.get("OCRPlantCount__detailed_dead_juvenile")
            ),
            OCRPlantCount__detailed_dead_mature=utils.to_int_maybe(
                d.get("OCRPlantCount__detailed_dead_mature")
            ),
            OCRPlantCount__detailed_dead_seedling=utils.to_int_maybe(
                d.get("OCRPlantCount__detailed_dead_seedling")
            ),
            OCRPlantCount__estimated_population_area=utils.to_decimal_maybe(
                d.get("OCRPlantCount__estimated_population_area")
            ),
            OCRPlantCount__flower_bud_present=d.get(
                "OCRPlantCount__flower_bud_present"
            ),
            OCRPlantCount__flower_present=d.get("OCRPlantCount__flower_present"),
            OCRPlantCount__flowering_plants_per=utils.to_decimal_maybe(
                d.get("OCRPlantCount__flowering_plants_per")
            ),
            OCRPlantCount__immature_fruit_present=d.get(
                "OCRPlantCount__immature_fruit_present"
            ),
            OCRPlantCount__pollinator_observation=utils.safe_strip(
                d.get("OCRPlantCount__pollinator_observation")
            ),
            OCRPlantCount__quadrats_surveyed=utils.to_int_maybe(
                d.get("OCRPlantCount__quadrats_surveyed")
            ),
            OCRPlantCount__ripe_fruit_present=d.get(
                "OCRPlantCount__ripe_fruit_present"
            ),
            OCRPlantCount__simple_alive=utils.to_int_maybe(
                d.get("OCRPlantCount__simple_alive")
            ),
            OCRPlantCount__simple_dead=utils.to_int_maybe(
                d.get("OCRPlantCount__simple_dead")
            ),
            OCRPlantCount__total_quadrat_area=utils.to_decimal_maybe(
                d.get("OCRPlantCount__total_quadrat_area")
            ),
            OCRPlantCount__vegetative_state_present=d.get(
                "OCRPlantCount__vegetative_state_present"
            ),
            OCRPlantCount__obs_date=obs_date,
            OCRVegetationStructure__vegetation_structure_layer_one=utils.safe_strip(
                d.get("OCRVegetationStructure__vegetation_structure_layer_one")
            ),
            OCRVegetationStructure__vegetation_structure_layer_two=utils.safe_strip(
                d.get("OCRVegetationStructure__vegetation_structure_layer_two")
            ),
            OCRVegetationStructure__vegetation_structure_layer_three=utils.safe_strip(
                d.get("OCRVegetationStructure__vegetation_structure_layer_three")
            ),
            OCRVegetationStructure__vegetation_structure_layer_four=utils.safe_strip(
                d.get("OCRVegetationStructure__vegetation_structure_layer_four")
            ),
        )

    def validate(self, source: str | None = None) -> list[tuple[str, str]]:
        """
        Return list of (level, message). Basic business rules enforced here.
        """
        issues: list[tuple[str, str]] = []

        if not self.migrated_from_id:
            issues.append(("error", "migrated_from_id is required"))

        if not self.processing_status:
            issues.append(("error", "processing_status is required"))

        if self.group_type_id is not None:
            # for flora/fauna require species; for community require community
            if str(self.group_type_id).lower() in [
                GroupType.GROUP_TYPE_FLORA,
                GroupType.GROUP_TYPE_FAUNA,
            ]:
                if not self.species_id:
                    issues.append(("error", "species_id is required for flora/fauna"))
            elif (
                str(self.group_type_id).lower()
                == str(GroupType.GROUP_TYPE_COMMUNITY).lower()
            ):
                if not self.community_id:
                    issues.append(("error", "community_id is required for community"))

        # source-specific examples could be added here using `source`
        return issues

    def to_model_defaults(self) -> dict:
        """
        Return dict ready for ORM update/create defaults.
        """
        return {
            "group_type_id": self.group_type_id,
            "species_id": self.species_id,
            "community_id": self.community_id,
            "processing_status": self.processing_status,
            "customer_status": self.customer_status,
            "observation_date": self.observation_date,
            "record_source": self.record_source,
            "comments": self.comments or "",
            "ocr_for_occ_number": self.ocr_for_occ_number or "",
            "ocr_for_occ_name": self.ocr_for_occ_name or "",
            "assessor_data": self.assessor_data or "",
            "reported_date": self.reported_date,
            "lodgement_date": self.lodgement_date,
            "approved_by": self.approved_by,
            "submitter": self.submitter,
            "occurrence": self.Occurrence__migrated_from_id,
        }
