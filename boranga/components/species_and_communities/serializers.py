import logging

from rest_framework import serializers

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.conservation_status.serializers import (
    BasicConservationStatusSerializer,
)
from boranga.components.main.serializers import (
    BaseModelSerializer,
    BaseSerializer,
    CommunicationLogEntrySerializer,
    EmailUserSerializer,
    ListMultipleChoiceField,
    SafeFileUrlField,
)
from boranga.components.species_and_communities.models import (
    Community,
    CommunityConservationAttributes,
    CommunityDistribution,
    CommunityDocument,
    CommunityLogEntry,
    CommunityPublishingStatus,
    CommunityTaxonomy,
    CommunityUserAction,
    ConservationThreat,
    District,
    InformalGroup,
    Region,
    Species,
    SpeciesConservationAttributes,
    SpeciesDistribution,
    SpeciesDocument,
    SpeciesLogEntry,
    SpeciesPublishingStatus,
    SpeciesUserAction,
    Taxonomy,
)
from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_internal,
    is_occurrence_approver,
    is_occurrence_assessor,
    is_species_communities_approver,
)
from boranga.ledger_api_utils import retrieve_email_user

logger = logging.getLogger("boranga")


class ListSpeciesSerializer(BaseModelSerializer):
    group_type = serializers.SerializerMethodField()
    scientific_name = serializers.SerializerMethodField()
    common_name = serializers.SerializerMethodField()
    family = serializers.SerializerMethodField()
    genus = serializers.SerializerMethodField()
    informal_group = serializers.SerializerMethodField()
    regions = serializers.SerializerMethodField()
    districts = serializers.SerializerMethodField()
    processing_status = serializers.CharField(source="get_processing_status_display")
    user_process = serializers.SerializerMethodField(read_only=True)
    can_user_edit = serializers.SerializerMethodField()
    publishing_status = serializers.SerializerMethodField()
    wa_legislative_list = serializers.SerializerMethodField()
    wa_legislative_category = serializers.SerializerMethodField()
    wa_priority_category = serializers.SerializerMethodField()
    commonwealth_conservation_category = serializers.SerializerMethodField()
    other_conservation_assessment = serializers.SerializerMethodField()
    conservation_criteria = serializers.SerializerMethodField()
    fauna_group_name = serializers.CharField(
        source="fauna_group.name", read_only=True, required=False, allow_null=True
    )
    fauna_sub_group_name = serializers.CharField(
        source="fauna_sub_group.name", read_only=True, required=False, allow_null=True
    )

    class Meta:
        model = Species
        fields = (
            "id",
            "species_number",
            "group_type",
            "scientific_name",
            "common_name",
            "family",
            "genus",
            "informal_group",
            "regions",
            "districts",
            "processing_status",
            "can_user_edit",
            "can_user_view",
            "user_process",
            "comment",
            "publishing_status",
            "wa_legislative_list",
            "wa_legislative_category",
            "wa_priority_category",
            "commonwealth_conservation_category",
            "other_conservation_assessment",
            "conservation_criteria",
            "fauna_group",
            "fauna_group_name",
            "fauna_sub_group",
            "fauna_sub_group_name",
        )
        datatables_always_serialize = (
            "id",
            "species_number",
            "group_type",
            "scientific_name",
            "common_name",
            "family",
            "genus",
            "informal_group",
            "regions",
            "districts",
            "processing_status",
            "can_user_edit",
            "can_user_view",
            "user_process",
            "comment",
            "publishing_status",
            "wa_legislative_list",
            "wa_legislative_category",
            "wa_priority_category",
            "commonwealth_conservation_category",
            "other_conservation_assessment",
            "conservation_criteria",
            "fauna_group",
            "fauna_group_name",
            "fauna_sub_group",
            "fauna_sub_group_name",
        )

    def get_group_type(self, obj):
        return obj.group_type.name

    def get_scientific_name(self, obj):
        if obj.taxonomy:
            return obj.taxonomy.scientific_name
        return ""

    def get_common_name(self, obj):
        if not obj.taxonomy or not obj.taxonomy.vernaculars:
            return ""
        return ", ".join(
            str(vn.vernacular_name) for vn in obj.taxonomy.vernaculars.all()
        )

    def get_family(self, obj):
        if obj.taxonomy:
            if obj.taxonomy.family_id:
                return obj.taxonomy.family_name
        return ""

    def get_genus(self, obj):
        if obj.taxonomy:
            if obj.taxonomy.genera_id:
                return obj.taxonomy.genera_name
        return ""

    def get_informal_group(self, obj):
        if obj.taxonomy:
            if obj.taxonomy.informal_groups:
                return obj.taxonomy.informal_groups.all().values_list(
                    "classification_system_fk_id__class_desc", flat=True
                )
        return ""

    def get_regions(self, obj):
        if obj.regions:
            regions_list = obj.regions.all().values_list("name", flat=True)
            return ", ".join(regions_list)
        return ""

    def get_districts(self, obj):
        if obj.districts:
            districts_list = obj.districts.all().values_list("name", flat=True)
            return ", ".join(districts_list)
        return ""

    def get_user_process(self, obj):
        # Check if currently logged in user has access to process the Species
        request = self.context["request"]
        return obj.can_user_action and is_species_communities_approver(request)

    def get_can_user_edit(self, obj):
        request = self.context["request"]
        if not is_species_communities_approver(request):
            return False
        return obj.can_user_edit

    def get_publishing_status(self, obj):
        if not hasattr(obj, "species_publishing_status"):
            SpeciesPublishingStatus.objects.create(species=obj)
        return SpeciesPublishingStatusSerializer(obj.species_publishing_status).data

    def get_wa_legislative_list(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.wa_legislative_list:
            return conservation_status.wa_legislative_list.code
        return ""

    def get_wa_legislative_category(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.wa_legislative_category:
            return conservation_status.wa_legislative_category.code
        return ""

    def get_wa_priority_category(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.wa_priority_category:
            return conservation_status.wa_priority_category.code
        return ""

    def get_commonwealth_conservation_category(self, obj):
        conservation_status = obj.approved_conservation_status
        if (
            conservation_status
            and conservation_status.commonwealth_conservation_category
        ):
            return conservation_status.commonwealth_conservation_category.code
        return ""

    def get_other_conservation_assessment(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.other_conservation_assessment:
            return conservation_status.other_conservation_assessment.code
        return ""

    def get_conservation_criteria(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.conservation_criteria:
            return conservation_status.conservation_criteria
        return ""


class ListCommunitiesSerializer(BaseModelSerializer):
    group_type = serializers.SerializerMethodField()
    community_migrated_id = serializers.SerializerMethodField()
    community_name = serializers.SerializerMethodField()
    regions = serializers.SerializerMethodField()
    districts = serializers.SerializerMethodField()
    processing_status = serializers.CharField(source="get_processing_status_display")
    user_process = serializers.SerializerMethodField(read_only=True)
    can_user_edit = serializers.SerializerMethodField()
    publishing_status = serializers.SerializerMethodField()
    wa_legislative_list = serializers.SerializerMethodField()
    wa_legislative_category = serializers.SerializerMethodField()
    wa_priority_category = serializers.SerializerMethodField()
    commonwealth_conservation_category = serializers.SerializerMethodField()
    other_conservation_assessment = serializers.SerializerMethodField()
    conservation_criteria = serializers.SerializerMethodField()

    class Meta:
        model = Community
        fields = (
            "id",
            "community_number",
            "group_type",
            "community_migrated_id",
            "community_name",
            "regions",
            "districts",
            "processing_status",
            "can_user_edit",
            "can_user_view",
            "user_process",
            "comment",
            "publishing_status",
            "wa_legislative_list",
            "wa_legislative_category",
            "wa_priority_category",
            "commonwealth_conservation_category",
            "other_conservation_assessment",
            "conservation_criteria",
        )
        datatables_always_serialize = (
            "id",
            "community_number",
            "group_type",
            "community_migrated_id",
            "community_name",
            "regions",
            "districts",
            "processing_status",
            "can_user_edit",
            "can_user_view",
            "user_process",
            "comment",
            "publishing_status",
            "wa_legislative_list",
            "wa_legislative_category",
            "wa_priority_category",
            "commonwealth_conservation_category",
            "other_conservation_assessment",
            "conservation_criteria",
        )

    def get_group_type(self, obj):
        return obj.group_type.name

    def get_community_name(self, obj):
        try:
            taxonomy = CommunityTaxonomy.objects.get(community=obj)
            return taxonomy.community_name
        except CommunityTaxonomy.DoesNotExist:
            return ""

    def get_community_migrated_id(self, obj):
        if not obj.taxonomy:
            return ""
        return obj.taxonomy.community_migrated_id

    def get_regions(self, obj):
        if obj.regions:
            regions_list = obj.regions.all().values_list("name", flat=True)
            return ", ".join(regions_list)
        return ""

    def get_districts(self, obj):
        if obj.districts:
            districts_list = obj.districts.all().values_list("name", flat=True)
            return ", ".join(districts_list)
        return ""

    def get_user_process(self, obj):
        # Check if currently logged in user has access to process the Community
        request = self.context["request"]
        return obj.can_user_action and is_species_communities_approver(request)

    def get_can_user_edit(self, obj):
        request = self.context["request"]
        if not is_species_communities_approver(request):
            return False
        return obj.can_user_edit

    def get_publishing_status(self, obj):
        if not hasattr(obj, "community_publishing_status"):
            CommunityPublishingStatus.objects.create(community=obj)
        return CommunityPublishingStatusSerializer(obj.community_publishing_status).data

    def get_wa_legislative_list(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.wa_legislative_list:
            return conservation_status.wa_legislative_list.code
        return ""

    def get_wa_legislative_category(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.wa_legislative_category:
            return conservation_status.wa_legislative_category.code
        return ""

    def get_wa_priority_category(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.wa_priority_category:
            return conservation_status.wa_priority_category.code
        return ""

    def get_commonwealth_conservation_category(self, obj):
        conservation_status = obj.approved_conservation_status
        if (
            conservation_status
            and conservation_status.commonwealth_conservation_category
        ):
            return conservation_status.commonwealth_conservation_category.code
        return ""

    def get_other_conservation_assessment(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.other_conservation_assessment:
            return conservation_status.other_conservation_assessment.code
        return ""

    def get_conservation_criteria(self, obj):
        conservation_status = obj.approved_conservation_status
        if conservation_status and conservation_status.conservation_criteria:
            return conservation_status.conservation_criteria
        return ""


class TaxonomySerializer(BaseModelSerializer):
    # text is added as need for select2 format
    text = serializers.SerializerMethodField()
    common_name = serializers.SerializerMethodField()
    informal_group = serializers.SerializerMethodField()
    conservation_status = serializers.SerializerMethodField()
    species_id = serializers.SerializerMethodField()
    common_names_list = serializers.SerializerMethodField()

    class Meta:
        model = Taxonomy
        fields = (
            "id",
            "text",
            "taxon_name_id",
            "scientific_name",
            "kingdom_name",
            # need to fetch common name in multiple select
            "common_name",
            "common_names_list",
            "taxon_previous_name",
            "informal_group",
            "name_authority",
            "name_comments",
            "conservation_status",
            "family_name",
            "genera_name",
            "species_id",
        )

    def get_species_id(self, obj):
        if hasattr(obj, "species"):
            return obj.species.id

    def get_text(self, obj):
        scientific_name = obj.scientific_name
        if self.context.get("has_duplicates", False):
            parts = []
            if obj.family_name:
                parts.append(obj.family_name)
            if obj.kingdom_name:
                parts.append(obj.kingdom_name)
            parts.append(f"NOMOS: {obj.taxon_name_id}")
            scientific_name = f"{obj.scientific_name} ({', '.join(parts)})"
        scientific_name = scientific_name.replace("&amp;", "&")

        # append custom marker for taxonomies that are previous names (annotated in the view)
        if getattr(obj, "is_previous", False):
            scientific_name = f"{scientific_name} (non-current)"

        return scientific_name

    def get_common_name(self, obj):
        if not obj.vernaculars:
            return ""
        return ", ".join(str(vn.vernacular_name) for vn in obj.vernaculars.all())

    def get_common_names_list(self, obj):
        if not obj.vernaculars:
            return ""

        return obj.vernaculars.values_list("vernacular_name", flat=True)

    def get_informal_group(self, obj):
        try:
            if obj.informal_groups:
                # informal_groups = InformalGroup.objects.get(taxonomy=obj.id)
                informal_groups = InformalGroup.objects.filter(
                    taxonomy_id=obj.id
                ).values_list("classification_system_fk__class_desc", flat=True)
                return ", ".join(informal_groups)
        except InformalGroup.DoesNotExist:
            return ""

    def get_conservation_status(self, obj):
        request = self.context["request"]
        if not is_internal(request):
            return None
        try:
            taxonSpecies = Species.objects.get(taxonomy=obj)
            qs = ConservationStatus.objects.get(
                species=taxonSpecies,
                processing_status="approved",
            )
            return BasicConservationStatusSerializer(qs, context=self.context).data
        except (ConservationStatus.DoesNotExist, Species.DoesNotExist):
            return BasicConservationStatusSerializer(context=self.context).data


class CommonNameTaxonomySerializer(TaxonomySerializer):
    def get_text(self, obj):
        return super().get_common_name(obj)


class SpeciesConservationAttributesSerializer(BaseModelSerializer):

    class Meta:
        model = SpeciesConservationAttributes
        fields = (
            "id",
            "species_id",
            # flora related attributes
            "flowering_period",
            "fruiting_period",
            "flora_recruitment_type_id",
            "flora_recruitment_notes",
            "seed_viability_germination_info",
            "root_morphology_id",
            "pollinator_information",
            "hydrology",
            "response_to_dieback",
            # fauna related attributes
            "breeding_period",
            "fauna_breeding",
            "fauna_reproductive_capacity",
            "diet_and_food_source",
            "home_range",
            # common attributes
            "habitat_growth_form",
            "time_to_maturity_from",
            "time_to_maturity_to",
            "time_to_maturity_choice",
            "generation_length_from",
            "generation_length_to",
            "generation_length_choice",
            "average_lifespan_from",
            "average_lifespan_to",
            "average_lifespan_choice",
            "minimum_fire_interval_from",
            "minimum_fire_interval_to",
            "minimum_fire_interval_choice",
            "response_to_fire",
            "post_fire_habitat_interaction_id",
            "habitat",
            "research_requirements",
            "other_relevant_diseases",
        )

        def __init__(self, *args, **kwargs):
            super(SpeciesConservationAttributesSerializer, self).__init__(
                *args, **kwargs
            )
            PERIOD_CHOICES = []
            for rs in SpeciesConservationAttributes.PERIOD_CHOICES:
                PERIOD_CHOICES.append([rs[0], rs[1]])
            self.fields["flowering_period", "fruiting_period", "breeding_period"] = (
                ListMultipleChoiceField(choices=PERIOD_CHOICES, allow_blank=False)
            )


class SaveSpeciesConservationAttributesSerializer(BaseModelSerializer):
    species_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    flowering_period = ListMultipleChoiceField(
        choices=SpeciesConservationAttributes.PERIOD_CHOICES,
        allow_null=True,
        allow_blank=True,
        required=False,
    )
    fruiting_period = ListMultipleChoiceField(
        choices=SpeciesConservationAttributes.PERIOD_CHOICES,
        allow_null=True,
        allow_blank=True,
        required=False,
    )
    flora_recruitment_type_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    root_morphology_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    breeding_period = ListMultipleChoiceField(
        choices=SpeciesConservationAttributes.PERIOD_CHOICES,
        allow_null=True,
        allow_blank=True,
        required=False,
    )
    post_fire_habitat_interaction_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )

    class Meta:
        model = SpeciesConservationAttributes
        fields = (
            "id",
            "species_id",
            # flora related attributes
            "flowering_period",
            "fruiting_period",
            "flora_recruitment_type_id",
            "flora_recruitment_notes",
            "seed_viability_germination_info",
            "root_morphology_id",
            "pollinator_information",
            "hydrology",
            "response_to_dieback",
            # fauna related attributes
            "breeding_period",
            "fauna_breeding",
            "fauna_reproductive_capacity",
            "diet_and_food_source",
            "home_range",
            # common attributes
            "habitat_growth_form",
            "time_to_maturity_from",
            "time_to_maturity_to",
            "time_to_maturity_choice",
            "generation_length_from",
            "generation_length_to",
            "generation_length_choice",
            "average_lifespan_from",
            "average_lifespan_to",
            "average_lifespan_choice",
            "minimum_fire_interval_from",
            "minimum_fire_interval_to",
            "minimum_fire_interval_choice",
            "response_to_fire",
            "post_fire_habitat_interaction_id",
            "habitat",
            "research_requirements",
            "other_relevant_diseases",
        )


class SpeciesDistributionSerializer(BaseModelSerializer):
    extent_of_occurrences = serializers.FloatField(required=False, allow_null=True)

    class Meta:
        model = SpeciesDistribution
        fields = (
            "number_of_occurrences",
            "noo_auto",
            "extent_of_occurrences",
            "eoo_auto",
            "area_of_occupancy",
            "area_of_occupancy_actual",
            "aoo_actual_auto",
            "number_of_iucn_locations",
            "number_of_iucn_subpopulations",
            "distribution",
        )

    def to_internal_value(self, data):
        if data["extent_of_occurrences"] == "":
            data["extent_of_occurrences"] = None
        return super().to_internal_value(data)


class SaveSpeciesDistributionSerializer(BaseModelSerializer):
    species_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    extent_of_occurrences = serializers.FloatField(required=False, allow_null=True)

    class Meta:
        model = SpeciesDistribution
        fields = (
            "species_id",
            "number_of_occurrences",
            "noo_auto",
            "extent_of_occurrences",
            "eoo_auto",
            "area_of_occupancy",
            "area_of_occupancy_actual",
            "aoo_actual_auto",
            "number_of_iucn_locations",
            "number_of_iucn_subpopulations",
            "distribution",
        )


class SpeciesPublishingStatusSerializer(BaseModelSerializer):
    public_status = serializers.SerializerMethodField()

    class Meta:
        model = SpeciesPublishingStatus
        fields = (
            "public_status",
            "species_id",
            "species_public",
            "distribution_public",
            "conservation_status_public",
            "conservation_attributes_public",
            "threats_public",
        )

    def get_public_status(self, obj):
        if obj.species_public:
            return "Public"
        return "Private"


class SaveSpeciesPublishingStatusSerializer(BaseModelSerializer):

    species_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    public_status = serializers.SerializerMethodField()

    class Meta:
        model = SpeciesPublishingStatus
        fields = (
            "public_status",
            "species_id",
            "species_public",
            "distribution_public",
            "conservation_status_public",
            "conservation_attributes_public",
            "threats_public",
        )

    def get_public_status(self, obj):
        if obj.species_public:
            return "Public"
        return "Private"


class BaseSpeciesSerializer(BaseModelSerializer):
    readonly = serializers.SerializerMethodField(read_only=True)
    group_type = serializers.SerializerMethodField(read_only=True)
    conservation_status = serializers.SerializerMethodField()
    taxonomy_details = serializers.SerializerMethodField()
    conservation_attributes = serializers.SerializerMethodField()
    distribution = serializers.SerializerMethodField()
    publishing_status = serializers.SerializerMethodField()
    image_doc = serializers.SerializerMethodField()
    # regions = serializers.SerializerMethodField()

    class Meta:
        model = Species
        fields = (
            "id",
            "migrated_from_id",
            "species_number",
            "group_type",
            "group_type_id",
            "taxonomy_id",
            "conservation_status",
            "taxonomy_details",
            "conservation_attributes",
            "distribution",
            "regions",
            "districts",
            "last_data_curation_date",
            "image_doc",
            "processing_status",
            "applicant",
            "submitter",
            "lodgement_date",
            "readonly",
            "can_user_edit",
            "can_user_view",
            "applicant_details",
            "comment",
            "publishing_status",
        )

    def get_readonly(self, obj):
        return False

    def get_group_type(self, obj):
        return obj.group_type.name

    def get_regions(self, obj):
        return [r.id for r in obj.regions.all()]

    def get_districts(self, obj):
        return [d.id for d in obj.districts.all()]

    def get_taxonomy_details(self, obj):
        if not obj.taxonomy:
            return None

        return TaxonomySerializer(
            obj.taxonomy, context={"request": self.context["request"]}
        ).data

    def get_conservation_status(self, obj):
        request = self.context["request"]
        if is_internal(request) or (
            obj.species_publishing_status.species_public
            and obj.species_publishing_status.conservation_status_public
        ):
            try:
                qs = ConservationStatus.objects.get(
                    species=obj,
                    processing_status="approved",
                )
                return BasicConservationStatusSerializer(qs, context=self.context).data
            except ConservationStatus.DoesNotExist:
                return None
        else:
            return None

    def get_conservation_attributes(self, obj):
        request = self.context["request"]
        if is_internal(request) or (
            obj.species_publishing_status.species_public
            and obj.species_publishing_status.conservation_attributes_public
        ):
            try:
                qs = SpeciesConservationAttributes.objects.get(species=obj)
                return SpeciesConservationAttributesSerializer(qs).data
            except SpeciesConservationAttributes.DoesNotExist:
                return SpeciesConservationAttributesSerializer().data
        else:
            return None

    def get_distribution(self, obj):
        request = self.context["request"]
        if is_internal(request) or (
            obj.species_publishing_status.species_public
            and obj.species_publishing_status.distribution_public
        ):
            try:
                # to create the distribution instance for fetching the calculated values from serializer
                distribution_instance, created = (
                    SpeciesDistribution.objects.get_or_create(species=obj)
                )
                return SpeciesDistributionSerializer(distribution_instance).data
            except SpeciesDistribution.DoesNotExist:
                return SpeciesDistributionSerializer().data
        else:
            return None

    def get_publishing_status(self, obj):
        if not hasattr(obj, "species_publishing_status"):
            SpeciesPublishingStatus.objects.create(species=obj)
        return SpeciesPublishingStatusSerializer(obj.species_publishing_status).data

    def get_image_doc(self, obj):
        if obj.image_doc and obj.image_doc._file:
            return obj.image_doc._file.url
        return None

    def get_processing_status(self, obj):
        return obj.get_processing_status_display()


class SpeciesSerializer(BaseSpeciesSerializer):
    scientific_name = serializers.SerializerMethodField()
    processing_status = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Species
        fields = (
            "id",
            "species_number",
            "scientific_name",
            "group_type",
            "group_type_id",
            "taxonomy_id",
            "conservation_status",
            "taxonomy_details",
            "conservation_attributes",
            "distribution",
            "regions",
            "districts",
            "image_doc",
            "processing_status",
            # "readonly",
            # "can_user_edit",
            # "can_user_view",
            "comment",
            "publishing_status",
        )

    def get_readonly(self, obj):
        return obj.can_user_view

    def get_submitter(self, obj):
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return None

    def get_scientific_name(self, obj):
        if obj.taxonomy:
            return obj.taxonomy.scientific_name
        return ""


class InternalSpeciesSerializer(BaseSpeciesSerializer):
    submitter = serializers.SerializerMethodField(read_only=True)
    processing_status = serializers.SerializerMethodField(read_only=True)
    current_assessor = serializers.SerializerMethodField()
    user_edit_mode = serializers.SerializerMethodField()
    can_user_edit = serializers.SerializerMethodField()
    can_add_log = serializers.SerializerMethodField()
    fauna_group_name = serializers.CharField(
        source="fauna_group.name", read_only=True, required=False, allow_null=True
    )
    fauna_sub_group_name = serializers.CharField(
        source="fauna_sub_group.name", read_only=True, required=False, allow_null=True
    )

    class Meta:
        model = Species
        fields = (
            "id",
            "migrated_from_id",
            "species_number",
            "group_type",
            "group_type_id",
            "taxonomy_id",
            "conservation_status",
            "taxonomy_details",
            "conservation_attributes",
            "distribution",
            "publishing_status",
            "regions",
            "districts",
            "last_data_curation_date",
            "image_doc",
            "processing_status",
            "readonly",
            "can_user_edit",
            "can_user_view",
            "submitter",
            "lodgement_date",
            "current_assessor",
            "user_edit_mode",
            "comment",
            "conservation_plan_exists",
            "conservation_plan_reference",
            "comment",
            "occurrence_count",
            "area_of_occupancy_km2",
            "area_occurrence_convex_hull_km2",
            "can_add_log",
            "department_file_numbers",
            "fauna_group",
            "fauna_group_name",
            "fauna_sub_group",
            "fauna_sub_group_name",
        )

    def get_submitter(self, obj):
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return None

    def get_readonly(self, obj):
        request = self.context["request"]
        return not (
            (obj.can_user_edit or obj.has_user_edit_mode(request))
            and is_species_communities_approver(request)
        )

    def get_can_add_log(self, obj):
        request = self.context["request"]
        return (
            is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
        )

    def get_can_user_edit(self, obj):
        request = self.context["request"]
        if is_species_communities_approver(request):
            return obj.can_user_edit
        return False

    def get_current_assessor(self, obj):
        return {
            "id": self.context["request"].user.id,
            "name": self.context["request"].user.get_full_name(),
            "email": self.context["request"].user.email,
        }

    def get_user_edit_mode(self, obj):
        request = self.context["request"]
        return obj.has_user_edit_mode(request)


class CommunityDistributionSerializer(BaseModelSerializer):
    extent_of_occurrences = serializers.FloatField(required=False, allow_null=True)
    community_original_area = serializers.FloatField(required=False, allow_null=True)

    class Meta:
        model = CommunityDistribution
        fields = (
            "number_of_occurrences",
            "noo_auto",
            "extent_of_occurrences",
            "eoo_auto",
            "area_of_occupancy",
            "area_of_occupancy_actual",
            "aoo_actual_auto",
            "number_of_iucn_locations",
            "community_original_area",
            "community_original_area_accuracy",
            "community_original_area_reference",
            "distribution",
        )

    def to_internal_value(self, data):
        if data["extent_of_occurrences"] == "":
            data["extent_of_occurrences"] = None
        if data["community_original_area"] == "":
            data["community_original_area"] = None
        return super().to_internal_value(data)


class SaveCommunityDistributionSerializer(BaseModelSerializer):
    community_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    area_of_occupancy = serializers.IntegerField(
        required=False,
        allow_null=True,
    )
    extent_of_occurrences = serializers.FloatField(
        required=False,
        allow_null=True,
    )
    community_original_area = serializers.FloatField(required=False, allow_null=True)

    class Meta:
        model = CommunityDistribution
        fields = (
            "community_id",
            "number_of_occurrences",
            "noo_auto",
            "extent_of_occurrences",
            "eoo_auto",
            "area_of_occupancy",
            "area_of_occupancy_actual",
            "aoo_actual_auto",
            "number_of_iucn_locations",
            "community_original_area",
            "community_original_area_accuracy",
            "community_original_area_reference",
            "distribution",
        )


class CommunityConservationAttributesSerializer(BaseModelSerializer):
    class Meta:
        model = CommunityConservationAttributes
        fields = (
            "id",
            "community_id",
            # 'habitat_growth_form',
            "pollinator_information",
            "minimum_fire_interval_from",
            "minimum_fire_interval_to",
            "minimum_fire_interval_choice",
            "response_to_fire",
            "post_fire_habitat_interaction_id",
            "hydrology",
            "ecological_and_biological_information",
            "research_requirements",
            "response_to_dieback",
            "other_relevant_diseases",
        )


class SaveCommunityConservationAttributesSerializer(BaseModelSerializer):
    community_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    post_fire_habitat_interaction_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )

    class Meta:
        model = CommunityConservationAttributes
        fields = (
            "id",
            "community_id",
            # 'habitat_growth_form',
            "pollinator_information",
            "minimum_fire_interval_from",
            "minimum_fire_interval_to",
            "minimum_fire_interval_choice",
            "response_to_fire",
            "post_fire_habitat_interaction_id",
            "hydrology",
            "ecological_and_biological_information",
            "research_requirements",
            "response_to_dieback",
            "other_relevant_diseases",
        )


class CommunityTaxonomySerializer(BaseModelSerializer):
    text = serializers.SerializerMethodField()

    class Meta:
        model = CommunityTaxonomy
        fields = (
            "id",
            "text",
            "community_id",
            "community_migrated_id",
            "community_name",
            "community_description",
            "previous_name",
            "name_authority",
            "name_comments",
        )

    def get_text(self, obj):
        return obj.community_name


class SaveCommunityTaxonomySerializer(BaseModelSerializer):
    community_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )

    class Meta:
        model = CommunityTaxonomy
        fields = (
            "id",
            "community_id",
            "community_migrated_id",
            "community_name",
            "community_description",
            "previous_name",
            "name_authority",
            "name_comments",
        )


class CommunityPublishingStatusSerializer(BaseModelSerializer):
    public_status = serializers.SerializerMethodField()

    class Meta:
        model = CommunityPublishingStatus
        fields = (
            "public_status",
            "community_id",
            "community_public",
            "distribution_public",
            "conservation_status_public",
            "conservation_attributes_public",
            "threats_public",
        )

    def get_public_status(self, obj):
        if obj.community_public:
            return "Public"
        return "Private"


class SaveCommunityPublishingStatusSerializer(BaseModelSerializer):

    community_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    public_status = serializers.SerializerMethodField()

    class Meta:
        model = CommunityPublishingStatus
        fields = (
            "public_status",
            "community_id",
            "community_public",
            "distribution_public",
            "conservation_status_public",
            "conservation_attributes_public",
            "threats_public",
        )

    def get_public_status(self, obj):
        if obj.community_public:
            return "Public"
        return "Private"


class BaseCommunitySerializer(BaseModelSerializer):
    species = serializers.SerializerMethodField()
    group_type = serializers.SerializerMethodField(read_only=True)
    taxonomy_details = serializers.SerializerMethodField()
    conservation_status = serializers.SerializerMethodField()
    distribution = serializers.SerializerMethodField()
    publishing_status = serializers.SerializerMethodField()
    conservation_attributes = serializers.SerializerMethodField()
    readonly = serializers.SerializerMethodField(read_only=True)
    image_doc = serializers.SerializerMethodField()

    class Meta:
        model = Community
        fields = (
            "id",
            "migrated_from_id",
            "community_number",
            "species",
            "group_type",
            "taxonomy_details",
            "conservation_status",
            "distribution",
            "publishing_status",
            "conservation_attributes",
            "last_data_curation_date",
            "image_doc",
            "processing_status",
            "lodgement_date",
            "submitter",
            "readonly",
            "can_user_edit",
            "can_user_view",
            "applicant_details",
            "comment",
            "regions",
            "districts",
        )

    def get_species(self, obj):
        return [s.id for s in obj.species.all()]

    def get_readonly(self, obj):
        request = self.context["request"]
        return not (
            (obj.can_user_edit or obj.has_user_edit_mode(request))
            and is_species_communities_approver(request)
        )

    def get_group_type(self, obj):
        return obj.group_type.name

    def get_regions(self, obj):
        return [r.id for r in obj.regions.all()]

    def get_districts(self, obj):
        return [d.id for d in obj.districts.all()]

    def get_taxonomy_details(self, obj):
        try:
            taxonomy_instance, created = CommunityTaxonomy.objects.get_or_create(
                community=obj
            )
            return CommunityTaxonomySerializer(taxonomy_instance).data
        except CommunityTaxonomy.MultipleObjectsReturned:
            qs = None
        return CommunityTaxonomySerializer(qs).data

    def get_conservation_status(self, obj):
        request = self.context["request"]
        if is_internal(request) or (
            obj.community_publishing_status.community_public
            and obj.community_publishing_status.conservation_status_public
        ):
            try:
                qs = ConservationStatus.objects.get(
                    community=obj,
                    processing_status="approved",
                )
                return BasicConservationStatusSerializer(qs, context=self.context).data
            except ConservationStatus.DoesNotExist:
                return None
        else:
            return None

    def get_distribution(self, obj):
        request = self.context["request"]
        if is_internal(request) or (
            obj.community_publishing_status.community_public
            and obj.community_publishing_status.distribution_public
        ):
            try:
                # to create the distribution instance for fetching the calculated values from serializer
                distribution_instance, created = (
                    CommunityDistribution.objects.get_or_create(community=obj)
                )
                return CommunityDistributionSerializer(distribution_instance).data
            except CommunityDistribution.MultipleObjectsReturned:
                qs = None
            return CommunityDistributionSerializer(qs).data
        else:
            return None

    def get_publishing_status(self, obj):
        if not hasattr(obj, "community_publishing_status"):
            CommunityPublishingStatus.objects.create(community=obj)
        return CommunityPublishingStatusSerializer(obj.community_publishing_status).data

    def get_conservation_attributes(self, obj):
        request = self.context["request"]
        if is_internal(request) or (
            obj.community_publishing_status.community_public
            and obj.community_publishing_status.conservation_attributes_public
        ):
            try:
                qs = CommunityConservationAttributes.objects.get(community=obj)
                return CommunityConservationAttributesSerializer(qs).data
            except CommunityConservationAttributes.DoesNotExist:
                return CommunityConservationAttributesSerializer().data
        else:
            return None

    def get_can_user_edit(self, obj):
        return True

    def get_submitter(self, obj):
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data

        else:
            return None

    def get_image_doc(self, obj):
        if obj.image_doc and obj.image_doc._file:
            return obj.image_doc._file.url
        return None

    def get_processing_status(self, obj):
        return obj.get_processing_status_display()


class CommunitySerializer(BaseCommunitySerializer):
    # submitter = serializers.SerializerMethodField(read_only=True)
    processing_status = serializers.SerializerMethodField(read_only=True)

    def get_readonly(self, obj):
        return obj.can_user_view

    # Priya updated as gives error for submitter when resubmit after amendment request
    def get_submitter(self, obj):
        # if obj.submitter:
        #     email_user = retrieve_email_user(obj.submitter)
        #     return email_user.get_full_name()
        # else:
        #     return None
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return None


class SimpleCommunityDisplaySerializer(BaseModelSerializer):
    community_name = serializers.CharField(
        source="taxonomy.community_name", read_only=True
    )
    community_migrated_id = serializers.CharField(
        source="taxonomy.community_migrated_id", read_only=True
    )

    class Meta:
        model = Community
        fields = (
            "id",
            "community_number",
            "community_name",
            "community_migrated_id",
        )


class InternalCommunitySerializer(BaseCommunitySerializer):
    submitter = serializers.SerializerMethodField(read_only=True)
    processing_status = serializers.SerializerMethodField(read_only=True)
    current_assessor = serializers.SerializerMethodField()
    user_edit_mode = serializers.SerializerMethodField()
    can_user_edit = serializers.SerializerMethodField()
    can_add_log = serializers.SerializerMethodField()
    renamed_from = SimpleCommunityDisplaySerializer(read_only=True, allow_null=True)
    renamed_to = SimpleCommunityDisplaySerializer(
        many=True, read_only=True, allow_null=True
    )
    readonly = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Community
        fields = (
            "id",
            "migrated_from_id",
            "community_number",
            "group_type",
            "taxonomy_details",
            "regions",
            "districts",
            "conservation_status",
            "distribution",
            "publishing_status",
            "conservation_attributes",
            "last_data_curation_date",
            "image_doc",
            "processing_status",
            "lodgement_date",
            "submitter",
            "readonly",
            "can_user_edit",
            "can_user_view",
            "current_assessor",
            "user_edit_mode",
            "comment",
            "conservation_plan_exists",
            "conservation_plan_reference",
            "publishing_status",
            "conservation_status",
            "occurrence_count",
            "area_of_occupancy_km2",
            "area_occurrence_convex_hull_km2",
            "can_add_log",
            "renamed_from",
            "renamed_to",
            "department_file_numbers",
        )

    def get_submitter(self, obj):
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return None

    def get_readonly(self, obj):
        request = self.context["request"]
        return not (
            (obj.can_user_edit or obj.has_user_edit_mode(request))
            and is_species_communities_approver(request)
        )

    def get_can_add_log(self, obj):
        request = self.context["request"]
        return (
            is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
        )

    def get_can_user_edit(self, obj):
        request = self.context["request"]
        if is_species_communities_approver(request):
            return obj.can_user_edit
        return False

    def get_current_assessor(self, obj):
        return {
            "id": self.context["request"].user.id,
            "name": self.context["request"].user.get_full_name(),
            "email": self.context["request"].user.email,
        }

    def get_user_edit_mode(self, obj):
        request = self.context["request"]
        return obj.has_user_edit_mode(request)


class SaveSpeciesSerializer(BaseSpeciesSerializer):
    taxonomy_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )

    class Meta:
        model = Species
        fields = (
            "id",
            "group_type",
            "taxonomy_id",
            "last_data_curation_date",
            "submitter",
            "readonly",
            "can_user_edit",
            "can_user_view",
            "comment",
            "conservation_plan_exists",
            "conservation_plan_reference",
            "regions",
            "districts",
            "department_file_numbers",
            "fauna_group",
            "fauna_sub_group",
        )
        read_only_fields = ("id", "group_type")

    def to_internal_value(self, data):
        if data["last_data_curation_date"] == "":
            data["last_data_curation_date"] = None
        return super().to_internal_value(data)


class CreateSpeciesSerializer(BaseSpeciesSerializer):
    group_type_id = serializers.IntegerField(required=True, write_only=True)

    class Meta:
        model = Species
        fields = (
            "id",
            "group_type_id",
        )
        read_only_fields = ("id",)

    # override save so we can include our kwargs
    def save(self, *args, **kwargs):
        instance = Species()
        validated_data = self.run_validation(self.initial_data)
        for field_name in self.Meta.fields:
            if field_name not in self.Meta.read_only_fields:
                setattr(instance, field_name, validated_data[field_name])
        instance.save(*args, **kwargs)
        data = {}  # here we also return the instance data
        for field_name in self.Meta.fields:
            data[field_name] = getattr(instance, field_name)
        return instance, data


class EmptySpeciesSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=False, read_only=True)
    group_type = serializers.CharField(required=False, read_only=True)
    group_type_id = serializers.IntegerField(required=False, read_only=True)
    taxonomy_id = serializers.IntegerField(required=False, read_only=True)
    taxonomy_details = TaxonomySerializer(read_only=True)
    distribution = SpeciesDistributionSerializer(read_only=True)
    regions = serializers.ListField(
        child=serializers.IntegerField(),
        required=False,
        allow_empty=True,
        read_only=True,
    )
    districts = serializers.ListField(
        child=serializers.IntegerField(),
        required=False,
        allow_empty=True,
        read_only=True,
    )
    department_file_numbers = serializers.CharField(
        required=False,
        allow_blank=True,
        allow_null=True,
        read_only=True,
    )
    last_data_curation_date = serializers.DateTimeField(
        required=False,
        allow_null=True,
        read_only=True,
    )
    conservation_plan_exists = serializers.BooleanField(
        required=False,
        allow_null=True,
        read_only=True,
    )
    conservation_plan_reference = serializers.CharField(
        required=False,
        allow_blank=True,
        allow_null=True,
        read_only=True,
    )
    comment = serializers.CharField(
        required=False,
        allow_blank=True,
        allow_null=True,
        read_only=True,
    )

    def __init__(self, *args, **kwargs):
        if not kwargs["taxonomy"] or not isinstance(kwargs["taxonomy"], Taxonomy):
            raise ValueError(
                "EmptySpeciesSerializer requires a Taxonomy instance as the 'taxonomy' keyword argument."
            )

        taxonomy = kwargs.pop("taxonomy")
        super().__init__(*args, **kwargs)

        self.fields["group_type_id"].initial = taxonomy.kingdom_fk.grouptype.id
        self.fields["group_type"].initial = taxonomy.kingdom_fk.grouptype.name
        self.fields["taxonomy_id"].initial = taxonomy.id
        self.fields["taxonomy_details"].initial = TaxonomySerializer(
            taxonomy, context=self.context, read_only=True
        ).data
        self.fields["distribution"].initial = SpeciesDistributionSerializer(
            SpeciesDistribution(species=None), context=self.context, read_only=True
        ).data

    def to_representation(self, instance):
        # If no instance, use initial values
        if instance is None:
            ret = {}
            for field_name, field in self.fields.items():
                if hasattr(field, "initial"):
                    ret[field_name] = field.initial
                else:
                    ret[field_name] = None
            return ret
        # Otherwise, use normal representation
        return super().to_representation(instance)

    @property
    def data(self):
        # Always call to_representation(None) if no instance
        return self.to_representation(None)


class SaveCommunitySerializer(BaseCommunitySerializer):

    class Meta:
        model = Community
        fields = (
            "id",
            "group_type",
            "last_data_curation_date",
            "submitter",
            "readonly",
            "can_user_edit",
            "can_user_view",
            "comment",
            "conservation_plan_exists",
            "conservation_plan_reference",
            "regions",
            "districts",
            "department_file_numbers",
        )
        read_only_fields = ("id", "group_type")

    def to_internal_value(self, data):
        if data["last_data_curation_date"] == "":
            data["last_data_curation_date"] = None
        return super().to_internal_value(data)


class RenameCommunitySerializer(BaseSerializer):
    community_name = serializers.CharField(required=True)
    community_migrated_id = serializers.CharField(required=True)
    community_description = serializers.CharField(
        required=False, allow_blank=True, allow_null=True
    )
    previous_name = serializers.CharField(
        required=False, allow_blank=True, allow_null=True
    )
    name_authority = serializers.CharField(
        required=False, allow_blank=True, allow_null=True
    )
    name_comments = serializers.CharField(
        required=False, allow_blank=True, allow_null=True
    )


class CreateCommunitySerializer(BaseCommunitySerializer):
    group_type_id = serializers.IntegerField(required=True, write_only=True)

    class Meta:
        model = Community
        fields = (
            "id",
            "group_type_id",
        )
        read_only_fields = ("id",)

    # override save so we can include our kwargs
    def save(self, *args, **kwargs):
        instance = Community()
        validated_data = self.run_validation(self.initial_data)
        for field_name in self.Meta.fields:
            if field_name not in self.Meta.read_only_fields:
                setattr(instance, field_name, validated_data[field_name])
        instance.save(*args, **kwargs)
        data = {}
        for field_name in self.Meta.fields:
            data[field_name] = getattr(instance, field_name)
        return instance, data


class DocumentSerializer(BaseModelSerializer):
    _file = SafeFileUrlField(
        allow_null=True,
        required=False,
    )

    class Meta:
        model = SpeciesDocument
        fields = ("id", "name", "_file")


class SpeciesDocumentSerializer(BaseModelSerializer):
    document_category_name = serializers.SerializerMethodField()
    document_sub_category_name = serializers.SerializerMethodField()
    _file = SafeFileUrlField(
        allow_null=True,
        required=False,
    )

    class Meta:
        model = SpeciesDocument
        fields = (
            "id",
            "document_number",
            "species",
            "name",
            "_file",
            "description",
            "input_name",
            "uploaded_date",
            "document_category",
            "document_category_name",
            "document_sub_category",
            "document_sub_category_name",
            "active",
        )
        read_only_fields = ("id", "document_number")

    def get_document_category_name(self, obj):
        if obj.document_category:
            return obj.document_category.document_category_name

    def get_document_sub_category_name(self, obj):
        if obj.document_sub_category:
            return obj.document_sub_category.document_sub_category_name


class SaveSpeciesDocumentSerializer(BaseModelSerializer):
    class Meta:
        model = SpeciesDocument
        fields = (
            "id",
            "species",
            "name",
            "description",
            "input_name",
            "uploaded_date",
            "document_category",
            "document_sub_category",
        )
        read_only_fields = ("id",)

    # override save so we can include our kwargs
    def save(self, *args, **kwargs):
        # if the instance already exists, carry on as normal
        if self.instance:
            return super().save(*args, **kwargs)
        else:
            instance = SpeciesDocument()
            validated_data = self.run_validation(self.initial_data)
            for field_name in self.Meta.fields:
                if (
                    field_name in validated_data
                    and field_name not in self.Meta.read_only_fields
                ):
                    setattr(instance, field_name, validated_data[field_name])
            instance.save(*args, **kwargs)
            return instance


class CommunityDocumentSerializer(BaseModelSerializer):
    document_category_name = serializers.SerializerMethodField()
    document_sub_category_name = serializers.SerializerMethodField()
    _file = SafeFileUrlField(
        allow_null=True,
        required=False,
    )

    class Meta:
        model = CommunityDocument
        fields = (
            "id",
            "document_number",
            "community",
            "name",
            "_file",
            "description",
            "input_name",
            "uploaded_date",
            "document_category",
            "document_category_name",
            "document_sub_category",
            "document_sub_category_name",
            "active",
        )
        read_only_fields = ("id", "document_number")

    def get_document_category_name(self, obj):
        if obj.document_category:
            return obj.document_category.document_category_name

    def get_document_sub_category_name(self, obj):
        if obj.document_sub_category:
            return obj.document_sub_category.document_sub_category_name


class SaveCommunityDocumentSerializer(BaseModelSerializer):
    class Meta:
        model = CommunityDocument
        fields = (
            "id",
            "community",
            "name",
            "description",
            "input_name",
            "uploaded_date",
            "document_category",
            "document_sub_category",
        )
        read_only_fields = ("id",)

    # override save so we can include our kwargs
    def save(self, *args, **kwargs):
        # if the instance already exists, carry on as normal
        if self.instance:
            return super().save(*args, **kwargs)
        else:
            instance = CommunityDocument()
            validated_data = self.run_validation(self.initial_data)
            for field_name in self.Meta.fields:
                if (
                    field_name in validated_data
                    and field_name not in self.Meta.read_only_fields
                ):
                    setattr(instance, field_name, validated_data[field_name])
            instance.save(*args, **kwargs)
            return instance


class SpeciesLogEntrySerializer(CommunicationLogEntrySerializer):
    documents = serializers.SerializerMethodField()

    class Meta:
        model = SpeciesLogEntry
        fields = "__all__"
        read_only_fields = ("customer",)

    def get_documents(self, obj):
        return [[d.name, d._file.url] for d in obj.documents.all()]


class SpeciesUserActionSerializer(BaseModelSerializer):
    who = serializers.SerializerMethodField()

    class Meta:
        model = SpeciesUserAction
        fields = "__all__"

    def get_who(self, species_user_action):
        email_user = retrieve_email_user(species_user_action.who)
        fullname = email_user.get_full_name()
        return fullname


class ConservationThreatSerializer(BaseModelSerializer):
    threat_category = serializers.SerializerMethodField()
    threat_agent = serializers.SerializerMethodField()
    current_impact_name = serializers.SerializerMethodField()
    potential_impact_name = serializers.SerializerMethodField()
    potential_threat_onset_name = serializers.SerializerMethodField()

    class Meta:
        model = ConservationThreat
        fields = (
            "id",
            "threat_number",
            "threat_category_id",
            "threat_category",
            "threat_agent",
            "threat_agent_id",
            "current_impact",
            "current_impact_name",
            "potential_impact",
            "potential_impact_name",
            "potential_threat_onset",
            "potential_threat_onset_name",
            "comment",
            "date_observed",
            "source",
            "species",
            "community",
            "visible",
        )
        read_only_fields = (
            "id",
            "threat_number",
        )

    def get_threat_category(self, obj):
        if obj.threat_category:
            return obj.threat_category.name

    def get_threat_agent(self, obj):
        if obj.threat_agent:
            return obj.threat_agent.name

    def get_current_impact_name(self, obj):
        if obj.current_impact:
            return obj.current_impact.name

    def get_potential_impact_name(self, obj):
        if obj.potential_impact:
            return obj.potential_impact.name

    def get_potential_threat_onset_name(self, obj):
        if obj.potential_threat_onset:
            return obj.potential_threat_onset.name


class SaveConservationThreatSerializer(BaseModelSerializer):
    threat_category_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )
    threat_agent_id = serializers.IntegerField(
        required=False, allow_null=True, write_only=True
    )

    class Meta:
        model = ConservationThreat
        fields = (
            "id",
            "species",
            "community",
            "threat_category_id",
            "threat_agent_id",
            "comment",
            "current_impact",
            "potential_impact",
            "potential_threat_onset",
            "date_observed",
        )
        read_only_fields = ("id",)

    # override save so we can include our kwargs
    def save(self, *args, **kwargs):
        # if the instance already exists, carry on as normal
        if self.instance:
            return super().save(*args, **kwargs)
        else:
            instance = ConservationThreat()
            validated_data = self.run_validation(self.initial_data)
            for field_name in self.Meta.fields:
                if (
                    field_name in validated_data
                    and field_name not in self.Meta.read_only_fields
                ):
                    setattr(instance, field_name, validated_data[field_name])
            instance.save(*args, **kwargs)
            return instance


class CommunityLogEntrySerializer(CommunicationLogEntrySerializer):
    documents = serializers.SerializerMethodField()

    class Meta:
        model = CommunityLogEntry
        fields = "__all__"
        read_only_fields = ("customer",)

    def get_documents(self, obj):
        return [[d.name, d._file.url] for d in obj.documents.all()]


class CommunityUserActionSerializer(BaseModelSerializer):
    who = serializers.SerializerMethodField()

    class Meta:
        model = CommunityUserAction
        fields = "__all__"

    def get_who(self, community_user_action):
        email_user = retrieve_email_user(community_user_action.who)
        fullname = email_user.get_full_name()
        return fullname


class DistrictSerializer(BaseModelSerializer):
    class Meta:
        model = District
        fields = ("id", "name", "code", "archive_date")


class RegionSerializer(BaseModelSerializer):
    districts = DistrictSerializer(many=True)

    class Meta:
        model = Region
        fields = ("id", "name", "forest_region", "districts")
