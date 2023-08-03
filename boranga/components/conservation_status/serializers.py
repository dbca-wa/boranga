import logging

from django.conf import settings
from ledger_api_client.ledger_models import EmailUserRO as EmailUser, Address
from boranga.components.species_and_communities.models import(
    GroupType,
    Species,
    Community,
    Taxonomy,
    CommunityTaxonomy
    )
from boranga.components.conservation_status.models import(
    ConservationStatus,
    ConservationStatus,
    ConservationStatusLogEntry,
    ConservationStatusUserAction,
    ConservationCriteria,
    ConservationStatusReferral,
    ConservationStatusAmendmentRequest,
    ConservationStatusAmendmentRequestDocument,
    ConservationStatusDeclinedDetails,
    ConservationStatusIssuanceApprovalDetails,
    ConservationStatusDocument,
    )

from boranga.components.users.serializers import UserSerializer
from boranga.components.users.serializers import UserAddressSerializer, DocumentSerializer
from boranga.components.main.serializers import(
    CommunicationLogEntrySerializer,
    EmailUserSerializer,
    )
from boranga.ledger_api_utils import retrieve_email_user
from rest_framework import serializers
from django.db.models import Q

logger = logging.getLogger('boranga')

#Serializer used for species form
class SpeciesConservationStatusSerializer(serializers.ModelSerializer):
    conservation_status = serializers.SerializerMethodField()
    conservation_list = serializers.SerializerMethodField()
    conservation_category = serializers.SerializerMethodField()
    class Meta:
        model = ConservationStatus
        fields = (
            'id',
            'conservation_status_number',
            'species',
            'conservation_status',
            'conservation_list',
            'conservation_category',
            #'conservation_criteria',
            )

    def get_conservation_status(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.code
        return ''

    def get_conservation_list(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.code
        return ''

    def get_conservation_category(self,obj):
        if obj.conservation_category:
            return obj.conservation_category.code
        return ''

#Serializer used for community form
class CommunityConservationStatusSerializer(serializers.ModelSerializer):
    conservation_status = serializers.SerializerMethodField()
    conservation_list = serializers.SerializerMethodField()
    conservation_category = serializers.SerializerMethodField()
    class Meta:
        model = ConservationStatus
        fields = (
            'id',
            'conservation_status_number',
            'community',
            'conservation_status',
            'conservation_list',
            'conservation_category',
            #'conservation_criteria',
            )

    def get_conservation_status(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.code
        return ''

    def get_conservation_list(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.code
        return ''

    def get_conservation_category(self,obj):
        if obj.conservation_category:
            return obj.conservation_category.code
        return ''


class ConservationCriteriaSerializer(serializers.ModelSerializer):

    class Meta:
        model = ConservationCriteria
        fields = ('id',
            'code',
            'label'
            )
        read_only_fields = ('id',)


class ListConservationStatusSerializer(serializers.ModelSerializer):
    scientific_name = serializers.SerializerMethodField()
    community_name = serializers.SerializerMethodField()
    conservation_list = serializers.SerializerMethodField()
    conservation_category = serializers.SerializerMethodField()
    customer_status = serializers.CharField(source='get_customer_status_display')
    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'conservation_status_number',
                'group_type',
                'scientific_name',
                'community_name',
                'conservation_list',
                'conservation_category',
                'processing_status',
                'customer_status',
                'can_user_edit',
                'can_user_view',
            )
        datatables_always_serialize = (
                'id',
                'conservation_status_number',
                'group_type',
                'scientific_name',
                'community_name',
                'conservation_list',
                'conservation_category',
                'processing_status',
                'customer_status',
                'can_user_edit',
                'can_user_view',
            )   

    def get_scientific_name(self,obj):
        if obj.species:
            if obj.species.taxonomy:
                return obj.species.taxonomy.scientific_name
        return ''

    def get_community_name(self,obj):
        if obj.community:
            try:
                taxonomy = CommunityTaxonomy.objects.get(community = obj.community)
                return taxonomy.community_name
            except CommunityTaxonomy.DoesNotExist:
                return ''
        return ''

    def get_conservation_list(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.code
        return ''

    def get_conservation_category(self,obj):
        if obj.conservation_category:
            return obj.conservation_category.code
        return ''


class ListSpeciesConservationStatusSerializer(serializers.ModelSerializer):
    group_type = serializers.SerializerMethodField()
    species_number = serializers.SerializerMethodField()
    scientific_name = serializers.SerializerMethodField()
    common_name = serializers.SerializerMethodField()
    family = serializers.SerializerMethodField()
    genus = serializers.SerializerMethodField()
    phylogenetic_group = serializers.SerializerMethodField()
    conservation_list = serializers.SerializerMethodField()
    conservation_category = serializers.SerializerMethodField()
    processing_status = serializers.CharField(source='get_processing_status_display')
    region = serializers.SerializerMethodField()
    district = serializers.SerializerMethodField()
    assessor_process = serializers.SerializerMethodField(read_only=True)
    assessor_edit = serializers.SerializerMethodField(read_only=True)
    internal_user_edit = serializers.SerializerMethodField(read_only=True)
    effective_from_date = serializers.SerializerMethodField()
    effective_to_date = serializers.SerializerMethodField()

    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'conservation_status_number',
                'group_type',
                'species_number',
                'scientific_name',
                'common_name',
                'family',
                'genus',
                'phylogenetic_group',
                'region',
                'district',
                'conservation_list',
                'conservation_category',
                'processing_status',
                'customer_status',
                'can_user_edit',
                'can_user_view',
                'assessor_process',
                'assessor_edit',
                'internal_application',
                'internal_user_edit',
                'effective_from_date',
                'effective_to_date',
            )
        datatables_always_serialize = (
                'id',
                'conservation_status_number',
                'group_type',
                'species_number',
                'scientific_name',
                'common_name',
                'family',
                'genus',
                'phylogenetic_group',
                'region',
                'district',
                'conservation_list',
                'conservation_category',
                'processing_status',
                'customer_status',
                'can_user_edit',
                'can_user_view',
                'assessor_process',
                'assessor_edit',
                'internal_application',
                'internal_user_edit',
                'effective_from_date',
                'effective_to_date',
            )   

    def get_group_type(self,obj):
        if obj.species:
            return obj.species.group_type.name
        else:
            return obj.application_type.name # if user haven't filled up the form yet(ie. species not selected)

    def get_species_number(self,obj):
        if obj.species:
            return obj.species.species_number
        return ''

    def get_scientific_name(self,obj):
        if obj.species:
            if obj.species.taxonomy:
                return obj.species.taxonomy.scientific_name
        return ''

    def get_common_name(self,obj):
        if obj.species:
            if obj.species.taxonomy.vernaculars:
                names_list=obj.species.taxonomy.vernaculars.all().values_list('vernacular_name', flat=True)
                return ','.join(names_list)
        return ''

    def get_family(self,obj):
        if obj.species:
            if obj.species.taxonomy.family_fk:
                return obj.species.taxonomy.family_fk.scientific_name
        return ''

    def get_genus(self,obj):
        if obj.species:
            if obj.species.taxonomy.genus:
                return obj.species.taxonomy.genus.name
        return ''

    def get_phylogenetic_group(self,obj):
        if obj.species:
            if obj.species.taxonomy.phylogenetic_group:
                return obj.species.taxonomy.phylogenetic_group.name
        return ''

    def get_conservation_list(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.code
        return ''

    def get_conservation_category(self,obj):
        if obj.conservation_category:
            return obj.conservation_category.code
        return ''

    def get_region(self,obj):
        if obj.species:
            if obj.species.region:
                return obj.species.region.name
        return ''

    def get_effective_from_date(self,obj):
        try:
            approval = ConservationStatusIssuanceApprovalDetails.objects.get(conservation_status=obj.id)
            if approval.effective_from_date:
                return approval.effective_from_date
        except ConservationStatusIssuanceApprovalDetails.DoesNotExist:
            return ''
    
    def get_effective_to_date(self,obj):
        try:
            approval = ConservationStatusIssuanceApprovalDetails.objects.get(conservation_status=obj.id)
            if approval.effective_to_date:
                return approval.effective_to_date
        except ConservationStatusIssuanceApprovalDetails.DoesNotExist:
            return ''
    
    def get_district(self,obj):
        if obj.species:
            if obj.species.district:
                return obj.species.district.name
        return ''
    
    def get_assessor_process(self,obj):
        # Check if currently logged in user has access to process the proposal
        request = self.context['request']
        template_group = self.context.get('template_group')
        user = request.user
        # if obj.can_officer_process and template_group == 'apiary':
        # TODO if internal user proposal then check condition that he is not able to process
        if obj.can_officer_process:
            if obj.assigned_officer:
                if obj.assigned_officer == user.id:
                    return True
            elif user in obj.allowed_assessors:
                return True
        return False
    
    def get_assessor_edit(self,obj):
        request = self.context['request']
        user = request.user
        if obj.can_officer_edit:
            if user in obj.allowed_assessors:
                return True
        return False


    def get_internal_user_edit(self,obj):
        request = self.context['request']
        user = request.user
        if obj.can_user_edit:
            if obj.internal_application == True:
                return True
        else:
            return False


class ListCommunityConservationStatusSerializer(serializers.ModelSerializer):
    group_type = serializers.SerializerMethodField()
    community_number = serializers.SerializerMethodField()
    community_migrated_id = serializers.SerializerMethodField()
    community_name = serializers.SerializerMethodField()
    #conservation_status = serializers.SerializerMethodField()
    conservation_list = serializers.SerializerMethodField()
    conservation_category = serializers.SerializerMethodField()
    processing_status = serializers.CharField(source='get_processing_status_display')
    region = serializers.SerializerMethodField()
    district = serializers.SerializerMethodField()
    assessor_process = serializers.SerializerMethodField(read_only=True)
    assessor_edit = serializers.SerializerMethodField(read_only=True)
    internal_user_edit = serializers.SerializerMethodField(read_only=True)
    effective_from_date = serializers.SerializerMethodField()
    effective_to_date = serializers.SerializerMethodField()

    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'conservation_status_number',
                'group_type',
                'community_number',
                'community_migrated_id',
                'community_name',
                #'conservation_status',
                'conservation_list',
                'conservation_category',
                'region',
                'district',
                'processing_status',
                'customer_status',
                'can_user_edit',
                'can_user_view',
                'assessor_process',
                'assessor_edit',
                'internal_application',
                'internal_user_edit',
                'effective_from_date',
                'effective_to_date',
            )
        datatables_always_serialize = (
                'id',
                'conservation_status_number',
                'community_number',
                'group_type',
                'community_migrated_id',
                'community_name',
                #'conservation_status',
                'conservation_list',
                'conservation_category',
                'region',
                'district',
                'processing_status',
                'customer_status',
                'can_user_edit',
                'can_user_view',
                'assessor_process',
                'assessor_edit',
                'internal_application',
                'internal_user_edit',
                'effective_from_date',
                'effective_to_date',
            )

    def get_group_type(self,obj):
        if obj.community:
            return obj.community.group_type.name
        else:
            return obj.application_type.name # if user haven't filled up the form yet(ie. species not selected)

    # def get_conservation_status(self,obj):
    #   try:
    #       conservation_status = ConservationStatus.objects.get(community=obj)
    #       return conservation_status.conservation_list.code
    #   except ConservationStatus.DoesNotExist:
    #       return None

    def get_community_number(self,obj):
        if obj.community:
            return obj.community.community_number
        return ''

    def get_community_migrated_id(self,obj):
        if obj.community:
            try:
                taxonomy = CommunityTaxonomy.objects.get(community= obj.community)
                return taxonomy.community_migrated_id
            except CommunityTaxonomy.DoesNotExist:
                return ''
        return ''

    def get_community_name(self,obj):
        if obj.community:
            try:
                taxonomy = CommunityTaxonomy.objects.get(community= obj.community)
                return taxonomy.community_name
            except CommunityTaxonomy.DoesNotExist:
                return ''
        return ''

    def get_conservation_list(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.code
        return ''

    def get_conservation_category(self,obj):
        if obj.conservation_category:
            return obj.conservation_category.code
        return ''

    def get_region(self,obj):
        if obj.community:
            if obj.community.region:
                return obj.community.region.name
        return ''

    def get_district(self,obj):
        if obj.community:
            if obj.community.district:
                return obj.community.district.name
        return ''

    def get_effective_from_date(self,obj):
        try:
            approval = ConservationStatusIssuanceApprovalDetails.objects.get(conservation_status=obj.id)
            if approval.effective_from_date:
                return approval.effective_from_date
        except ConservationStatusIssuanceApprovalDetails.DoesNotExist:
            return ''

    def get_effective_to_date(self,obj):
        try:
            approval = ConservationStatusIssuanceApprovalDetails.objects.get(conservation_status=obj.id)
            if approval.effective_to_date:
                return approval.effective_to_date
        except ConservationStatusIssuanceApprovalDetails.DoesNotExist:
            return ''
    
    def get_assessor_process(self,obj):
        # Check if currently logged in user has access to process the proposal
        request = self.context['request']
        template_group = self.context.get('template_group')
        user = request.user
        # if obj.can_officer_process and template_group == 'apiary':
        if obj.can_officer_process:
            if obj.assigned_officer:
                if obj.assigned_officer == user.id:
                    return True
            elif user in obj.allowed_assessors:
                return True
        return False
    
    def get_assessor_edit(self,obj):
        request = self.context['request']
        user = request.user
        if obj.can_officer_edit:
            if user in obj.allowed_assessors:
                return True
        return False

    def get_internal_user_edit(self,obj):
        request = self.context['request']
        user = request.user
        if obj.can_user_edit:
            if obj.internal_application == True:
                return True
        else:
            return False


class BaseConservationStatusSerializer(serializers.ModelSerializer):
    readonly = serializers.SerializerMethodField(read_only=True)
    group_type = serializers.SerializerMethodField(read_only=True)
    group_type_id = serializers.SerializerMethodField(read_only=True)
    conservation_criteria = serializers.SerializerMethodField()
    allowed_assessors = EmailUserSerializer(many=True)
    list_approval_level = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'group_type',
                'group_type_id',
                'species_id',
                'community_id',
                'conservation_status_number',
                'conservation_list_id',
                'conservation_category_id',
                'conservation_criteria',
                'comment',
                'proposed_date',
                'lodgement_date',
                'applicant_type',
                'applicant',
                'submitter',
                'assigned_officer',
                'customer_status',
                'processing_status',
                'review_status',
                'readonly',
                'can_user_edit',
                'can_user_view',
                'reference',
                'applicant_details',
                'assigned_officer',
                'assigned_approver',
                'allowed_assessors',
                'deficiency_data',
                'assessor_data',
                'list_approval_level',
                )

    def get_readonly(self,obj):
        return False
    
    def get_group_type(self,obj):
        if obj.species:
            return obj.species.group_type.name
        elif obj.community:
            return obj.community.group_type.name
        else:
            return obj.application_type.name
    
    def get_group_type_id(self,obj):
        if obj.species:
            return obj.species.group_type.id
        elif obj.community:
            return obj.community.group_type.id
        else:
            return obj.application_type.id

    def get_conservation_criteria(self,obj):
        return [c.id for c in obj.conservation_criteria.all()]

    def get_processing_status(self,obj):
        return obj.get_processing_status_display()

    def get_review_status(self,obj):
        return obj.get_review_status_display()

    def get_customer_status(self,obj):
        return obj.get_customer_status_display()
    
    def get_list_approval_level(self,obj):
        if obj.conservation_list:
            return obj.conservation_list.approval_level
        else:
            return None


class ConservationStatusSerializer(BaseConservationStatusSerializer):
    submitter = serializers.SerializerMethodField(read_only=True)
    processing_status = serializers.SerializerMethodField(read_only=True)
    review_status = serializers.SerializerMethodField(read_only=True)
    customer_status = serializers.SerializerMethodField(read_only=True)

    def get_readonly(self,obj):
        return obj.can_user_view

    # Priya updated as gives error for submitter when resubmit after amendment request
    def get_submitter(self,obj):
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
    

class CreateConservationStatusSerializer(BaseConservationStatusSerializer):
    application_type = serializers.IntegerField(required=False, allow_null=True, write_only= True)
    class Meta:
        model = ConservationStatus
        fields = (
            'id',
            'application_type',
            'internal_application',
            'submitter',
            )
        read_only_fields = (
            'id',
            'application_type',
            'submitter',
            )


class ConservationStatusProposalReferralSerializer(serializers.ModelSerializer):
    #referral = serializers.CharField(source='referral.get_full_name')
    referral_obj = serializers.SerializerMethodField()
    processing_status = serializers.CharField(source='get_processing_status_display')
    class Meta:
        model = ConservationStatusReferral
        fields = '__all__'

    def get_referral_obj(self, obj):
        referral_email_user = retrieve_email_user(obj.referral)
        serializer = EmailUserSerializer(referral_email_user)
        return serializer.data


class ConservationStatusDeclinedDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConservationStatusDeclinedDetails
        fields = '__all__'

class ConservationStatusIssuanceApprovalDetailsSerializer(serializers.ModelSerializer):
    effective_from_date = serializers.DateField(format="%Y-%m-%d",required=False,allow_null=True)
    #effective_to_date = serializers.DateTimeField(format="%d/%m/%Y",input_formats=['%d/%m/%Y'],required=False,allow_null=True)
    effective_to_date = serializers.DateField(format="%Y-%m-%d",required=False,allow_null=True)
    class Meta:
        model = ConservationStatusIssuanceApprovalDetails
        fields = (
                'effective_from_date',
                'effective_to_date',
                'details',
                'cc_email',
            )


# this internal serializer is used rather than InternalSpeciesConservationStatusSerializer and InternalCommunityConservationStatusSerializer
class InternalConservationStatusSerializer(BaseConservationStatusSerializer):
    curr_conservation_list = serializers.SerializerMethodField(read_only=True)
    curr_conservation_category = serializers.SerializerMethodField(read_only=True)
    curr_conservation_criteria = serializers.SerializerMethodField(read_only=True)
    submitter = serializers.SerializerMethodField(read_only=True)
    processing_status = serializers.SerializerMethodField(read_only=True)
    customer_status = serializers.SerializerMethodField(read_only=True)
    current_assessor = serializers.SerializerMethodField()
    latest_referrals = ConservationStatusProposalReferralSerializer(many=True)
    allowed_assessors = EmailUserSerializer(many=True)
    assessor_mode = serializers.SerializerMethodField()
    conservationstatusdeclineddetails = ConservationStatusDeclinedDetailsSerializer()
    conservationstatusissuanceapprovaldetails = ConservationStatusIssuanceApprovalDetailsSerializer()
    conservation_status_approval_document = serializers.SerializerMethodField()
    internal_user_edit = serializers.SerializerMethodField(read_only=True)
    # accessing_user_roles = (
    #     serializers.SerializerMethodField()
    # )


    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'group_type',
                'group_type_id',
                'species_id',
                'community_id',
                'conservation_status_number',
                'curr_conservation_list',
                'curr_conservation_category',
                'curr_conservation_criteria',
                'conservation_list_id',
                'conservation_category_id',
                'conservation_criteria',
                'comment',
                'proposed_date',
                'processing_status',
                'customer_status',
                'readonly',
                'lodgement_date',
                'submitter',
                'applicant_type',
                'assigned_officer',
                'assigned_approver',
                'can_user_edit',
                'can_user_view',
                'current_assessor',
                'latest_referrals',
                'allowed_assessors',
                'assessor_mode',
                'deficiency_data',
                'assessor_data',
                'proposed_issuance_approval',
                'proposed_decline_status',
                'conservationstatusdeclineddetails',
                'conservationstatusissuanceapprovaldetails',
                'internal_user_edit',
                'conservation_status_approval_document',
                #'accessing_user_roles',
                'list_approval_level',
                )

    # def get_accessing_user_roles(self, conservation_status):
    #     request = self.context.get("request")
    #     accessing_user = request.user
    #     roles = []
    #     if (
    #         accessing_user.id
    #         in conservation_status.get_assessor_group().get_system_group_member_ids()
    #     ):
    #         roles.append("assessor")
    #     if (
    #         accessing_user.id
    #         in conservation_status.get_approver_group().get_system_group_member_ids()
    #     ):
    #         roles.append("approver")
    #     referral_ids = list(conservation_status.referrals.values_list("referral", flat=True))
    #     if accessing_user.id in referral_ids:
    #         roles.append("referral")
    #     return roles


    def get_submitter(self, obj):
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return None
    
    def get_processing_status(self,obj):
        return obj.get_processing_status_display()

    def get_readonly(self,obj):
        # for internal add new conservation status change the below readonly
        #return True
        # Check if in 'draft' shouldn't be editable internal(if application is external) but should be editable(if internal_application)
        if obj.can_user_edit:
            if obj.internal_application:
                return False
            else:
                return True
        else:
            return obj.can_user_view

    def get_current_assessor(self, obj):
        return {
            "id": self.context["request"].user.id,
            "name": self.context["request"].user.get_full_name(),
            "email": self.context["request"].user.email,
        }

    def get_assessor_mode(self,obj):
        # TODO check if the proposal has been accepted or declined
        request = self.context["request"]
        user = (
            request.user._wrapped if hasattr(request.user, "_wrapped") else request.user
        )
        return {
            "assessor_mode": True,
            "has_assessor_mode": obj.has_assessor_mode(user),
            "assessor_can_assess": obj.can_assess(user),
            "assessor_level": "assessor",
            "assessor_box_view": obj.assessor_comments_view(user),
        }

    def get_internal_user_edit(self,obj):
        request = self.context['request']
        user = request.user
        if obj.can_user_edit:
            if obj.internal_application == True:
                return True
        else:
            return False

    def get_conservation_status_approval_document(self, obj):
        try:
            if obj.conservationstatusissuanceapprovaldetails.conservation_status_approval_document is not None:
                return [
                    obj.conservationstatusissuanceapprovaldetails.conservation_status_approval_document.name,
                    obj.conservationstatusissuanceapprovaldetails.conservation_status_approval_document._file.url,
                ]
            else:
                return obj.conservationstatusissuanceapprovaldetails.conservation_status_approval_document
        except ConservationStatusIssuanceApprovalDetails.DoesNotExist:
            return None
    
    def get_curr_conservation_list(self,obj):
        try:
            if obj.species:
                # TODO Do we need to condsider the wa, commonwealth, international approved status
                # prev_approved_cs=ConservationStatus.objects.get(species=obj.species, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED, conservation_list__applies_to_wa=True)
                prev_approved_cs=ConservationStatus.objects.get(species=obj.species, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED)
                return prev_approved_cs.conservation_list.label
            elif obj.community:
                # TODO Do we need to condsider the wa, commonwealth, international approved status
                # prev_approved_cs=ConservationStatus.objects.get(community=obj.community, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED, conservation_list__applies_to_wa=True)
                prev_approved_cs=ConservationStatus.objects.get(community=obj.community, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED)
                return prev_approved_cs.conservation_list.code
        except ConservationStatus.DoesNotExist:
            return ''
    
    def get_curr_conservation_category(self,obj):
        try:
            if obj.species:
                # TODO Do we need to condsider the wa, commonwealth, international approved status
                # prev_approved_cs=ConservationStatus.objects.get(species=obj.species, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED, conservation_list__applies_to_wa=True)
                prev_approved_cs=ConservationStatus.objects.get(species=obj.species, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED)
                return prev_approved_cs.conservation_category.code
            elif obj.community:
                # TODO Do we need to condsider the wa, commonwealth, international approved status
                # prev_approved_cs=ConservationStatus.objects.get(community=obj.community, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED, conservation_list__applies_to_wa=True)
                prev_approved_cs=ConservationStatus.objects.get(community=obj.community, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED)
                return prev_approved_cs.conservation_category.code
        except ConservationStatus.DoesNotExist:
            return ''
    
    def get_curr_conservation_criteria(self,obj):
        try:
            if obj.species:
                # TODO Do we need to condsider the wa, commonwealth, international approved status
                # prev_approved_cs=ConservationStatus.objects.get(species=obj.species, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED, conservation_list__applies_to_wa=True)
                prev_approved_cs=ConservationStatus.objects.get(species=obj.species, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED)
                criteria_list = prev_approved_cs.conservation_criteria.all().values_list('code', flat=True)
                return ','.join(criteria_list)
            elif obj.community:
                # TODO Do we need to condsider the wa, commonwealth, international approved status
                # prev_approved_cs=ConservationStatus.objects.get(community=obj.community, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED, conservation_list__applies_to_wa=True)
                prev_approved_cs=ConservationStatus.objects.get(community=obj.community, processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED)
                criteria_list = prev_approved_cs.conservation_criteria.all().values_list('code', flat=True)
                return ','.join(criteria_list)
        except ConservationStatus.DoesNotExist:
            return ''


# Not used at the moment
class InternalSpeciesConservationStatusSerializer(BaseConservationStatusSerializer):
    submitter = serializers.SerializerMethodField(read_only=True)
    processing_status = serializers.SerializerMethodField(read_only=True)
    customer_status = serializers.SerializerMethodField(read_only=True)
    current_assessor = serializers.SerializerMethodField()
    latest_referrals = ConservationStatusProposalReferralSerializer(many=True)
    allowed_assessors = EmailUserSerializer(many=True)
    assessor_mode = serializers.SerializerMethodField()
    conservationstatusdeclineddetails = ConservationStatusDeclinedDetailsSerializer()
    # accessing_user_roles = (
    #     serializers.SerializerMethodField()
    # )

    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'group_type',
                'species_id',
                'conservation_status_number',
                'conservation_list_id',
                'conservation_category_id',
                'conservation_criteria',
                'comment',
                'proposed_date',
                'processing_status',
                'customer_status',
                'readonly',
                'lodgement_date',
                'submitter',
                'applicant_type',
                'assigned_officer',
                'assigned_approver',
                'can_user_edit',
                'can_user_view',
                'current_assessor',
                'latest_referrals',
                'allowed_assessors',
                'assessor_mode',
                #'accessing_user_roles',
                'deficiency_data',
                'assessor_data',
                'proposed_issuance_approval',
                'proposed_decline_status',
                'conservationstatusdeclineddetails',
                )

    # def get_accessing_user_roles(self, conservation_status):
    #     request = self.context.get("request")
    #     accessing_user = request.user
    #     roles = []
    #     if (
    #         accessing_user.id
    #         in conservation_status.get_assessor_group().get_system_group_member_ids()
    #     ):
    #         roles.append("assessor")
    #     if (
    #         accessing_user.id
    #         in conservation_status.get_approver_group().get_system_group_member_ids()
    #     ):
    #         roles.append("approver")
    #     referral_ids = list(conservation_status.referrals.values_list("referral", flat=True))
    #     if accessing_user.id in referral_ids:
    #         roles.append("referral")
    #     return roles


    def get_submitter(self, obj):
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return None

    def get_readonly(self,obj):
        return True

    def get_current_assessor(self, obj):
        return {
            "id": self.context["request"].user.id,
            "name": self.context["request"].user.get_full_name(),
            "email": self.context["request"].user.email,
        }

    def get_assessor_mode(self,obj):
        # TODO check if the proposal has been accepted or declined
        request = self.context["request"]
        user = (
            request.user._wrapped if hasattr(request.user, "_wrapped") else request.user
        )
        return {
            "assessor_mode": True,
            "has_assessor_mode": obj.has_assessor_mode(user),
            "assessor_can_assess": obj.can_assess(user),
            "assessor_level": "assessor",
            "assessor_box_view": obj.assessor_comments_view(user),
        }


class SaveSpeciesConservationStatusSerializer(BaseConservationStatusSerializer):
    species_id = serializers.IntegerField(required=False, allow_null=True, write_only= True)
    conservation_list_id = serializers.IntegerField(required=False, allow_null=True, write_only= True)
    conservation_category_id = serializers.IntegerField(required=False, allow_null=True, write_only= True)
    #conservation_criteria = ConservationCriteriaSerializer(read_only = True)

    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'application_type',
                'species_id',
                'conservation_list_id',
                'conservation_category_id',
                'comment',
                'lodgement_date',
                'applicant_type',
                'submitter',
                'readonly',
                'can_user_edit',
                'can_user_view',
                'reference',
                'deficiency_data',
                'assessor_data',
                )
        read_only_fields=('id',)


class CreateSpeciesConservationStatusSerializer(BaseConservationStatusSerializer):
    class Meta:
        model = ConservationStatus
        fields = ('id',
                )
        read_only_fields = (
            'id',
            )


# Not used at the moment
class InternalCommunityConservationStatusSerializer(BaseConservationStatusSerializer):
    submitter = serializers.SerializerMethodField(read_only=True)
    processing_status = serializers.SerializerMethodField(read_only=True)
    customer_status = serializers.SerializerMethodField(read_only=True)
    current_assessor = serializers.SerializerMethodField()
    latest_referrals = ConservationStatusProposalReferralSerializer(many=True)
    allowed_assessors = EmailUserSerializer(many=True)
    assessor_mode = serializers.SerializerMethodField()
    conservationstatusdeclineddetails = ConservationStatusDeclinedDetailsSerializer(many=True)
    # accessing_user_roles = (
    #     serializers.SerializerMethodField()
    # )

    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'group_type',
                'community_id',
                'conservation_status_number',
                'conservation_list_id',
                'conservation_category_id',
                'conservation_criteria',
                'comment',
                'proposed_date',
                'processing_status',
                'customer_status',
                'readonly',
                'lodgement_date',
                'submitter',
                'applicant_type',
                'assigned_officer',
                'assigned_approver',
                'can_user_edit',
                'can_user_view',
                'current_assessor',
                'latest_referrals',
                'allowed_assessors',
                'assessor_mode',
                'deficiency_data',
                'assessor_data',
                'proposed_issuance_approval',
                'proposed_decline_status',
                'conservationstatusdeclineddetails',
                #'accessing_user_roles',
                )

    # def get_accessing_user_roles(self, conservation_status):
    #     request = self.context.get("request")
    #     accessing_user = request.user
    #     roles = []
    #     if (
    #         accessing_user.id
    #         in conservation_status.get_assessor_group().get_system_group_member_ids()
    #     ):
    #         roles.append("assessor")
    #     if (
    #         accessing_user.id
    #         in conservation_status.get_approver_group().get_system_group_member_ids()
    #     ):
    #         roles.append("approver")
    #     referral_ids = list(conservation_status.referrals.values_list("referral", flat=True))
    #     if accessing_user.id in referral_ids:
    #         roles.append("referral")
    #     return roles


    def get_submitter(self, obj):
        if obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return None

    def get_readonly(self,obj):
        return True

    def get_current_assessor(self, obj):
        return {
            "id": self.context["request"].user.id,
            "name": self.context["request"].user.get_full_name(),
            "email": self.context["request"].user.email,
        }

    def get_assessor_mode(self,obj):
        # TODO check if the proposal has been accepted or declined
        request = self.context["request"]
        user = (
            request.user._wrapped if hasattr(request.user, "_wrapped") else request.user
        )
        return {
            "assessor_mode": True,
            "has_assessor_mode": obj.has_assessor_mode(user),
            "assessor_can_assess": obj.can_assess(user),
            "assessor_level": "assessor",
            "assessor_box_view": obj.assessor_comments_view(user),
        }


class SaveCommunityConservationStatusSerializer(BaseConservationStatusSerializer):
    community_id = serializers.IntegerField(required=False, allow_null=True, write_only= True)
    conservation_list_id = serializers.IntegerField(required=False, allow_null=True, write_only= True)
    conservation_category_id = serializers.IntegerField(required=False, allow_null=True, write_only= True)
    #conservation_criteria = ConservationCriteriaSerializer(read_only = True)

    class Meta:
        model = ConservationStatus
        fields = (
                'id',
                'application_type',
                'community_id',
                'conservation_list_id',
                'conservation_category_id',
                'comment',
                'lodgement_date',
                'applicant_type',
                'submitter',
                'readonly',
                'can_user_edit',
                'can_user_view',
                'reference',
                'deficiency_data',
                'assessor_data',
                )
        read_only_fields=('id',)


class ConservationStatusUserActionSerializer(serializers.ModelSerializer):
    who = serializers.SerializerMethodField()
    class Meta:
        model = ConservationStatusUserAction
        fields = '__all__'

    def get_who(self, conservation_status_user_action):
        email_user = retrieve_email_user(conservation_status_user_action.who)
        fullname = email_user.get_full_name()
        return fullname


class ConservationStatusLogEntrySerializer(CommunicationLogEntrySerializer):
    documents = serializers.SerializerMethodField()

    class Meta:
        model = ConservationStatusLogEntry
        fields = "__all__"
        read_only_fields = ("customer",)

    def get_documents(self, obj):
        return [[d.name, d._file.url] for d in obj.documents.all()]


class SendReferralSerializer(serializers.Serializer):
    email = serializers.EmailField(allow_blank=True)
    text = serializers.CharField(allow_blank=True)

    def validate(self, data):
        field_errors = {}
        non_field_errors = []

        request = self.context.get("request")
        if request.user.email == data["email"]:
            non_field_errors.append("You cannot send referral to yourself.")
        elif not data["email"]:
            non_field_errors.append("Referral not found.")

        # Raise errors
        if field_errors:
            raise serializers.ValidationError(field_errors)
        if non_field_errors:
            raise serializers.ValidationError(non_field_errors)
        # else:
        # pass

        return data


class DTConservationStatusReferralSerializer(serializers.ModelSerializer):
    processing_status = serializers.CharField(source='conservation_status.get_processing_status_display')
    referral_status = serializers.CharField(source='get_processing_status_display')
    conservation_status_lodgement_date = serializers.CharField(source='conservation_status.lodgement_date')
    conservation_status_number = serializers.CharField(source='conservation_status.conservation_status_number')
    referral = serializers.SerializerMethodField()
    submitter = serializers.SerializerMethodField()
    document = serializers.SerializerMethodField()
    can_user_process=serializers.SerializerMethodField()
    # Priya commented as not used on boranga referral dash yet
    #assigned_officer = serializers.CharField(source='assigned_officer.get_full_name', allow_null=True)
    group_type = serializers.SerializerMethodField()
    # species related fields
    species_number = serializers.SerializerMethodField()
    scientific_name = serializers.SerializerMethodField()
    conservation_list = serializers.SerializerMethodField()
    conservation_category = serializers.SerializerMethodField()
    family = serializers.SerializerMethodField()
    genus = serializers.SerializerMethodField()
    # community related fields
    community_number = serializers.SerializerMethodField()
    community_migrated_id = serializers.SerializerMethodField()
    community_name = serializers.SerializerMethodField()

    class Meta:
        model = ConservationStatusReferral
        fields = (
            'id',
            'processing_status',
            'referral_status',
            'conservation_status_lodgement_date',
            'conservation_status_number',
            'referral',
            'submitter',
            'can_user_process',
            'lodged_on',
            'conservation_status',
            'can_be_processed',
            'document',
            'referral_text',
            'referral_comment',
            #'assigned_officer',
            'group_type',
            'species_number',
            'scientific_name',
            'conservation_list',
            'conservation_category',
            'family',
            'genus',
            'community_number',
            'community_migrated_id',
            'community_name',
        ) 

    def get_referral(self, obj):
        serializer = EmailUserSerializer(retrieve_email_user(obj.referral))
        return serializer.data

    def get_submitter(self,obj):
        # if obj.submitter:
        if hasattr(obj, "submitter") and obj.submitter:
            email_user = retrieve_email_user(obj.submitter)
            return EmailUserSerializer(email_user).data
        else:
            return ""

    def get_document(self, obj):
        # doc = obj.referral_documents.last()
        return [obj.document.name, obj.document._file.url] if obj.document else None

    def get_can_user_process(self,obj):
        request = self.context['request']
        user = request.user._wrapped if hasattr(request.user,'_wrapped') else request.user
        if obj.can_process(user) and obj.can_be_completed:
            if obj.assigned_officer:
                if obj.assigned_officer == user:
                    return True
            else:
                return True
        return False

    def get_group_type(self,obj):
        if obj.conservation_status:
            return obj.conservation_status.application_type.name # if user haven't filled up the form yet(ie. species not selected)

    def get_species_number(self,obj):
        if obj.conservation_status.species:
            return obj.conservation_status.species.species_number
        return ''

    def get_scientific_name(self,obj):
        if obj.conservation_status.species:
            if obj.conservation_status.species.taxonomy:
                return obj.conservation_status.species.taxonomy.scientific_name
        return ''

    def get_community_number(self,obj):
        if obj.conservation_status.community:
            return obj.conservation_status.community.community_number
        return ''

    def get_community_migrated_id(self,obj):
        try:
            taxonomy = CommunityTaxonomy.objects.get(community= obj.conservation_status.community)
            return taxonomy.community_migrated_id
        except CommunityTaxonomy.DoesNotExist:
            return ''

    def get_community_name(self,obj):
        try:
            taxonomy = CommunityTaxonomy.objects.get(community= obj.conservation_status.community)
            return taxonomy.community_name
        except CommunityTaxonomy.DoesNotExist:
            return ''

    def get_conservation_list(self,obj):
        if obj.conservation_status.conservation_list:
            return obj.conservation_status.conservation_list.code
        return ''

    def get_conservation_category(self,obj):
        if obj.conservation_status.conservation_category:
            return obj.conservation_status.conservation_category.code
        return ''
    
    def get_family(self,obj):
        if obj.conservation_status.species:
            if obj.conservation_status.species.taxonomy.family_fk:
                return obj.conservation_status.species.taxonomy.family_fk.scientific_name
        return ''

    def get_genus(self,obj):
        if obj.conservation_status.species:
            if obj.conservation_status.species.taxonomy.genus:
                return obj.conservation_status.species.taxonomy.genus.name
        return ''


class ConservationStatusReferralProposalSerializer(InternalConservationStatusSerializer):
    def get_assessor_mode(self,obj):
        # TODO check if the proposal has been accepted or declined
        request = self.context['request']
        user = request.user._wrapped if hasattr(request.user,'_wrapped') else request.user
        try:
            referral = ConservationStatusReferral.objects.get(conservation_status=obj,referral=user.id)
        except:
            referral = None
        return {
            'assessor_mode': True,
            'assessor_can_assess': referral.can_assess_referral(user) if referral else None,
            'assessor_level': 'referral',
            'assessor_box_view': obj.assessor_comments_view(user)
        }


class ConservationStatusReferralSerializer(serializers.ModelSerializer):
    processing_status = serializers.CharField(source="get_processing_status_display")
    latest_referrals = ConservationStatusProposalReferralSerializer(many=True)
    can_be_completed = serializers.BooleanField()

    class Meta:
        model = ConservationStatusReferral
        fields = '__all__'

    # def get_referral_obj(self, obj):
    #     referral_email_user = retrieve_email_user(obj.referral)
    #     serializer = EmailUserSerializer(referral_email_user)
    #     return serializer.data

    # def get_current_assessor(self, obj):
    #     return {
    #         "id": self.context["request"].user.id,
    #         "name": self.context["request"].user.get_full_name(),
    #         "email": self.context["request"].user.email,
    #     }

    def __init__(self, *args, **kwargs):
        super(ConservationStatusReferralSerializer, self).__init__(*args, **kwargs)
        try:
            self.fields["conservation_status"] = ConservationStatusReferralProposalSerializer(
                context={"request": self.context["request"]}
            )
        except:
            raise

    # def get_can_process(self, obj):
    #     request = self.context["request"]
    #     user = (
    #         request.user._wrapped if hasattr(request.user, "_wrapped") else request.user
    #     )
    #     return obj.can_process(user)


class ConservationStatusAmendmentRequestDocumentSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConservationStatusAmendmentRequestDocument
        fields = ('id', 'name', '_file')
        #fields = '__all__'

class ConservationStatusAmendmentRequestSerializer(serializers.ModelSerializer):
    #reason = serializers.SerializerMethodField()
    cs_amendment_request_documents = ConservationStatusAmendmentRequestDocumentSerializer(many=True, read_only=True)

    class Meta:
        model = ConservationStatusAmendmentRequest
        fields = '__all__'


class ConservationStatusAmendmentRequestDisplaySerializer(serializers.ModelSerializer):
    reason = serializers.SerializerMethodField()

    class Meta:
        model = ConservationStatusAmendmentRequest
        fields = '__all__'

    def get_reason (self,obj):
        #return obj.get_reason_display()
        return obj.reason.reason if obj.reason else None


class ProposedDeclineSerializer(serializers.Serializer):
    reason = serializers.CharField()
    cc_email = serializers.CharField(required=False, allow_null=True)


class ProposedApprovalSerializer(serializers.Serializer):
    # effective_from_date = serializers.DateField(input_formats=['%d/%m/%Y'])
    # effective_to_date = serializers.DateField(input_formats=['%d/%m/%Y'])
    effective_from_date = serializers.DateField()
    effective_to_date = serializers.DateField()
    details = serializers.CharField()
    cc_email = serializers.CharField(required=False,allow_null=True)

class ConservationStatusDocumentSerializer(serializers.ModelSerializer):
	document_category_name = serializers.SerializerMethodField()
	document_sub_category_name = serializers.SerializerMethodField()
	class Meta:
		model = ConservationStatusDocument
		fields = (
			'id',
			'document_number',
			'conservation_status',
			'name',
			'_file',
			'description',
			'input_name',
			'uploaded_date',
			'document_category',
			'document_category_name',
			'document_sub_category',
			'document_sub_category_name',
			'visible',
		)
		read_only_fields = ('id','document_number')

	def get_document_category_name(self,obj):
		if obj.document_category:
			return obj.document_category.document_category_name

	def get_document_sub_category_name(self,obj):
		if obj.document_sub_category:
			return obj.document_sub_category.document_sub_category_name


class SaveConservationStatusDocumentSerializer(serializers.ModelSerializer):
	class Meta:
		model = ConservationStatusDocument
		fields = (
			'id',
			'conservation_status',
			'name',
			'description',
			'input_name',
			'uploaded_date',
			'document_category',
			'document_sub_category',
			)