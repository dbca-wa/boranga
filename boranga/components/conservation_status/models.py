import logging
import datetime
from django.db import models
from django.core.exceptions import ValidationError
from django.contrib.postgres.fields.jsonb import JSONField
from boranga import exceptions
from boranga.components.main.models import (
    CommunicationsLogEntry, 
    UserAction,
    Document,
    RevisionedMixin,
)
from boranga.components.species_and_communities.models import(
    Species,
    Community,
    GroupType,
    DocumentCategory,
    DocumentSubCategory,
)
# from boranga.components.meetings.models import (
#     AgendaItem,
# )
from boranga.ledger_api_utils import retrieve_email_user
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ledger_api_client.managed_models import SystemGroup
import json
from django.db import models,transaction
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from boranga.settings import (
    GROUP_NAME_ASSESSOR,
    GROUP_NAME_APPROVER,
    GROUP_NAME_EDITOR,
)
from boranga.components.main.utils import get_department_user
from boranga.components.main.related_item import RelatedItem
from boranga.components.conservation_status.email import (
    send_conservation_status_referral_email_notification,
    send_conservation_status_referral_recall_email_notification,
    send_conservation_status_amendment_email_notification,
    send_conservation_status_referral_complete_email_notification,
    send_approver_decline_email_notification,
    send_approver_approve_email_notification,
    send_proposal_approver_sendback_email_notification,
    send_conservation_status_decline_email_notification,
    send_conservation_status_approval_email_notification,
    send_assessor_ready_for_agenda_email_notification,
    )


logger = logging.getLogger(__name__)

private_storage = FileSystemStorage(location=settings.BASE_DIR+"/private-media/", base_url='/private-media/')


def update_species_conservation_status_comms_log_filename(instance, filename):
    return '{}/conservation_status/{}/communications/{}'.format(settings.MEDIA_APP_DIR, instance.log_entry.conservation_status.id,filename)

def update_conservation_status_comms_log_filename(instance, filename):
    return '{}/conservation_status/{}/communications/{}'.format(settings.MEDIA_APP_DIR, instance.log_entry.conservation_status.id,filename)

def update_referral_doc_filename(instance, filename):
    return "{}/conservation_status/{}/referral/{}".format(
        settings.MEDIA_APP_DIR, instance.referral.proposal.id, filename
    )

def update_conservation_status_amendment_request_doc_filename(instance, filename):
    return 'conservation_status/{}/amendment_request_documents/{}'.format(instance.conservation_status_amendment_request.conservation_status.id,filename)

def update_conservation_status_doc_filename(instance, filename):
    return '{}/conservation_status/{}/documents/{}'.format(settings.MEDIA_APP_DIR, instance.conservation_status.id,filename)


class ConservationList(models.Model):
    """

    NB: Can have multiple lists per species
    WAPEC       WA Priority Ecological Community List
    DBCA_RLE    Pre-BCA DBCA precursor to IUCN RLE
    WAPS        WA Priority Species List
    SPFN        Wildlife Conservation (Specially Protected Fauna) Notice, Schedules
    IUCN_RLE    IUCN Red List of Ecosystems
    IUCN2012    IUCN Red List Categories and Criteria v3.1(2001) 2nd edition (2012)
    IUCN2001    IUCN Red List Categories and Criteria v3.1(2001)
    EPBC        Environment Protection and Biodiversity Conservation Act 1999
    IUCN1994    IUCN Red List Categories v2.3 (1994)
    WAWCA       Wildlife Conservation Act 1950, Gazettal notice listing

    Has a:
    - N/A
    Used by:
    - SpeciesConservationStatus
    - CommunityConservationStatus
    - ConservationCategory
    - ConservationCriteria
    Is:
    - TBD
    """
    APPROVAL_LEVEL_INTERMEDIATE = 'intermediate'
    APPROVAL_LEVEL_MINISTER = 'minister'
    APPROVAL_LEVEL_CHOICES = (
        (APPROVAL_LEVEL_INTERMEDIATE, 'Intermediate'),
        (APPROVAL_LEVEL_MINISTER, 'Minister'),
    )

    code = models.CharField(max_length=64,
                            default="None")
    label = models.CharField(max_length=512,
                            default="None")
    applies_to_wa = models.BooleanField(default=False)
    applies_to_commonwealth = models.BooleanField(default=False)
    applies_to_international = models.BooleanField(default=False)
    applies_to_species = models.BooleanField(default=False)
    applies_to_communities = models.BooleanField(default=False)
    approval_level = models.CharField('Approval level', max_length=20, choices=APPROVAL_LEVEL_CHOICES,
                                        default=APPROVAL_LEVEL_CHOICES[0][0])

    class Meta:
        app_label = 'boranga'
        verbose_name = "Conservation List"
        verbose_name_plural = "Conservation Lists"
        ordering = ['code']

    def __str__(self):
        return str(self.code)


class ConservationCategory(models.Model):
    """
    Dependent on Conservation List (FK)
    eg.:
    CR  Critically endangered fauna (S1)
    P1  Priority 1
    PD  Presumed Totally Destroyed
    P1  Priority 1
    EN  Endangered fauna (S2)
    P2  Priority 2
    

    Has a:
    - ConservationList
    Used by:
    - SpeciesConservationStatus
    - CommunityConservationStatus
    Is:
    - TBD
    """
    conservation_list = models.ForeignKey(ConservationList, on_delete=models.CASCADE, related_name="conservation_categories", null=True)
    code = models.CharField(max_length=64,
                            default="None")
    label = models.CharField(max_length=512,
                            default="None")

    class Meta:
        app_label = 'boranga'
        verbose_name = "Conservation Category"
        verbose_name_plural = "Conservation Categories"
        ordering = ['code']

    def __str__(self):
        return str(self.code)


class ConservationCriteria(models.Model):
    """
    Dependent on Conservation List (FK)

    Justification for listing as threatened (IUCN-how everything is defined)
    eg:
    A
    Ai
    Aii
    B
    Bi
    Bii
    NB: may have multiple of these per species
    Has a:
    - N/A
    Used by:
    - SpeciesConservationStatus
    - CommunityConservationStatus
    Is:
    - TBD
    """
    conservation_list = models.ForeignKey(ConservationList, on_delete=models.CASCADE, related_name="conservation_criterias", null=True)
    code = models.CharField(max_length=64)
    label = models.CharField(max_length=512,
                            default="None")

    class Meta:
        app_label = 'boranga'
        verbose_name = "Conservation Criteria"
        verbose_name_plural = "Conservation Criterias"
        ordering = ['code']

    def __str__(self):
        return str(self.code)


class ConservationChangeCode(models.Model):
    """
    When the conservation status of a species/community is changed, it can be for a number of reasons. 
    These reasons are represented by change codes.
    """
    code = models.CharField(max_length=32,
                            default="None")
    label = models.CharField(max_length=512,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.code)


class ConservationStatus(models.Model):
    """
    Several lists with different attributes

    NB: Different lists has different different entries
    mainly interest in wa but must accomodte comm as well
    Has a:
    - ConservationChangeCode
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - SpeciesConservationStatus
    - CommunityConservationStatus
    """
    CUSTOMER_STATUS_DRAFT = 'draft'
    CUSTOMER_STATUS_WITH_ASSESSOR = 'with_assessor'
    CUSTOMER_STATUS_READY_FOR_AGENDA = 'ready_for_agenda'
    CUSTOMER_STATUS_AMENDMENT_REQUIRED = 'amendment_required'
    CUSTOMER_STATUS_APPROVED = 'approved'
    CUSTOMER_STATUS_DECLINED = 'declined'
    CUSTOMER_STATUS_DISCARDED = 'discarded'
    CUSTOMER_STATUS_CLOSED = 'closed'
    CUSTOMER_STATUS_PARTIALLY_APPROVED = 'partially_approved'
    CUSTOMER_STATUS_PARTIALLY_DECLINED = 'partially_declined'
    CUSTOMER_STATUS_CHOICES = ((CUSTOMER_STATUS_DRAFT, 'Draft'),
                               (CUSTOMER_STATUS_WITH_ASSESSOR, 'Under Review'),
                               (CUSTOMER_STATUS_READY_FOR_AGENDA, 'In Meeting'),
                               (CUSTOMER_STATUS_AMENDMENT_REQUIRED, 'Amendment Required'),
                               (CUSTOMER_STATUS_APPROVED, 'Approved'),
                               (CUSTOMER_STATUS_DECLINED, 'Declined'),
                               (CUSTOMER_STATUS_DISCARDED, 'Discarded'),
                               (CUSTOMER_STATUS_CLOSED, 'DeListed'),
                               (CUSTOMER_STATUS_PARTIALLY_APPROVED, 'Partially Approved'),
                               (CUSTOMER_STATUS_PARTIALLY_DECLINED, 'Partially Declined'),
                               )

    # List of statuses from above that allow a customer to edit an application.
    CUSTOMER_EDITABLE_STATE = ['draft',
                                'amendment_required',
                            ]

    # List of statuses from above that allow a customer to view an application (read-only)
    CUSTOMER_VIEWABLE_STATE = ['with_assessor', 'ready_for_agenda', 'under_review', 'approved', 'declined','closed','partially_approved', 'partially_declined']

    PROCESSING_STATUS_TEMP = 'temp'
    PROCESSING_STATUS_DRAFT = 'draft'
    PROCESSING_STATUS_WITH_ASSESSOR = 'with_assessor'
    PROCESSING_STATUS_WITH_REFERRAL = 'with_referral'
    PROCESSING_STATUS_WITH_APPROVER = 'with_approver'
    PROCESSING_STATUS_READY_FOR_AGENDA = 'ready_for_agenda'
    PROCESSING_STATUS_AWAITING_APPLICANT_RESPONSE = 'awaiting_applicant_respone'
    PROCESSING_STATUS_AWAITING_ASSESSOR_RESPONSE = 'awaiting_assessor_response'
    PROCESSING_STATUS_AWAITING_RESPONSES = 'awaiting_responses'
    PROCESSING_STATUS_APPROVED = 'approved'
    PROCESSING_STATUS_DECLINED = 'declined'
    PROCESSING_STATUS_DISCARDED = 'discarded'
    PROCESSING_STATUS_CLOSED = 'closed'
    PROCESSING_STATUS_PARTIALLY_APPROVED = 'partially_approved'
    PROCESSING_STATUS_PARTIALLY_DECLINED = 'partially_declined'
    PROCESSING_STATUS_CHOICES = ((PROCESSING_STATUS_DRAFT, 'Draft'),
                                 (PROCESSING_STATUS_WITH_ASSESSOR, 'With Assessor'),
                                 (PROCESSING_STATUS_WITH_REFERRAL, 'With Referral'),
                                 (PROCESSING_STATUS_WITH_APPROVER, 'With Approver'),
                                 (PROCESSING_STATUS_READY_FOR_AGENDA, 'Ready For Agenda'),
                                 (PROCESSING_STATUS_AWAITING_APPLICANT_RESPONSE, 'Awaiting Applicant Response'),
                                 (PROCESSING_STATUS_AWAITING_ASSESSOR_RESPONSE, 'Awaiting Assessor Response'),
                                 (PROCESSING_STATUS_AWAITING_RESPONSES, 'Awaiting Responses'),
                                 (PROCESSING_STATUS_APPROVED, 'Approved'),
                                 (PROCESSING_STATUS_DECLINED, 'Declined'),
                                 (PROCESSING_STATUS_DISCARDED, 'Discarded'),
                                 (PROCESSING_STATUS_CLOSED, 'DeListed'),
                                 (PROCESSING_STATUS_PARTIALLY_APPROVED, 'Partially Approved'),
                                 (PROCESSING_STATUS_PARTIALLY_DECLINED, 'Partially Declined'),
                                )
    REVIEW_STATUS_CHOICES = (
        ('not_reviewed', 'Not Reviewed'), ('awaiting_amendments', 'Awaiting Amendments'), ('amended', 'Amended'),
        ('accepted', 'Accepted'))
    customer_status = models.CharField('Customer Status', max_length=40, choices=CUSTOMER_STATUS_CHOICES,
                                       default=CUSTOMER_STATUS_CHOICES[0][0])

    RECURRENCE_PATTERNS = [(1, 'Month'), (2, 'Year')]
    change_code = models.ForeignKey(ConservationChangeCode, 
                                    on_delete=models.SET_NULL , blank=True, null=True)
    
    APPLICATION_TYPE_CHOICES = (
        ('new_proposal', 'New Application'),
        ('amendment', 'Amendment'),
        ('renewal', 'Renewal'),
        ('external', 'External'),
    )

    RELATED_ITEM_CHOICES = [('species', 'Species'),('community', 'Community'),('agendaitem', 'Meeting Agenda Item')]

    # group_type of application
    application_type = models.ForeignKey(GroupType, on_delete=models.SET_NULL, blank=True, null=True)
    #
    proposal_type = models.CharField('Application Status Type', max_length=40, choices=APPLICATION_TYPE_CHOICES,
                                        default=APPLICATION_TYPE_CHOICES[0][0])

    #species related conservation status
    species = models.ForeignKey(Species, on_delete=models.CASCADE , related_name="conservation_status", null=True, blank=True)

    #communties related conservation status
    community = models.ForeignKey(Community, on_delete=models.CASCADE, related_name="conservation_status", null=True, blank=True)

    conservation_status_number = models.CharField(max_length=9, blank=True, default='')

    # listing details
    conservation_list = models.ForeignKey(ConservationList,
                                             on_delete=models.CASCADE, blank=True, null=True, related_name="curr_conservation_list")
    conservation_category = models.ForeignKey(ConservationCategory, 
                                              on_delete=models.SET_NULL, blank=True, null=True, related_name="curr_conservation_category")
    conservation_criteria = models.ManyToManyField(ConservationCriteria, blank=True, null=True, related_name="curr_conservation_criteria")
    comment = models.CharField(max_length=512, blank=True, null=True)
    review_date = models.DateField(null=True,blank=True)
    recurrence_pattern = models.SmallIntegerField(choices=RECURRENCE_PATTERNS,default=1)
    recurrence_schedule = models.IntegerField(null=True,blank=True)
    proposed_date = models.DateTimeField(auto_now_add=True, null=False, blank=False)
    effective_from = models.DateTimeField(null=True, blank=True)
    effective_to = models.DateTimeField(null=True, blank=True)
    submitter = models.IntegerField(null=True)  # EmailUserRO
    lodgement_date = models.DateTimeField(blank=True, null=True) # TODO confirm if proposed date is the same or different

    assigned_officer = models.IntegerField(null=True) #EmailUserRO
    assigned_approver = models.IntegerField(null=True) #EmailUserRO
    approved_by = models.IntegerField(null=True) #EmailUserRO
    # internal user who edits the approved conservation status(only specific fields)
    #modified_by = models.IntegerField(null=True) #EmailUserRO 
    processing_status = models.CharField('Processing Status', max_length=30, choices=PROCESSING_STATUS_CHOICES,
                                         default=PROCESSING_STATUS_CHOICES[0][0])
    prev_processing_status = models.CharField(max_length=30, blank=True, null=True)
    review_status = models.CharField('Review Status', max_length=30, choices=REVIEW_STATUS_CHOICES,
                                     default=REVIEW_STATUS_CHOICES[0][0])
    proposed_decline_status = models.BooleanField(default=False)
    deficiency_data = models.TextField(null=True, blank=True) # deficiency comment
    assessor_data = models.TextField(null=True, blank=True)  # assessor comment
    # to store the proposed start and end date of proposal
    proposed_issuance_approval = JSONField(blank=True, null=True) # Not used in boranga as created another model to store the details
    approver_comment = models.TextField(blank=True)
    internal_application = models.BooleanField(default=False)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.conservation_status_number)  # TODO: is the most appropriate?

    def save(self, *args, **kwargs):
        super(ConservationStatus, self).save(*args,**kwargs)
        if self.conservation_status_number == '':
            new_conservation_status_id = 'CS{}'.format(str(self.pk))
            self.conservation_status_number = new_conservation_status_id
            self.save()

    @property
    def reference(self):
        return '{}-{}'.format(self.conservation_status_number,self.conservation_status_number) #TODO : the second parameter is lodgement.sequence no. don't know yet what for species it should be

    @property
    def group_type(self):
        if self.species:
            return self.species.group_type.get_name_display()
        elif self.community:
            return self.community.group_type.get_name_display()
        else:
            return self.application_type.get_name_display() # when the form is incomplete

    @property
    def applicant(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return "{} {}".format(
                email_user.first_name,
                email_user.last_name)

    @property
    def applicant_email(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return self.email_user.email

    @property
    def applicant_details(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return "{} {}\n{}".format(
                email_user.first_name,
                email_user.last_name,
                email_user.addresses.all().first())

    @property
    def applicant_address(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return email_user.residential_address

    @property
    def applicant_id(self):
        if self.submitter:
            email_user = retrieve_email_user(self.submitter)
            return self.email_user.id

    @property
    def applicant_type(self):
        if self.submitter:
            #return self.APPLICANT_TYPE_SUBMITTER
            return 'SUB'

    @property
    def applicant_field(self):
        # if self.org_applicant:
        #     return 'org_applicant'
        # elif self.proxy_applicant:
        #     return 'proxy_applicant'
        # else:
        #     return 'submitter'
        return 'submitter'

    def log_user_action(self, action, request):
        return ConservationStatusUserAction.log_action(self, action, request.user.id)

    @property
    def is_assigned(self):
        return self.assigned_officer is not None
    
    @property
    def can_officer_process(self):
        """
        :return: True if the application is in one of the processable status for Assessor role.
        """
        officer_view_state = ['draft','approved','declined','temp','discarded', 'closed']
        if self.processing_status in officer_view_state:
            return False
        else:
            return True
    
    @property
    def can_officer_edit(self):
        """
        :return: True if the application is in one of the internal editable status for internal edit(few fields).
        """
        #officer_view_state = ['draft','approved','declined','temp','discarded', 'closed']
        officer_edit_state = ['approved']
        if self.processing_status in officer_edit_state:
            return True
        else:
            return False

    @property
    def can_user_edit(self):
        """
        :return: True if the application is in one of the editable status.
        """
        return self.customer_status in self.CUSTOMER_EDITABLE_STATE

    @property
    def can_user_view(self):
        """
        :return: True if the application is in one of the approved status.
        """
        return self.customer_status in self.CUSTOMER_VIEWABLE_STATE

    @property
    def is_discardable(self):
        """
        An application can be discarded by a customer if:
        1 - It is a draft
        2- or if the application has been pushed back to the user
        """
        return self.customer_status == 'draft' or self.processing_status == 'awaiting_applicant_response'

    @property
    def is_deletable(self):
        """
        An application can be deleted only if it is a draft and it hasn't been lodged yet
        :return:
        """
        return self.customer_status == 'draft' and not self.conservation_status_number

    @property
    def latest_referrals(self):
        return self.referrals.all()[:2]

    @property
    def is_flora_application(self):
        if self.application_type.name==GroupType.GROUP_TYPE_FLORA:
            return True
        return False

    @property
    def is_fauna_application(self):
        if self.application_type.name==GroupType.GROUP_TYPE_FAUNA:
            return True
        return False

    @property
    def is_community_application(self):
        if self.application_type.name==GroupType.GROUP_TYPE_COMMUNITY:
            return True
        return False

    @property
    def allowed_assessors(self):
        # if self.processing_status == 'with_approver':
        #     group = self.__approver_group()
        # elif self.processing_status =='with_qa_officer':
        #     group = QAOfficerGroup.objects.get(default=True)
        # else:
        #     group = self.__assessor_group()
        # return group.members.all() if group else []

        group = None
        # TODO: Take application_type into account
        if self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_WITH_APPROVER,
        ]:
            group = self.get_approver_group()
        elif self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL,
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR,
            ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA,
        ]:
            group = self.get_assessor_group()
        # for tO SHOW edit action on dashoard of CS
        elif self.processing_status in [
            ConservationStatus.PROCESSING_STATUS_APPROVED,
        ]:
            group = self.get_editor_group()
        users = (
            list(
                map(
                    lambda id: retrieve_email_user(id),
                    group.get_system_group_member_ids(),
                )
            )
            if group
            else []
        )
        return users

    def get_assessor_group(self):
        # TODO: Take application_type into account
        return SystemGroup.objects.get(name=GROUP_NAME_ASSESSOR)

    def get_approver_group(self):
        # TODO: Take application_type into account
        return SystemGroup.objects.get(name=GROUP_NAME_APPROVER)

    # Group for editing the Approved CS(only specific fields)
    def get_editor_group(self):
        return SystemGroup.objects.get(name=GROUP_NAME_EDITOR)

    @property
    def assessor_recipients(self):
        logger.info("assessor_recipients")
        recipients = []
        group_ids = self.get_assessor_group().get_system_group_member_ids()
        for id in group_ids:
            logger.info(id)
            recipients.append(EmailUser.objects.get(id=id).email)
        return recipients

    @property
    def approver_recipients(self):
        logger.info("assessor_recipients")
        recipients = []
        group_ids = self.get_approver_group().get_system_group_member_ids()
        for id in group_ids:
            logger.info(id)
            recipients.append(EmailUser.objects.get(id=id).email)
        return recipients

    #Check if the user is member of assessor group for the CS Proposal
    def is_assessor(self,user):
            return user.id in self.get_assessor_group().get_system_group_member_ids()

    #Check if the user is member of assessor group for the CS Proposal
    def is_approver(self,user):
            return user.id in self.get_assessor_group().get_system_group_member_ids()


    def can_assess(self,user):
        logger.info("can assess")
        logger.info("user")
        logger.info(type(user))
        logger.info(user)
        logger.info(user.id)
        if self.processing_status in [
            # "on_hold",
            "ready_for_agenda",
            "with_assessor",
            "with_referral",
            "with_assessor_conditions",
        ]:
            logger.info("self.__assessor_group().get_system_group_member_ids()")
            logger.info(self.get_assessor_group().get_system_group_member_ids())
            return user.id in self.get_assessor_group().get_system_group_member_ids()
        elif self.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_APPROVER:
            return user.id in self.get_approver_group().get_system_group_member_ids()
        elif self.processing_status == ConservationStatus.PROCESSING_STATUS_APPROVED:
            return user.id in self.get_editor_group().get_system_group_member_ids()
        else:
            return False

    def assessor_comments_view(self,user):

        if (self.processing_status == 'with_assessor' 
            or self.processing_status == 'with_referral' or 
            self.processing_status == 'with_approver'
        ):
            try:
                referral = ConservationStatusReferral.objects.get(conservation_status=self,referral=user.id)
            except:
                referral = None
            if referral:
                return True
            # elif self.__assessor_group() in user.proposalassessorgroup_set.all():
            elif user.id in self.get_assessor_group().get_system_group_member_ids():
                return True
            # elif self.__approver_group() in user.proposalapprovergroup_set.all():
            elif user.id in self.get_approver_group().get_system_group_member_ids():
                return True
            else:
                return False
        #TODO not sure if we need to show the comments while editing the approved CS
        # if(self.processing_status == 'approved'):
        #     if user.id in self.get_editor_group().get_system_group_member_ids():
        #         return True

    @property   
    def status_without_assessor(self):
        status_without_assessor = ['with_approver','approved','closed','declined','draft', 'with_referral']
        if self.processing_status in status_without_assessor:
            return True
        return False

    def has_assessor_mode(self,user):
        status_without_assessor = [
            "with_approver",
            "approved",
            "closed",
            "declined",
            "draft",
        ]
        if self.processing_status in status_without_assessor:
            # For Editing the 'Approved' conservation status for authorised group
            if self.processing_status == ConservationStatus.PROCESSING_STATUS_APPROVED:
                return user.id in self.get_editor_group().get_system_group_member_ids()
            else:
                return False
        
        else:
            if self.assigned_officer:
                if self.assigned_officer == user.id:
                    return (
                        user.id
                        in self.get_assessor_group().get_system_group_member_ids()
                    )
                else:
                    return False
            else:
                return (
                    user.id in self.get_assessor_group().get_system_group_member_ids()
                )
    
    def assign_officer(self,request,officer):
        with transaction.atomic():
            try:
                if not self.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                if not self.can_assess(officer):
                    raise ValidationError('The selected person is not authorised to be assigned to this proposal')
                if self.processing_status == 'with_approver':
                    if officer != self.assigned_approver:
                        self.assigned_approver = officer.id
                        self.save()
                        # Create a log entry for the proposal
                        self.log_user_action(
                            ConservationStatusUserAction.ACTION_ASSIGN_TO_APPROVER.format(
                            self.conservation_status_number,
                            '{}({})'.format(officer.get_full_name(),officer.email),
                            ),
                            request,
                        )
                        # Create a log entry for the organisation
                        # applicant_field=getattr(self, self.applicant_field)
                        # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_ASSIGN_TO_APPROVER.format(self.id,'{}({})'.format(officer.get_full_name(),officer.email)),request)
                else:
                    if officer != self.assigned_officer:
                        self.assigned_officer = officer.id
                        self.save()
                        # Create a log entry for the proposal
                        self.log_user_action(
                            ConservationStatusUserAction.ACTION_ASSIGN_TO_ASSESSOR.format(
                                self.conservation_status_number,
                                '{}({})'.format(officer.get_full_name(),officer.email),
                                ),
                            request,
                        )
                        # Create a log entry for the organisation
                        # applicant_field=getattr(self, self.applicant_field)
                        # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_ASSIGN_TO_ASSESSOR.format(self.id,'{}({})'.format(officer.get_full_name(),officer.email)),request)
            except:
                raise

    def unassign(self,request):
        with transaction.atomic():
            try:
                if not self.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                if self.processing_status == 'with_approver':
                    if self.assigned_approver:
                        self.assigned_approver = None
                        self.save()
                        # Create a log entry for the proposal
                        self.log_user_action(
                            ConservationStatusUserAction.ACTION_UNASSIGN_APPROVER.format(
                                self.conservation_status_number),
                            request,
                        )
                        # Create a log entry for the organisation
                        # applicant_field=getattr(self, self.applicant_field)
                        # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_UNASSIGN_APPROVER.format(self.id),request)
                else:
                    if self.assigned_officer:
                        self.assigned_officer = None
                        self.save()
                        # Create a log entry for the proposal
                        self.log_user_action(
                            ConservationStatusUserAction.ACTION_UNASSIGN_ASSESSOR.format(
                                self.conservation_status_number),
                            request,
                        )
                        # Create a log entry for the organisation
                        # applicant_field=getattr(self, self.applicant_field)
                        # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_UNASSIGN_ASSESSOR.format(self.id),request)
            except:
                raise

    def send_referral(self,request,referral_email,referral_text):
        with transaction.atomic():
            try:
                referral_email = referral_email.lower()
                if (
                    self.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR or 
                    self.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
                ):
                    self.processing_status = ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
                    self.save()
                    referral = None
                    # Check if the user is in ledger
                    try:
                        user = EmailUser.objects.get(email__icontains=referral_email)
                    except EmailUser.DoesNotExist:
                        # Validate if it is a deparment user
                        department_user = get_department_user(referral_email)
                        if not department_user:
                            raise ValidationError('The user you want to send the referral to is not a member of the department')
                        # Check if the user is in ledger or create

                        user,created = EmailUser.objects.get_or_create(email=department_user['email'].lower())
                        if created:
                            user.first_name = department_user['given_name']
                            user.last_name = department_user['surname']
                            user.save()
                    try:
                        ConservationStatusReferral.objects.get(referral=user.id,conservation_status=self)
                        raise ValidationError('A referral has already been sent to this user')
                    except ConservationStatusReferral.DoesNotExist:
                        # Create Referral
                        referral = ConservationStatusReferral.objects.create(
                            conservation_status = self,
                            referral=user.id,
                            sent_by=request.user.id,
                            text=referral_text,
                            assigned_officer=request.user.id, # TODO should'nt use assigned officer as per das
                        )
                    # Create a log entry for the proposal
                    self.log_user_action(
                        ConservationStatusUserAction.ACTION_SEND_REFERRAL_TO.format(
                            referral.id, 
                            self.conservation_status_number, 
                            '{}({})'.format(user.get_full_name(), user.email),
                        ), 
                        request,
                    )
                    # Create a log entry for the organisation
                    if self.applicant:
                        pass
                        #self.applicant.log_user_action(ProposalUserAction.ACTION_SEND_REFERRAL_TO.format(referral.id, self.lodgement_number, '{}({})'.format(user.get_full_name(), user.email)), request)
                    # send email
                    send_conservation_status_referral_email_notification(referral,request)
                else:
                    raise exceptions.ConservationStatusReferralCannotBeSent()
            except:
                raise

    @property
    def amendment_requests(self):
        qs =ConservationStatusAmendmentRequest.objects.filter(conservation_status = self)
        return qs
    
    def move_to_status(self,request,status, approver_comment):
        if not self.can_assess(request.user):
            raise exceptions.ProposalNotAuthorized()
        if status in ['with_assessor','with_approver']:
            if self.processing_status == 'with_referral' or self.can_user_edit:
                raise ValidationError('You cannot change the current status at this time')
            if self.processing_status != status:
                if self.processing_status =='with_approver':
                    self.approver_comment=''
                    if approver_comment:
                        self.approver_comment = approver_comment
                        self.save()
                        send_proposal_approver_sendback_email_notification(request, self)
                self.processing_status = status
                self.save()
                # if status=='with_assessor_requirements':
                #     self.add_default_requirements()

                # Create a log entry for the Conservation status proposal
                if self.processing_status == self.PROCESSING_STATUS_WITH_ASSESSOR:
                    self.log_user_action(ConservationStatusUserAction.ACTION_BACK_TO_PROCESSING.format(self.conservation_status_number),request)
                # elif self.processing_status == self.PROCESSING_STATUS_WITH_ASSESSOR_REQUIREMENTS:
                #     self.log_user_action(ProposalUserAction.ACTION_ENTER_REQUIREMENTS.format(self.id),request)
        else:
            raise ValidationError('The provided status cannot be found.')
    
    def proposed_decline(self,request,details):
        with transaction.atomic():
            try:
                if not self.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                if self.processing_status != 'with_assessor':
                    raise ValidationError('You cannot propose to decline if it is not with assessor')

                reason = details.get('reason')
                ConservationStatusDeclinedDetails.objects.update_or_create(
                    conservation_status = self,
                    defaults={'officer': request.user.id, 'reason': reason, 'cc_email': details.get('cc_email',None)}
                )
                self.proposed_decline_status = True
                approver_comment = ''
                self.move_to_status(request,'with_approver', approver_comment)
                # Log proposal action
                self.log_user_action(ConservationStatusUserAction.ACTION_PROPOSED_DECLINE.format(self.conservation_status_number),request)
                # Log entry for organisation
                # applicant_field=getattr(self, self.applicant_field)
                # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_PROPOSED_DECLINE.format(self.id),request)

                send_approver_decline_email_notification(reason, request, self)
            except:
                raise
    
    def final_decline(self,request,details):
        with transaction.atomic():
            try:
                if not self.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                # if self.processing_status != 'with_approver':
                #     raise ValidationError('You cannot decline if it is not with approver')
                if self.processing_status not in ('with_assessor','ready_for_agenda'):
                    raise ValidationError('You cannot decline the proposal if it is not with an assessor')

                conservation_status_decline, success = ConservationStatusDeclinedDetails.objects.update_or_create(
                    conservation_status = self,
                    defaults={'officer':request.user.id,'reason':details.get('reason'),'cc_email':details.get('cc_email',None)}
                )
                self.proposed_decline_status = True
                self.processing_status = 'declined'
                self.customer_status = 'declined'
                self.save()
                # Log proposal action
                self.log_user_action(ConservationStatusUserAction.ACTION_DECLINE.format(self.conservation_status_number),request)
                # Log entry for organisation
                # applicant_field=getattr(self, self.applicant_field)
                # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_DECLINE.format(self.id),request)
                send_conservation_status_decline_email_notification(self,request, conservation_status_decline)
            except:
                raise

    def proposed_approval(self,request,details):
        with transaction.atomic():
            try:
                if not self.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                if self.processing_status != 'with_assessor':
                    raise ValidationError('You cannot propose for approval if it is not with assessor')
                # self.proposed_issuance_approval = {
                #     'effective_from_date' : details.get('effective_from_date').strftime('%d/%m/%Y'),
                #     'effective_to_date' : details.get('effective_to_date').strftime('%d/%m/%Y'),
                #     'details': details.get('details'),
                #     'cc_email':details.get('cc_email')
                # }
                ConservationStatusIssuanceApprovalDetails.objects.update_or_create(
                    conservation_status = self,
                    defaults={
                        'officer': request.user.id, 
                        'effective_from_date': details.get('effective_from_date'), 
                        'effective_to_date' : details.get('effective_to_date'),
                        'details': details.get('details'),
                        'cc_email': details.get('cc_email',None)}
                )
                self.proposed_decline_status = False
                approver_comment = ''
                self.move_to_status(request,'with_approver', approver_comment)
                self.assigned_officer = None
                self.save()
                # Log proposal action
                self.log_user_action(ConservationStatusUserAction.ACTION_PROPOSED_APPROVAL.format(self.conservation_status_number),request)
                # Log entry for organisation
                # applicant_field=getattr(self, self.applicant_field)
                # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_PROPOSED_APPROVAL.format(self.id),request)

                send_approver_approve_email_notification(request, self)
            except:
                raise
    
    def final_approval(self,request,details):
        from boranga.helpers import is_departmentUser
        with transaction.atomic():
            try:
                self.proposed_decline_status = False

                if not self.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                # if self.processing_status != 'with_approver':
                #     raise ValidationError('You cannot issue the approval if it is not with an approver')
                if self.processing_status not in ('with_assessor','ready_for_agenda'):
                    raise ValidationError('You cannot issue the approval if it is not with an assessor')
                # not approve if the cs not set ina ny meeting for list with minister
                if self.conservation_list.approval_level == 'minister':
                    # TODO may I need to check the the meeting status as schedules as well
                    cs_meeting_count = self.agendaitem_set.filter(meeting__processing_status="scheduled").count()
                    if cs_meeting_count==0:
                        raise ValidationError('You cannot issue the approval as meeting not scheduled for the minister approval')
                # Add the approval document first to to get the reference id in below model
                proposal_approval_document = request.data['proposal_approval_document']
                if proposal_approval_document != 'null':
                    try:
                        #document = self.documents.get(input_name=str(proposal_approval_document))
                        document = self.documents.get(name=str(proposal_approval_document)) #Priya, commented above as input_name=approval shouldn't be shown in documents tab 
                    except:
                        # can also use name = proposal_approval_document.name rather than str(proposal_approval_document)
                        #document = self.documents.get_or_create(input_name='str(proposal_approval_document)', name=str(proposal_approval_document))[0]
                        document = self.documents.get_or_create(input_name='conservation_status_approval_doc', name=str(proposal_approval_document))[0]  #Priya, commented above as input_name=approval shouldn't be shown in documents tab 

                    document.name = str(proposal_approval_document)
                    document._file = proposal_approval_document

                    document.save()
                    d=ConservationStatusDocument.objects.get(id=document.id)
                else:
                    d = None
                # assign document id to the IssuanceApprovalDetails
                ConservationStatusIssuanceApprovalDetails.objects.update_or_create(
                    conservation_status = self,
                    defaults={
                        'officer': request.user.id, 
                        'effective_from_date': details.get('effective_from_date'), 
                        'effective_to_date' : details.get('effective_to_date'),
                        'details': details.get('details'),
                        'cc_email': details.get('cc_email',None),
                        'conservation_status_approval_document':d,
                        }
                )
                if is_departmentUser(request):
                    # needed because external users come through this workflow following 'awaiting_payment; status
                    self.approved_by = request.user.id

                self.processing_status = 'approved'
                self.customer_status = 'approved'
                # Log proposal action
                self.log_user_action(ConservationStatusUserAction.ACTION_APPROVE_PROPOSAL_.format(self.conservation_status_number),request)
                # Log entry for organisation
                # applicant_field=getattr(self, self.applicant_field)
                # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_ISSUE_APPROVAL_.format(self.id),request)

                if self.processing_status == self.PROCESSING_STATUS_APPROVED:
                    # TODO if it is an ammendment proposal then check appropriately
                    checking_proposal = self
                    if self.proposal_type == 'renewal':
                        pass
                        # if self.previous_application:
                        #     previous_approval = self.previous_application.approval
                        #     approval,created = Approval.objects.update_or_create(
                        #         current_proposal = checking_proposal,
                        #         defaults = {
                        #             'issue_date' : timezone.now(),
                        #             'expiry_date' : datetime.datetime.strptime(self.proposed_issuance_approval.get('expiry_date'), '%d/%m/%Y').date(),
                        #             'start_date' : datetime.datetime.strptime(self.proposed_issuance_approval.get('start_date'), '%d/%m/%Y').date(),
                        #             'submitter': self.submitter,
                        #             #'org_applicant' : self.applicant if isinstance(self.applicant, Organisation) else None,
                        #             #'proxy_applicant' : self.applicant if isinstance(self.applicant, EmailUser) else None,
                        #             'org_applicant' : self.org_applicant,
                        #             'proxy_applicant' : self.proxy_applicant,
                        #             'lodgement_number': previous_approval.lodgement_number
                        #         }
                        #     )
                        #     if created:
                        #         previous_approval.replaced_by = approval
                        #         previous_approval.save()

                    elif self.proposal_type == 'amendment':
                        pass
                        # if self.previous_application:
                        #     previous_approval = self.previous_application.approval
                        #     approval,created = Approval.objects.update_or_create(
                        #         current_proposal = checking_proposal,
                        #         defaults = {
                        #             'issue_date' : timezone.now(),
                        #             'expiry_date' : datetime.datetime.strptime(self.proposed_issuance_approval.get('expiry_date'), '%d/%m/%Y').date(),
                        #             'start_date' : datetime.datetime.strptime(self.proposed_issuance_approval.get('start_date'), '%d/%m/%Y').date(),
                        #             'submitter': self.submitter,
                        #             #'org_applicant' : self.applicant if isinstance(self.applicant, Organisation) else None,
                        #             #'proxy_applicant' : self.applicant if isinstance(self.applicant, EmailUser) else None,
                        #             'org_applicant' : self.org_applicant,
                        #             'proxy_applicant' : self.proxy_applicant,
                        #             'lodgement_number': previous_approval.lodgement_number
                        #         }
                        #     )
                        #     if created:
                        #         previous_approval.replaced_by = approval
                        #         previous_approval.save()
                    else:
                        # approval,created = Approval.objects.update_or_create(
                        #     current_proposal = checking_proposal,
                        #     defaults = {
                        #         'issue_date' : timezone.now(),
                        #         'expiry_date' : datetime.datetime.strptime(self.proposed_issuance_approval.get('expiry_date'), '%d/%m/%Y').date(),
                        #         'start_date' : datetime.datetime.strptime(self.proposed_issuance_approval.get('start_date'), '%d/%m/%Y').date(),
                        #         'submitter': self.submitter,
                        #         #'org_applicant' : self.applicant if isinstance(self.applicant, Organisation) else None,
                        #         #'proxy_applicant' : self.applicant if isinstance(self.applicant, EmailUser) else None,
                        #         'org_applicant' : self.org_applicant,
                        #         'proxy_applicant' : self.proxy_applicant,
                        #         #'extracted_fields' = JSONField(blank=True, null=True)
                        #     }
                        # )

                        # Delist/Close the previous approved version for that species/community
                        try:
                            if self.species:
                                previous_approved_wa_version= ConservationStatus.objects.get(species=self.species, processing_status='approved', conservation_list__applies_to_wa=True)
                                if previous_approved_wa_version:
                                    previous_approved_wa_version.customer_status='closed'
                                    previous_approved_wa_version.processing_status='closed'
                                    previous_approved_wa_version.save()
                                    #add the log_user_action
                                    self.log_user_action(ConservationStatusUserAction.ACTION_CLOSE_CONSERVATIONSTATUS.format(previous_approved_wa_version.conservation_status_number),request)
                            elif self.community:
                                previous_approved_wa_version= ConservationStatus.objects.get(community=self.community, processing_status='approved', conservation_list__applies_to_wa=True)
                                if previous_approved_wa_version:
                                    previous_approved_wa_version.customer_status='closed'
                                    previous_approved_wa_version.processing_status='closed'
                                    previous_approved_wa_version.save()
                                    #add the log_user_action
                                    self.log_user_action(ConservationStatusUserAction.ACTION_CLOSE_CONSERVATIONSTATUS.format(previous_approved_wa_version.conservation_status_number),request)

                        except ConservationStatus.DoesNotExist:
                            pass
                    
                    #send Proposal approval email with attachment
                    send_conservation_status_approval_email_notification(self,request)
                    # self.save(version_comment='Final Approval: {}'.format(self.approval.lodgement_number))
                    self.save() 
                    self.documents.all().update(can_delete=False)

            except:
                raise
    
    def proposed_ready_for_agenda(self,request):
        with transaction.atomic():
            try:
                if not self.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                if self.processing_status != 'with_assessor':
                    raise ValidationError('You cannot propose for ready for agenda if it is not with assessor')
                # self.proposed_issuance_approval = {
                #     'effective_from_date' : details.get('effective_from_date').strftime('%d/%m/%Y'),
                #     'effective_to_date' : details.get('effective_to_date').strftime('%d/%m/%Y'),
                #     'details': details.get('details'),
                #     'cc_email':details.get('cc_email')
                # }
                # self.move_to_status(request,'with_approver', approver_comment)
                # self.assigned_officer = None
                self.processing_status = 'ready_for_agenda'
                self.customer_status = 'ready_for_agenda'
                self.save()
                # Log proposal action
                self.log_user_action(ConservationStatusUserAction.ACTION_PROPOSED_READY_FOR_AGENDA.format(self.conservation_status_number),request)
                # Log entry for organisation
                # applicant_field=getattr(self, self.applicant_field)
                # applicant_field.log_user_action(ConservationStatusUserAction.ACTION_PROPOSED_APPROVAL.format(self.id),request)

                send_assessor_ready_for_agenda_email_notification(request, self)
            except:
                raise
    
    def get_related_items(self,filter_type, **kwargs):
        return_list = []
        # import ipdb; ipdb.set_trace()
        if filter_type == 'all':
            related_field_names = ['species', 'community', 'agendaitem']
        else:
            related_field_names = [filter_type,]
        all_fields = self._meta.get_fields()
        for a_field in all_fields:
            if a_field.name in related_field_names:
                field_objects = []
                if a_field.is_relation:
                    if a_field.many_to_many:
                        pass
                    elif a_field.many_to_one:  # foreign key
                        field_objects = [getattr(self, a_field.name),]
                    elif a_field.one_to_many:  # reverse foreign key
                        field_objects = a_field.related_model.objects.filter(**{a_field.remote_field.name: self})
                    elif a_field.one_to_one:
                        if hasattr(self, a_field.name):
                            field_objects = [getattr(self, a_field.name),]
                for field_object in field_objects:
                    if field_object:
                        related_item = field_object.as_related_item
                        return_list.append(related_item)

        # serializer = RelatedItemsSerializer(return_list, many=True)
        # return serializer.data
        return return_list

    @property
    def as_related_item(self):
        related_item = RelatedItem(
            identifier=self.related_item_identifier,
            model_name=self._meta.verbose_name,
            descriptor=self.related_item_descriptor,
            status=self.related_item_status,
            action_url='<a href=/internal/conservation_status/{} target="_blank">View</a>'.format(self.id)
        )
        return related_item

    @property
    def related_item_identifier(self):
        return self.conservation_status_number

    @property
    def related_item_descriptor(self):
        if self.conservation_list:
            return self.conservation_list.code

    @property
    def related_item_status(self):
        return self.get_processing_status_display


class ConservationStatusLogEntry(CommunicationsLogEntry):
    conservation_status = models.ForeignKey(ConservationStatus, related_name='comms_logs', on_delete=models.CASCADE)

    def __str__(self):
        return '{} - {}'.format(self.reference, self.subject)

    class Meta:
        app_label = 'boranga'

    def save(self, **kwargs):
        # save the application reference if the reference not provided
        if not self.reference:
            self.reference = self.conservation_status.reference
        super(ConservationStatusLogEntry, self).save(**kwargs)


class ConservationStatusLogDocument(Document):
    log_entry = models.ForeignKey('ConservationStatusLogEntry',related_name='documents', on_delete=models.CASCADE)
    _file = models.FileField(upload_to=update_conservation_status_comms_log_filename, max_length=512, storage=private_storage)

    class Meta:
        app_label = 'boranga'


class ConservationStatusUserAction(UserAction):
    #ConservationStatus Proposal
    ACTION_EDIT_CONSERVATION_STATUS= "Edit Conservation Status {}"
    ACTION_LODGE_PROPOSAL = "Lodge proposal for conservation status {}"
    ACTION_SAVE_APPLICATION = "Save proposal {}"
    ACTION_EDIT_APPLICATION = "Edit proposal {}"
    ACTION_ASSIGN_TO_ASSESSOR = "Assign conservation status proposal {} to {} as the assessor"
    ACTION_UNASSIGN_ASSESSOR = "Unassign assessor from conservation status proposal {}"
    ACTION_ASSIGN_TO_APPROVER = "Assign conservation status proposal {} to {} as the approver"
    ACTION_UNASSIGN_APPROVER = "Unassign approver from conservation status proposal {}"
    ACTION_DECLINE = "Decline conservation status application {}"
    ACTION_APPROVE_PROPOSAL_ = "Approve conservation status  proposal {}"
    ACTION_CLOSE_CONSERVATIONSTATUS = "De list conservation status {}"
    ACTION_DISCARD_PROPOSAL = "Discard conservation status proposal {}"
    ACTION_APPROVAL_LEVEL_DOCUMENT = "Assign Approval level document {}"

    #Amendment
    ACTION_ID_REQUEST_AMENDMENTS = "Request amendments"
    
    # Assessors
    ACTION_SAVE_ASSESSMENT_ = "Save assessment {}"
    ACTION_CONCLUDE_ASSESSMENT_ = "Conclude assessment {}"
    ACTION_PROPOSED_READY_FOR_AGENDA = "Conservation status proposal {} has been proposed for ready for agenda"
    ACTION_PROPOSED_APPROVAL = "Conservation status proposal {} has been proposed for approval"
    ACTION_PROPOSED_DECLINE = "Conservation status proposal {} has been proposed for decline"

    # Referrals
    ACTION_SEND_REFERRAL_TO = "Send referral {} for conservation status proposal {} to {}"
    ACTION_RESEND_REFERRAL_TO = "Resend referral {} for conservation status proposal {} to {}"
    ACTION_REMIND_REFERRAL = "Send reminder for referral {} for conservation status proposal {} to {}"
    ACTION_BACK_TO_PROCESSING = "Back to processing for conservation status proposal {}"
    RECALL_REFERRAL = "Referral {} for conservation status proposal {} has been recalled"
    COMMENT_REFERRAL = "Referral {} for conservation status proposal {} has been commented by {}"
    CONCLUDE_REFERRAL = "Referral {} for conservation status proposal {} has been concluded by {}"


    class Meta:
        app_label = 'boranga'
        ordering = ('-when',)

    @classmethod
    def log_action(cls, conservation_status, action, user):
        return cls.objects.create(
            conservation_status=conservation_status,
            who=user,
            what=str(action)
        )

    conservation_status= models.ForeignKey(ConservationStatus, related_name='action_logs', on_delete=models.CASCADE)


class ConservationStatusDocument(Document):
    document_number = models.CharField(max_length=9, blank=True, default='')
    conservation_status = models.ForeignKey('ConservationStatus',related_name='documents', on_delete=models.CASCADE)
    _file = models.FileField(upload_to=update_conservation_status_doc_filename, max_length=512, storage=private_storage)
    input_name = models.CharField(max_length=255,null=True,blank=True)
    can_delete = models.BooleanField(default=True) # after initial submit prevent document from being deleted
    can_hide= models.BooleanField(default=False) # after initial submit, document cannot be deleted but can be hidden
    hidden=models.BooleanField(default=False) # after initial submit prevent document from being deleted # Priya alternatively used below visible field in boranga
    visible = models.BooleanField(default=True) # to prevent deletion on file system, hidden and still be available in history
    document_category = models.ForeignKey(DocumentCategory,
                                          null=True,
                                          blank=True,
                                          on_delete=models.SET_NULL)
    document_sub_category = models.ForeignKey(DocumentSubCategory,
                                          null=True,
                                          blank=True,
                                          on_delete=models.SET_NULL)

    class Meta:
        app_label = 'boranga'
        verbose_name = "Conservation Status Document"

    def save(self, *args, **kwargs):
        # Prefix "D" char to document_number.
        super(ConservationStatusDocument, self).save(*args,**kwargs)
        if self.document_number == '':
            new_document_id = 'D{}'.format(str(self.pk))
            self.document_number = new_document_id
            self.save()

    def add_documents(self, request):
        with transaction.atomic():
            try:
                # save the files
                data = json.loads(request.data.get('data'))
                # if not data.get('update'):
                #     documents_qs = self.filter(input_name='species_doc', visible=True)
                #     documents_qs.delete()
                for idx in range(data['num_files']):
                    _file = request.data.get('file-'+str(idx))
                    self._file=_file
                    self.name=_file.name
                    self.input_name = data['input_name']
                    self.can_delete = True
                    self.save()
                # end save documents
                self.save()
            except:
                raise
        return


class ConservationStatusDeclinedDetails(models.Model):
    # proposal = models.OneToOneField(Proposal, related_name='declined_details')
    conservation_status = models.OneToOneField(ConservationStatus, on_delete=models.CASCADE)
    # officer = models.ForeignKey(EmailUser, null=False, on_delete=models.CASCADE)
    officer = models.IntegerField()  # EmailUserRO
    reason = models.TextField(blank=True)
    cc_email = models.TextField(null=True)

    class Meta:
        app_label = "boranga"

class ConservationStatusIssuanceApprovalDetails(models.Model):
    conservation_status = models.OneToOneField(ConservationStatus, on_delete=models.CASCADE)
    # officer = models.ForeignKey(EmailUser, null=False, on_delete=models.CASCADE)
    officer = models.IntegerField()  # EmailUserRO
    effective_from_date = models.DateField(null=True, blank=True)
    effective_to_date = models.DateField(null=True, blank=True)
    details = models.TextField(blank=True)
    cc_email = models.TextField(null=True)
    conservation_status_approval_document = models.ForeignKey(ConservationStatusDocument, blank=True, null=True, related_name='conservation_status_approval_document', on_delete=models.SET_NULL)

    class Meta:
        app_label = "boranga"


class ConservationStatusReferralDocument(Document):
    referral = models.ForeignKey(
        "ConservationStatusReferral", related_name="referral_documents", on_delete=models.CASCADE
    )
    _file = models.FileField(upload_to=update_referral_doc_filename, max_length=512, storage=private_storage)
    input_name = models.CharField(max_length=255, null=True, blank=True)
    can_delete = models.BooleanField(
        default=True
    )  # after initial submit prevent document from being deleted

    def delete(self):
        if self.can_delete:
            # TODO Don't have ConservationDocument yet
            #return super(ProposalDocument, self).delete()
            return self.can_delete
        logger.info(
            "Cannot delete existing document object after Application has been submitted (including document submitted before Application pushback to status Draft): {}".format(
                self.name
            )
        )

    class Meta:
        app_label = "boranga"

#class ConservationStatusReferral(RevisionedMixin):
class ConservationStatusReferral(models.Model):
    SENT_CHOICES = ((1, "Sent From Assessor"), (2, "Sent From Referral"))
    PROCESSING_STATUS_WITH_REFERRAL = "with_referral"
    PROCESSING_STATUS_RECALLED = "recalled"
    PROCESSING_STATUS_COMPLETED = "completed"
    PROCESSING_STATUS_CHOICES = (
        (PROCESSING_STATUS_WITH_REFERRAL, "Awaiting"),
        (PROCESSING_STATUS_RECALLED, "Recalled"),
        (PROCESSING_STATUS_COMPLETED, "Completed"),
    )
    lodged_on = models.DateTimeField(auto_now_add=True)
    conservation_status = models.ForeignKey(
        ConservationStatus, related_name="referrals", on_delete=models.CASCADE
    )
    sent_by = models.IntegerField()  # EmailUserRO
    referral = models.IntegerField()  # EmailUserRO
    linked = models.BooleanField(default=False)
    sent_from = models.SmallIntegerField(
        choices=SENT_CHOICES, default=SENT_CHOICES[0][0]
    )
    processing_status = models.CharField(
        "Processing Status",
        max_length=30,
        choices=PROCESSING_STATUS_CHOICES,
        default=PROCESSING_STATUS_CHOICES[0][0],
    )
    text = models.TextField(blank=True)  # Assessor text when send_referral
    referral_text = models.TextField(blank=True) # used in other projects for complete referral comment but not used in boranga
    referral_comment = models.TextField(blank=True, null=True)  # Referral Comment
    document = models.ForeignKey(
        ConservationStatusReferralDocument,
        blank=True,
        null=True,
        related_name="referral_document",
        on_delete=models.SET_NULL,
    )
    assigned_officer = models.IntegerField(null=True)  # EmailUserRO

    class Meta:
        app_label = "boranga"
        ordering = ("-lodged_on",)

    def __str__(self):
        return "Application {} - Referral {}".format(self.conservation_status.id, self.id)

    # Methods
    @property
    def latest_referrals(self):
        return ConservationStatusReferral.objects.filter(sent_by=self.referral, conservation_status=self.conservation_status)[:2]

    @property
    def can_be_completed(self):
        #Referral cannot be completed until second level referral sent by referral has been completed/recalled
        qs=ConservationStatusReferral.objects.filter(sent_by=self.referral, conservation_status=self.conservation_status, processing_status=ConservationStatusReferral.PROCESSING_STATUS_WITH_REFERRAL)
        if qs:
            return False
        else:
            return True

    def can_process(self, user):
        return True  # TODO: implement
        # if self.processing_status == Referral.PROCESSING_STATUS_WITH_REFERRAL:
        #    group =  ReferralRecipientGroup.objects.filter(id=self.referral_group.id)
        #    #user=request.user
        #    if group and group[0] in user.referralrecipientgroup_set.all():
        #        return True
        #    else:
        #        return False
        # return False

    # def get_referral_group(self):
    #     # TODO: Take application_type into account
    #     return SystemGroup.objects.get(name=GROUP_NAME_REFERRAL)

    # @property
    # def referral_recipients(self):
    #     logger.info("referral_recipients")
    #     recipients = []
    #     group_ids = self.get_referral_group().get_system_group_member_ids()
    #     for id in group_ids:
    #         logger.info(id)
    #         recipients.append(EmailUser.objects.get(id=id).email)
    #     return recipients

    @property
    def referral_as_email_user(self):
        return retrieve_email_user(self.referral)

    def remind(self,request):
        with transaction.atomic():
            # TODO Is referral needed to check the assessor_group permission for below?
            if not self.conservation_status.can_assess(request.user):
                raise exceptions.ProposalNotAuthorized()
            # Create a log entry for the proposal
            self.conservation_status.log_user_action(
                ConservationStatusUserAction.ACTION_REMIND_REFERRAL.format(
                    self.id,
                    self.conservation_status.conservation_status_number,
                    "{}".format(self.referral_as_email_user.get_full_name()),
                ),
                request,
            )
            # Create a log entry for the organisation
            applicant_field = getattr(self.conservation_status, self.conservation_status.applicant_field)
            applicant_field = retrieve_email_user(applicant_field)
            
            # TODO: logging applicant_field
            # applicant_field.log_user_action(
            #     ConservationStatusUserAction.ACTION_REMIND_REFERRAL.format(
            #         self.id, 
            #         self.conservation_status.conservation_status_number, 
            #         '{}'.format(self.referral_as_email_user.get_full_name())
            #         ), request
            #     )

            # send email
            send_conservation_status_referral_email_notification(
                self,
                request,
                reminder=True,
                )

    def recall(self,request):
        with transaction.atomic():
            if not self.conservation_status.can_assess(request.user):
                raise exceptions.ProposalNotAuthorized()
            self.processing_status = 'recalled'
            self.save()
            send_conservation_status_referral_recall_email_notification(self, request)
            # TODO Log conservationstatus proposal action
            self.conservation_status.log_user_action(
                ConservationStatusUserAction.RECALL_REFERRAL.format(
                    self.id, 
                    self.conservation_status.conservation_status_number,
                ), 
                request,
            )
            # TODO log organisation action
            #self.proposal.applicant.log_user_action(ProposalUserAction.RECALL_REFERRAL.format(self.id, self.proposal.lodgement_number), request)

    def resend(self,request):
        with transaction.atomic():
            if not self.conservation_status.can_assess(request.user):
                raise exceptions.ProposalNotAuthorized()
            self.processing_status = 'with_referral'
            self.conservation_status.processing_status = 'with_referral'
            self.conservation_status.save()
            self.sent_from = 1
            self.save()
            # Create a log entry for the proposal
            self.conservation_status.log_user_action(
                ConservationStatusUserAction.ACTION_RESEND_REFERRAL_TO.format(
                    self.id,
                    self.conservation_status.conservation_status_number,
                    '{}({})'.format(self.referral_as_email_user.get_full_name(),self.referral_as_email_user.email),
                ),
                request,
            )
            # Create a log entry for the organisation
            #self.proposal.applicant.log_user_action(ProposalUserAction.ACTION_RESEND_REFERRAL_TO.format(self.id,self.proposal.lodgement_number,'{}({})'.format(self.referral.get_full_name(),self.referral.email)),request)
            
            # send email
            send_conservation_status_referral_email_notification(self,request)

    def send_referral(self,request,referral_email,referral_text):
        with transaction.atomic():
            try:
                referral_email = referral_email.lower()
                if self.conservation_status.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL:
                    if request.user.id != self.referral:
                        raise exceptions.ReferralNotAuthorized()
                    if self.sent_from != 1:
                        raise exceptions.ReferralCanNotSend()
                    self.conservation_status.processing_status = ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
                    self.conservation_status.save()
                    referral = None
                    # Check if the user is in ledger
                    try:
                        user = EmailUser.objects.get(email__icontains=referral_email)
                    except EmailUser.DoesNotExist:
                        # Validate if it is a deparment user
                        department_user = get_department_user(referral_email)
                        if not department_user:
                            raise ValidationError('The user you want to send the referral to is not a member of the department')
                        # Check if the user is in ledger or create

                        user,created = EmailUser.objects.get_or_create(email=department_user['email'].lower())
                        if created:
                            user.first_name = department_user['given_name']
                            user.last_name = department_user['surname']
                            user.save()
                    qs=ConservationStatusReferral.objects.filter(sent_by=user.id, conservation_status=self.conservation_status)
                    if qs:
                        raise ValidationError('You cannot send referral to this user')
                    try:
                        ConservationStatusReferral.objects.get(referral=user.id,conservation_status=self.conservation_status)
                        raise ValidationError('A referral has already been sent to this user')
                    except ConservationStatusReferral.DoesNotExist:
                        # Create Referral
                        referral = ConservationStatusReferral.objects.create(
                            conservation_status = self.conservation_status,
                            referral=user.id,
                            sent_by=request.user.id,
                            sent_from=2,
                            text=referral_text
                        )
                    # Create a log entry for the proposal
                    self.conservation_status.log_user_action(
                        ConservationStatusUserAction.ACTION_SEND_REFERRAL_TO.format(
                            referral.id,
                            self.conservation_status.conservation_status_number,
                            '{}({})'.format(user.get_full_name(),user.email),
                        ),
                        request,
                    )
                    # Create a log entry for the organisation
                    #self.proposal.applicant.log_user_action(ProposalUserAction.ACTION_SEND_REFERRAL_TO.format(referral.id,self.proposal.lodgement_number,'{}({})'.format(user.get_full_name(),user.email)),request)
                    # send email
                    send_conservation_status_referral_email_notification(referral,request)
                else:
                    raise exceptions.ConservationStatusReferralCannotBeSent()
            except:
                raise
    
    #def complete(self,request, referral_comment):
    def complete(self,request):
        with transaction.atomic():
            try:
                if request.user.id != self.referral:
                    raise exceptions.ReferralNotAuthorized()
                self.processing_status = 'completed'
                #self.referral_text = referral_comment
                self.save()
                
                outstanding  = self.conservation_status.referrals.filter(processing_status='with_referral')
                if len(outstanding) == 0:
                    self.conservation_status.processing_status = 'with_assessor'
                    self.conservation_status.save()
                
                # TODO Log conservationstatus action
                self.conservation_status.log_user_action(ConservationStatusUserAction.CONCLUDE_REFERRAL.format(
                    self.id,
                    self.conservation_status.conservation_status_number,
                    '{}({})'.format(self.referral_as_email_user.get_full_name(),self.referral_as_email_user.email),
                    ),
                    request,
                )
                # TODO log organisation action
                #self.proposal.applicant.log_user_action(ProposalUserAction.CONCLUDE_REFERRAL.format(self.id,self.proposal.lodgement_number,'{}({})'.format(self.referral.get_full_name(),self.referral.email)),request)
                send_conservation_status_referral_complete_email_notification(self,request)
            except:
                raise

    def can_assess_referral(self,user):
        return self.processing_status == 'with_referral'

    @property
    def can_be_processed(self):
        return self.processing_status == "with_referral"


class ConservationStatusProposalRequest(models.Model):
    conservation_status = models.ForeignKey(ConservationStatus, on_delete=models.CASCADE)
    subject = models.CharField(max_length=200, blank=True)
    text = models.TextField(blank=True)
    officer = models.IntegerField(null=True)  # EmailUserRO

    class Meta:
        app_label = 'boranga'


class ProposalAmendmentReason(models.Model):
    reason = models.CharField('Reason', max_length=125)

    class Meta:
        app_label = 'boranga'
        verbose_name = "Proposal Amendment Reason" # display name in Admin
        verbose_name_plural = "Proposal Amendment Reasons"

    def __str__(self):
        return self.reason


class ConservationStatusAmendmentRequest(ConservationStatusProposalRequest):
    STATUS_CHOICES = (('requested', 'Requested'), ('amended', 'Amended'))

    status = models.CharField('Status', max_length=30, choices=STATUS_CHOICES, default=STATUS_CHOICES[0][0])
    reason = models.ForeignKey(ProposalAmendmentReason, blank=True, null=True, on_delete=models.SET_NULL)
    
    class Meta:
        app_label = 'boranga'

    def generate_amendment(self,request):
        with transaction.atomic():
            try:
                if not self.conservation_status.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                if self.status == 'requested':
                    conservation_status = self.conservation_status
                    if conservation_status.processing_status != 'draft':
                        conservation_status.processing_status = 'draft'
                        conservation_status.customer_status = 'draft'
                        conservation_status.save()
                        # TODO at the moment conservation_status is not having it's document model
                        #conservation_status.documents.all().update(can_hide=True)

                    # Create a log entry for the conservationstatus
                    conservation_status.log_user_action(ConservationStatusUserAction.ACTION_ID_REQUEST_AMENDMENTS, request)
                    # Create a log entry for the organisation
                    # if conservation_status.applicant:
                    #     conservation_status.applicant.log_user_action(ConservationStatusUserAction.ACTION_ID_REQUEST_AMENDMENTS, request)

                    # send email

                    send_conservation_status_amendment_email_notification(self,request, conservation_status)

                self.save()
            except:
                raise

    def add_documents(self, request):
        with transaction.atomic():
            try:
                # save the files
                data = json.loads(request.data.get('data'))
                if not data.get('update'):
                    documents_qs = self.cs_amendment_request_documents.filter(input_name='amendment_request_doc', visible=True)
                    documents_qs.delete()
                for idx in range(data['num_files']):
                    _file = request.data.get('file-'+str(idx))
                    document = self.cs_amendment_request_documents.create(_file=_file, name=_file.name)
                    document.input_name = data['input_name']
                    document.can_delete = True
                    document.save()
                # end save documents
                self.save()
            except:
                raise
        return


class ConservationStatusAmendmentRequestDocument(Document):
    conservation_status_amendment_request = models.ForeignKey('ConservationStatusAmendmentRequest',related_name='cs_amendment_request_documents', on_delete=models.CASCADE)
    _file = models.FileField(upload_to=update_conservation_status_amendment_request_doc_filename, max_length=500, storage=private_storage)
    input_name = models.CharField(max_length=255,null=True,blank=True)
    can_delete = models.BooleanField(default=True) # after initial submit prevent document from being deleted
    visible = models.BooleanField(default=True) # to prevent deletion on file system, hidden and still be available in history

    def delete(self):
        if self.can_delete:
            return super(ConservationStatusAmendmentRequestDocument, self).delete()