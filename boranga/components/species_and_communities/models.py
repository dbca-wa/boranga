from django.db import models
from django.db.models.deletion import CASCADE


class GroupType(models.Model):
    """
    The three types of group managed by Boranga: fauna, flora and communities. These are the basis
    for all other models in Species and Communities.

    Has a:
    - N/A
    Used by:
    - Species
    Is:
    - Enumeration (GroupTypes)
    """
    name = models.CharField(max_length=4,
                            choices=[('flora', 'Flora'), ('fauna', 'Fauna'), ('community', 'Community')],
                            default='1',)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name)


class ConservationList(models.Model):
    """

    NB: Can have multiple lists per species
    WA BC Act,
    Commonwealth EPBC Act,
    WA Priority List,
    NB: more to come

    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    name = models.CharField(max_length=128,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name)


class ConservationCategory(models.Model):
    """
    1. Threatened

    2. Endangered

    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    name = models.CharField(max_length=128,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name)


class ConservationCriteria(models.Model):
    """
    Justification for listing as threatened (IUCN-how everything is defined)

    NB: may have multiple of these per species
    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    name = models.CharField(max_length=128,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name)


class ConservationStatus(models.Model):
    """
    Several lists with different attributes

    NB: Different lists has different different entries
    mainly interest in wa but must accomodte comm as well
    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    conservation_list = models.OneToOneField(ConservationList,
                                             on_delete=models.CASCADE,
                                             primary_key=True,)
    conservation_category = models.ForeignKey(ConservationCategory, on_delete=CASCADE)
    conservation_criteria = models.ForeignKey(ConservationCriteria, on_delete=CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.conservation_list)  # TODO: is the most appropriate?


class Taxonomy(models.Model):
    """
    Description from wacensus, to get the main name then fill in everything else


    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    taxon = models.CharField(max_length=512,
                             default="None")  # flora and fauna, name
    taxon_id = models.IntegerField()  # flora and fauna, name

    previous_name = models.CharField(max_length=512,
                                     default="None")
    family = models.CharField(max_length=512,
                              default="None")
    genus = models.CharField(max_length=512,
                             default="None")
    phylogenetic_group = models.CharField(max_length=512,
                                          default="None")
    name_authority = models.CharField(max_length=512,
                                      default="None")
    community_id = models.CharField(max_length=512,
                                    default="None")  # on a map, same as common name
    community_number = models.IntegerField(default=-1)  # same as taxon id
    community_description = models.CharField(max_length=512,
                                             default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.taxon)  # TODO: is the most appropriate?


class Species(models.Model):
    """
    Forms the basis for a Species and Communities record.

    Has a:
    - ConservationStatus
    - Community
    - GroupType
    - SpeciesDocument
    - ConservationThreat
    - ConservationPlan
    - Taxonomy
    - Distribution
    - ConservationAttributes
    Used by:
    - Communities
    Is:
    - Table
    """
    group_type = models.ForeignKey(GroupType,
                                   on_delete=models.CASCADE,
                                   primary_key=True,)
    scientific_name = models.CharField(max_length=128,
                                       default="None")
    common_name = models.CharField(max_length=128,
                                   default="None")
    name_currency = models.CharField(max_length=512,
                                     default="None")
    conservation_status = models.OneToOneField(ConservationStatus,
                                               on_delete=models.CASCADE,
                                               primary_key=True,)
    # community many to many
    region = models.IntegerField(default=-1)  # TODO: reuse DBCA
    district = models.IntegerField(default=-1)  # TODO: reuse DBCA
    image = models.CharField(max_length=512,
                             default="None")
    processing_status = models.CharField(max_length=512,
                                         default="None")
    # species_document foreign key
    # conservation_threats foreign key
    # conservation_plans many to many
    taxonomy = models.OneToOneField(Taxonomy, on_delete=models.CASCADE,)
    # distribution one to one
    # conservation_attributes one to one

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.taxonomy.taxon)  # TODO: is the most appropriate?


class Community(models.Model):
    """
    A collection of 2 or more Species within a specific location.

    Has a:
    - ConservationStatus
    - Species
    Used by:
    - N/A
    Is:
    - Table
    """
    community_name = models.CharField(max_length=128,
                                      default="None")
    community_id = models.IntegerField(default=-1)
    community_status = models.CharField(max_length=128,
                                        default="None")
    region = models.IntegerField(default=-1)  # TODO: reuse DBCA
    district = models.IntegerField(default=-1)  # TODO: reuse DBCA

    species = models.ManyToManyField(Species, blank=False)
    conservation_status = models.ForeignKey(to=ConservationStatus, on_delete=CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str("{}: {}".format(self.community_id, self.community_name))


class SpeciesDocument(models.Model):
    """
    Meta-data associated with a document relevant to a Species.

    Has a:
    - DocumentCategory
    Used by:
    - Species
    - Communities:
    Is:
    - Table
    """
    species_id = models.IntegerField(default=-1)
    category_id = models.IntegerField(default=-1)
    status = models.CharField(max_length=512,
                              default="None")
    document = models.CharField(max_length=512,                 # document file name without path
                                default="None")
    document_description = models.CharField(max_length=1024,
                                            default="None")
    date_time = models.DateField()

    species = models.ManyToManyField(Species, blank=False)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.document)


class DocumentCategory(models.Model):
    """
    This is particularly useful for organisation of documents e.g. preventing inappropriate documents being added
    to certain tables.

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    name = models.CharField(max_length=128,
                            default="None")

    species_document = models.ForeignKey(to=SpeciesDocument, on_delete=CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name)


class ConservationThreat(models.Model):
    """
    Threat for a species in a particular location.

    NB: Maybe make many to many

    Has a:
    - N/A
    Used by:
    - Species
    Is:
    - Table
    """
    threat_category_id = models.IntegerField(default=-1)
    threat_description = models.CharField(max_length=512,
                                          default="None")
    comment = models.CharField(max_length=512,
                               default="None")
    document = models.CharField(max_length=1024,
                                default="None")
    source = models.CharField(max_length=1024,
                              default="None")

    species = models.ForeignKey(Species, on_delete=CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.threat_category_id)  # TODO: is the most appropriate?


class ConservationPlan(models.Model):
    """
    Each occurrence of each species can have one or more plan to protect it.

    Has a:
    - N/A
    Used by:
    - Species
    Is:
    - Table
    """
    threat_category_id = models.IntegerField(default=-1)
    region_id = models.IntegerField(default=-1)
    district_id = models.IntegerField(default=-1)
    type = models.CharField(max_length=512,
                            default="None")
    comment = models.CharField(max_length=512,
                               default="None")
    source = models.CharField(max_length=1024,
                              default="None")

    species = models.ManyToManyField(Species, blank=False)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.threat_category_id)  # TODO: is the most appropriate?


class ConservationAttributes(models.Model):
    """
    Additional meta-data particularly of use to administration.

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    general_management_advice = models.CharField(max_length=512,
                                                 default="None")
    ecological_attributes = models.CharField(max_length=512,
                                             default="None")
    biological_attributes = models.CharField(max_length=512,
                                             default="None")
    specific_survey_advice = models.CharField(max_length=512,
                                              default="None")

    species = models.OneToOneField(Species,
                                   on_delete=models.CASCADE,
                                   primary_key=True,)
    comments = models.CharField(max_length=2048,
                                default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.species.taxonomy.taxon)  # TODO: is the most appropriate?


class Distribution(models.Model):
    """
    All the different locations where this species can be found.

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    department_file_numbers = models.CharField(max_length=512,
                                               default="None")  # objective, legacy, list of things
    community_original_area = models.IntegerField(default=-1)
    community_original_area_accuracy = models.IntegerField(default=-1)
    number_of_occurrences = models.IntegerField(default=-1)
    extent_of_occurrences = models.IntegerField(default=-1)
    area_of_occupancy = models.IntegerField(default=-1)
    number_of_iucn_locations = models.IntegerField(default=-1)
    community_original_area_reference = models.CharField(max_length=512,
                                                         default="None")
    species = models.OneToOneField(Species,
                                   on_delete=models.CASCADE,
                                   primary_key=True,)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.community_original_area_reference)  # TODO: is the most appropriate?
