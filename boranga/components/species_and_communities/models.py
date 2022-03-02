import datetime
from django.db import models


DISTRICT_PERTH_HILLS = 'PHS'
DISTRICT_SWAN_COASTAL = 'SWC'
DISTRICT_BLACKWOOD = 'BWD'
DISTRICT_WELLINGTON = 'WTN'
DISTRICT_DONNELLY = 'DON'
DISTRICT_FRANKLAND = 'FRK'
DISTRICT_ALBANY = 'ALB'
DISTRICT_ESPERANCE = 'ESP'
DISTRICT_EAST_KIMBERLEY = 'EKM'
DISTRICT_WEST_KIMBERLEY = 'WKM'
DISTRICT_EXMOUTH = 'EXM'
DISTRICT_PILBARA = 'PIL'
DISTRICT_KALGOORLIE = 'KAL'
DISTRICT_GERALDTON = 'GER'
DISTRICT_MOORA = 'MOR'
DISTRICT_SHARK_BAY = 'SHB'
DISTRICT_GREAT_SOUTHERN = 'GSN'
DISTRICT_CENTRAL_WHEATBELT = 'CWB'
DISTRICT_SOUTHERN_WHEATBELT = 'SWB'

DISTRICT_CHOICES = (
    (DISTRICT_PERTH_HILLS, "Perth Hills"),
    (DISTRICT_SWAN_COASTAL, "Swan Coastal"),
    (DISTRICT_BLACKWOOD, "Blackwood"),
    (DISTRICT_WELLINGTON, "Wellington"),
    (DISTRICT_DONNELLY, "Donnelly"),
    (DISTRICT_FRANKLAND, "Frankland"),
    (DISTRICT_ALBANY, "Albany"),
    (DISTRICT_ESPERANCE, "Esperance"),
    (DISTRICT_EAST_KIMBERLEY, "East Kimberley"),
    (DISTRICT_WEST_KIMBERLEY, "West Kimberley"),
    (DISTRICT_EXMOUTH, "Exmouth"),
    (DISTRICT_PILBARA, "Pilbara"),
    (DISTRICT_KALGOORLIE, "Kalgoorlie"),
    (DISTRICT_GERALDTON, "Geraldton"),
    (DISTRICT_MOORA, "Moora"),
    (DISTRICT_SHARK_BAY, "Shark Bay"),
    (DISTRICT_GREAT_SOUTHERN, "Great Southern"),
    (DISTRICT_CENTRAL_WHEATBELT, "Central Wheatbelt"),
    (DISTRICT_SOUTHERN_WHEATBELT, "Southern Wheatbelt")
)

REGION_KIMBERLEY = 'kimberley'
REGION_PILBARA = 'pilbara'
REGION_MIDWEST = 'midwest'
REGION_GOLDFIELDS = 'goldfields'
REGION_SWAN = 'swan'
REGION_WHEATBELT = 'wheatbelt'
REGION_SOUTH_WEST = 'southwest'
REGION_WARREN = 'warren'
REGION_SOUTH_COAST = 'southcoast'

REGION_CHOICES = (
    (REGION_KIMBERLEY,'Kimberley'),
    (REGION_PILBARA,'Pilbara'),
    (REGION_MIDWEST,'Midwest'),
    (REGION_GOLDFIELDS,'Goldfields'),
    (REGION_SWAN,'Swan'),
    (REGION_WHEATBELT,'Wheatbelt'),
    (REGION_SOUTH_WEST,'South West'),
    (REGION_WARREN,'Warren'),
    (REGION_SOUTH_COAST,'South Coast')
)

class Region(models.Model):
    name = models.CharField(choices=REGION_CHOICES, 
                            unique=True,
                            default=None,
                            max_length=64)

    class Meta:
        app_label = 'boranga'
        
    def __str__(self):
        return self.get_name_display()


class District(models.Model):
    name = models.CharField(choices=DISTRICT_CHOICES, 
                            unique=True,
                            max_length=64)
    region = models.ForeignKey(Region, 
                               on_delete=models.CASCADE )

    class Meta:
        app_label = 'boranga'
    def __str__(self):
        return self.get_name_display()


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
    GROUP_TYPES = [('flora', 'Flora'), ('fauna', 'Fauna'), ('community', 'Community')]
    name = models.CharField(max_length=64,
                            choices=GROUP_TYPES,
                            default=GROUP_TYPES[1],)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return self.get_name_display()


class ConservationList(models.Model):
    """

    NB: Can have multiple lists per species
    WAPEC	    WA Priority Ecological Community List
    DBCA_RLE	Pre-BCA DBCA precursor to IUCN RLE
    WAPS	    WA Priority Species List
    SPFN	    Wildlife Conservation (Specially Protected Fauna) Notice, Schedules
    IUCN_RLE	IUCN Red List of Ecosystems
    IUCN2012	IUCN Red List Categories and Criteria v3.1(2001) 2nd edition (2012)
    IUCN2001	IUCN Red List Categories and Criteria v3.1(2001)
    EPBC	    Environment Protection and Biodiversity Conservation Act 1999
    IUCN1994	IUCN Red List Categories v2.3 (1994)
    WAWCA	    Wildlife Conservation Act 1950, Gazettal notice listing

    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    code = models.CharField(max_length=64,
                            default="None")
    label = models.CharField(max_length=1024,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.code)


class ConservationCategory(models.Model):
    """
    CR	Critically endangered fauna (S1)
    P1	Priority 1
    PD	Presumed Totally Destroyed
    P1	Priority 1
    EN	Endangered fauna (S2)
    P2	Priority 2
    P2	Priority 2
    CR	Critically Endangered
    VU	Vulnerable fauna (S3)
    P3	Priority 3
    P3	Priority 3
    EN	Endangered
    EX	Presumed extinct fauna (S4)
    P4	Priority 4
    P4	Priority 4
    VU	Vulnerable
    P5	Priority 5
    LR	Lower Risk
    IA	Migratory birds under international agreement (S5)
    CD	Conservation dependent fauna (S6)
    DD	Data Deficient
    NE	Not Evaluated
    OS	Other specially protected fauna (S7)
    P5	Priority 5
    SP	Rare or otherwise in need of special protection
    P	Priority
    T	(Threatened) Rare or is likely to become extinct (S1)
    X	Presumed extinct (S2)
    I	Migratory birds under international agreement (S3)
    S	Other specially protected fauna (S4)
    CO	Collapsed
    CR	Critically Endangered
    EN	Endangered
    VU	Vulnerable
    NT	Near Threatened
    LC	Least Concern
    DD	Data Deficient
    NE	Not Evaluated
    EX	Extinct
    EW	Extinct in the Wild
    CR	Critically Endangered
    EN	Endangered
    VU	Vulnerable
    NT	Near Threatened
    LC	Least Concern
    DD	Data Deficient
    NE	Not Evaluated
    EX	Extinct
    EW	Extinct in the Wild
    CR	Critically Endangered
    EN	Endangered
    VU	Vulnerable
    EX	Extinct
    CR	Critically Endangered
    EN	Endangered
    VU	Vulnerable
    MA	Marine
    MI	Migratory
    CT	Cetacean
    EX	Extinct
    EW	Extinct in the Wild
    CR	Critically Endangered
    EN	Endangered
    VU	Vulnerable
    CD	Conservation Dependent
    SP	Specially Protected
    E	Extant (S1)
    X	Presumed extinct (S2)

    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    code = models.CharField(max_length=64,
                            default="None")
    label = models.CharField(max_length=1024,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.code)


class ConservationCriteria(models.Model):
    """
    Justification for listing as threatened (IUCN-how everything is defined)
    A
    Ai
    Aii
    B
    Bi
    Bii
    Biii
    C
    A1
    A2a
    A2b
    A3
    B1ai
    B1aii
    B1aiii
    B1b
    B1c
    B2ai
    B2aii
    B2aiii
    B2b
    B2c
    B3
    C1
    C2a
    C2b
    C3
    D1
    D2a
    D2b
    D3
    E
    A1a
    A1b
    A1c
    A1d
    A1e
    A2c
    A2d
    A2e
    A3b
    A3c
    A3d
    A3e
    A4a
    A4b
    A4c
    A4d
    A4e
    B1a
    B1b(i)
    B1b(ii)
    B1b(iii)
    B1b(iv)
    B1b(v)
    B1c(i)
    B1c(ii)
    B1c(iii)
    B1c(iv)
    B2a
    B2b(i)
    B2b(ii)
    B2b(iii)
    B2b(iv)
    B2b(v)
    B2c(i)
    B2c(ii)
    B2c(iii)
    B2c(iv)
    C2a(i)
    C2a(ii)
    D
    D2
    Criterion 1
    Criterion 2
    Criterion 3
    Criterion 4
    Criterion 5
    B1
    B2d
    B2e
    B3a
    B3b
    B3c
    B3d
    NB: may have multiple of these per species
    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    code = models.CharField(max_length=64)

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
    conservation_category = models.ForeignKey(ConservationCategory, on_delete=models.CASCADE)
    conservation_criteria = models.ForeignKey(ConservationCriteria, on_delete=models.CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.conservation_list)  # TODO: is the most appropriate?


class NameAuthority(models.Model):
    """

    """
    name = models.CharField(max_length=128,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name)


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
    taxon_id = models.IntegerField(default=-1)  # flora and fauna, name

    previous_name = models.CharField(max_length=512,
                                     default="None")
    family = models.CharField(max_length=512,
                              default="None")
    genus = models.CharField(max_length=512,
                             default="None")
    phylogenetic_group = models.CharField(max_length=512,
                                          default="None")
    name_authority = models.ForeignKey(NameAuthority,
                                       on_delete=models.CASCADE)

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
                                   on_delete=models.CASCADE)
    scientific_name = models.CharField(max_length=128,
                                       default="None")
    common_name = models.CharField(max_length=128,
                                   default="None")
    name_currency = models.CharField(max_length=16,
                                     default="None") # is it the current name? yes or no
    conservation_status = models.OneToOneField(ConservationStatus,
                                               on_delete=models.CASCADE)
    # community many to many
    region = models.ForeignKey(Region, 
                               default=None,
                               on_delete=models.CASCADE)
    district = models.ForeignKey(District, 
                                 default=None,
                                 on_delete=models.CASCADE)
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


class SpeciesAttributes(models.Model):
    """
    Do no know what this is but is required for SpeciesDocuments
    """
    name_reference = models.CharField(max_length=64,
                                      default="None")
    genetic = models.CharField(max_length=64,
                               default="None")
    biology = models.CharField(max_length=64,
                               default="None")
    ecology = models.CharField(max_length=64,
                               default="None")
    fire = models.CharField(max_length=64,
                            default="None")
    disease = models.CharField(max_length=64,
                               default="None")

    species = models.ForeignKey(Species, blank=False, 
                                on_delete=models.CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name_reference)  # TODO: is the most appropriate?


class Source(models.Model):
    """

    """
    name = models.CharField(max_length=128,
                            default="None")

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.name)


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
    group_type = models.ForeignKey(GroupType,
                                   on_delete=models.CASCADE)
    community_name = models.CharField(max_length=2048,
                                      default="None")
    community_id = models.IntegerField(default=-1) # this will be the display name and will appear in filter list
    community_status = models.CharField(max_length=128,
                                        default="None")
    region = models.ForeignKey(Region, 
                               default=None,
                               on_delete=models.CASCADE)
    district = models.ForeignKey(District, 
                                 default=None,
                                 on_delete=models.CASCADE)

    species = models.ManyToManyField(Species, blank=False)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str("{}: {}".format(self.community_id, self.community_name))


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

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.document)
                                    

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
    document = models.CharField(max_length=512,                 
                                default="None") # document file name without path
    document_description = models.CharField(max_length=1024,
                                            default="None")
    date_time = models.DateField(default=datetime.date.today)

    document_category = models.ForeignKey(DocumentCategory, 
                                          default="None",
                                          on_delete=models.CASCADE)
    species = models.ForeignKey(Species, 
                                blank=False,
                                default=None,
                                on_delete=models.CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.document)


class ThreatCategory(models.Model):
    """
    # e.g. mechnical disturbance
    """
    name = models.CharField(max_length=128,
                            default="None")

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
    threat_category = models.ForeignKey(ThreatCategory, on_delete=models.CASCADE)
    threat_description = models.CharField(max_length=512,
                                          default="None")
    comment = models.CharField(max_length=512,
                               default="None")
    document = models.CharField(max_length=1024,
                                default="None")
    source = models.CharField(max_length=1024,
                              default="None") # from species or occurrence_threat -> species level
    species = models.ForeignKey(Species, on_delete=models.CASCADE)

    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.threat_category)  # TODO: is the most appropriate?


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
    region = models.ForeignKey(Region, 
                               default=None,
                               on_delete=models.CASCADE)
    district = models.ForeignKey(District, 
                                 default=None,
                                 on_delete=models.CASCADE)
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
        return str(self.threat_category)  # TODO: is the most appropriate?


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
