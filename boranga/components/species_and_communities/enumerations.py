from django.db import models


class GroupTypes(models.TextChoices):
    """

    """
    FLORA = 'flora', 'Flora'
    FAUNA = 'fauna', 'Fauna'
    COMMUNITIES = 'communities', 'Communities'
