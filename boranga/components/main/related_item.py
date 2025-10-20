from rest_framework import serializers

from boranga.components.main.serializers import BaseSerializer
from boranga.helpers import is_internal


class RelatedItem:
    def __init__(
        self, model_name="", identifier="", descriptor="", status="", action_url=""
    ):
        self.model_name = model_name
        self.identifier = identifier
        self.descriptor = descriptor
        self.status = status
        self.action_url = action_url

    def __hash__(self):
        # Use a tuple for hashing so None values are handled safely
        return hash(
            (
                self.model_name,
                self.identifier,
                self.descriptor,
                self.status,
                self.action_url,
            )
        )

    def __eq__(self, other):
        if not isinstance(other, RelatedItem):
            return False

        return (
            self.identifier == other.identifier
            and self.model_name == other.model_name
            and self.descriptor == other.descriptor
            and self.status == other.status
            and self.action_url == other.action_url
        )

    def __str__(self):
        return f"{self.identifier}"


class RelatedItemsSerializer(BaseSerializer):
    model_name = serializers.CharField()
    identifier = serializers.CharField()
    descriptor = serializers.CharField()
    status = serializers.CharField()
    action_url = serializers.CharField(allow_blank=True)

    def to_representation(self, instance):
        # Am using to modify the action urls for external users so they only
        # see links to species / communities profile but don't see links to OCC/OCR etc
        request = self.context.get("request")
        if not is_internal(request) and instance.model_name.lower() not in [
            "species",
            "community",
        ]:
            # If the request is not internal, remove the action_url field
            instance.action_url = None

        return super().to_representation(instance)
