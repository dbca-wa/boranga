import csv
import os

import django
from django.apps import apps
from django.db import models

# Set up Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "boranga.settings")
django.setup()


output_file = "field_lengths.csv"

print("Generating field lengths CSV...")

with open(output_file, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    # Header requested: model name, field name, length
    writer.writerow(["Model Name", "Field Name", "Length", "Field Type"])

    # Iterate over all installed models
    for model in apps.get_models():
        if model._meta.app_label != "boranga":
            continue

        # format: app_label.ModelName
        model_name = f"{model._meta.app_label}.{model._meta.object_name}"

        # We only care about fields defined on the model (or inherited)
        # get_fields() returns all fields including reverse relations which we want to skip
        # usually usually we want concrete fields.
        for field in model._meta.get_fields():
            # Check for CharField only
            # Note: EmailField, URLField, SlugField inherit from CharField
            if isinstance(field, models.CharField):
                field_name = field.name
                field_type = type(field).__name__

                length = getattr(field, "max_length", None)
                if length is None:
                    length = "Unlimited"

                writer.writerow([model_name, field_name, length, field_type])

print(f"Successfully generated {output_file}")
