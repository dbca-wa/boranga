import os
import posixpath
from collections.abc import Callable
from uuid import uuid4

from django.utils.deconstruct import deconstructible


def _default_name_generator(instance=None, filename=None) -> str:
    """Return a uuid4 hex string. Signature allows instance/filename but ignores them."""
    return uuid4().hex


@deconstructible
class RandomizeUploadTo:
    def __init__(
        self, upload_to, name_generator: Callable | None = None, keep_ext: bool = True
    ):
        self.upload_to = upload_to
        self.name_generator = name_generator
        self.keep_ext = keep_ext

    def __call__(self, instance, filename: str) -> str:
        generator = self.name_generator or _default_name_generator

        # Get the path from the original upload_to (callable or string)
        if callable(self.upload_to):
            original_path = self.upload_to(instance, filename)
        else:
            # treat upload_to as a directory path
            original_path = self.upload_to

        # Normalize to POSIX style (Django storages expect forward slashes)
        original_path = original_path.replace(os.sep, "/")

        # Split into dir and final component
        dir_name, final_component = posixpath.split(original_path)

        # Decide which extension to preserve
        if self.keep_ext:
            if final_component:
                ext = os.path.splitext(final_component)[1]
            else:
                ext = os.path.splitext(filename)[1]
        else:
            ext = ""

        # Generate randomized basename (support callables that accept instance/filename)
        try:
            new_base = generator(instance=instance, filename=filename)
        except TypeError:
            # fallback for simple callables that ignore kwargs
            new_base = generator()

        new_name = f"{new_base}{ext}"

        if dir_name:
            return posixpath.join(dir_name, new_name)
        return new_name


def randomize_upload_to(
    upload_to, name_generator: Callable | None = None, keep_ext: bool = True
):
    """
    Wrap an existing upload_to (callable or string) and return a new callable suitable
    for Django FileField/ImageField `upload_to` that preserves the directory structure
    produced by the original `upload_to` but replaces the final filename with a
    randomized value (preserving the original extension by default).

    Parameters
    - upload_to: callable(instance, filename) -> path OR a string directory
    - name_generator: callable(instance=None, filename=None) -> str, defaults to uuid4 hex
    - keep_ext: whether to keep the original file extension (default True)

    Usage examples
    - file = models.FileField(upload_to=randomize_upload_to(old_upload_to_fn))
    - # or to override globally after the original function is defined:
      update_fn = randomize_upload_to(update_fn)
    """
    return RandomizeUploadTo(upload_to, name_generator, keep_ext)


def override_upload_to_in_module(
    module,
    func_name: str,
    name_generator: Callable | None = None,
    keep_ext: bool = True,
):
    """
    Convenience helper to replace an existing upload_to callable in-place inside a module.
    Example:
        override_upload_to_in_module(models_module, 'update_conservation_status_comms_log_filename')

    This is useful when you have many FileField definitions that reference the same
    upload_to function name and you want to change the behaviour globally without
    editing each field.
    """
    orig = getattr(module, func_name)
    wrapped = randomize_upload_to(
        orig, name_generator=name_generator, keep_ext=keep_ext
    )
    # replace the function on the module
    setattr(module, func_name, wrapped)

    # Update any already-declared model fields that reference the original function
    # so they pick up the wrapped callable (fields are created at import time).
    try:
        from django.db import models as dj_models

        for attr in vars(module).values():
            try:
                if isinstance(attr, type) and issubclass(attr, dj_models.Model):
                    model_class = attr
                    # iterate model fields
                    for field in getattr(model_class, "_meta").get_fields():
                        # only fields that have upload_to attribute (FileField/ImageField)
                        if hasattr(field, "upload_to"):
                            try:
                                if field.upload_to is orig:
                                    field.upload_to = wrapped
                            except Exception:
                                # ignore fields that are not simple callables
                                continue
            except Exception:
                # skip attributes that aren't model classes
                continue
    except Exception:
        # If Django isn't ready or import fails, we silently skip updating fields
        # The module-level function replacement still helps for future references.
        pass


# British-spelling alias
def randomise_upload_to(
    upload_to, name_generator: Callable | None = None, keep_ext: bool = True
):
    """Alias for randomize_upload_to using British spelling."""
    return randomize_upload_to(
        upload_to, name_generator=name_generator, keep_ext=keep_ext
    )
