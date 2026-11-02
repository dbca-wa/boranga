from django.contrib.admin import AdminSite

from boranga import helpers as boranga_helpers


class BorangaAdminSite(AdminSite):
    """Enforce the rule that only superusers and members of the django admin system group can access the admin"""

    def has_permission(self, request):
        return (
            super().has_permission(request)  # Checks is_active and is_staff
            and (request.user.is_superuser or boranga_helpers.is_django_admin(request))
        )
