from django.core.exceptions import PermissionDenied


class ReferralOwnerMixin:
    def check_owner(self, user):
        return self.get_object().referral == user.id

    def dispatch(self, request, *args, **kwargs):
        if not self.check_owner(request.user):
            raise PermissionDenied

        return super().dispatch(request, *args, **kwargs)
