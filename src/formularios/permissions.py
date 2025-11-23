from rest_framework.permissions import BasePermission

class IsWebAllowed(BasePermission):
    message = "Este usuario no tiene acceso a la plataforma web. Use la aplicación móvil."

    def has_permission(self, request, view):
        user = getattr(request, "user", None)
        if not user or not user.is_authenticated:
            return False
        return bool(getattr(user, "acceso_web", False)) and bool(getattr(user, "activo", False))