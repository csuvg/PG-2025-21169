from django.contrib import admin
from django.contrib.auth.admin import UserAdmin


from formularios.models import Usuario

# Register your models here.
@admin.register(Usuario)
class UsuarioAdmin(UserAdmin):
    model = Usuario
    list_display = ("nombre_usuario", "correo", "is_staff", "is_superuser", "activo", "acceso_web")
    list_filter = ("is_staff", "is_superuser", "activo", "acceso_web")
    search_fields = ("nombre_usuario", "correo")
    ordering = ("nombre_usuario",)

    fieldsets = (
        ("Credenciales", {"fields": ("nombre_usuario", "password")}),
        ("Información personal", {"fields": ("correo",)}),
        ("Permisos", {"fields": ("is_staff", "is_superuser", "activo", "acceso_web", "groups", "user_permissions")}),
        ("Fechas", {"fields": ("last_login",)}),
    )

    add_fieldsets = (
        (None, {
            "classes": ("wide",),
            "fields": ("nombre_usuario", "correo", "password1", "password2", "is_staff", "is_superuser", "activo", "acceso_web"),
        }),
    )