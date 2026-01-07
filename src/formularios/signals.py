from django.db.models.signals import post_save, pre_save
from django.db import transaction
from django.dispatch import receiver
from oauth2_provider.models import AccessToken, RefreshToken
from .models import PaginaVersion, Usuario

from .models import Formulario, FormularioIndexVersion, Formulario_Index_Version, Pagina, Pagina_Index_Version

from .services import activar_version

@receiver(post_save, sender=Formulario)
def crear_y_activar_version_inicial(sender, instance: Formulario, created, **kwargs):
    if created:
        # 1) crear versión y registrar historial
        v1 = FormularioIndexVersion.objects.create()
        Formulario_Index_Version.objects.get_or_create(
            id_index_version=v1,
            defaults={"id_formulario": instance},
        )

        def _despues_commit():
            # 2) crear página inicial
            nueva = Pagina.objects.create(
                secuencia=1,
                nombre="General",
                descripcion="",
            )
            # 3) puntero página ↔ versión
            Pagina_Index_Version.objects.update_or_create(
                id_pagina=nueva,
                defaults={"id_index_version": v1},
            )

            # 4) primera pagina_version (vacía)
            from django.utils import timezone
            from .services import _uuid32
            PaginaVersion.objects.create(
                id_pagina_version=_uuid32(),
                id_pagina=nueva,
                fecha_creacion=timezone.now(),
            )
        transaction.on_commit(_despues_commit)

# @receiver(post_save, sender=FormularioIndexVersion)
# def _registrar_historial_al_crear_version(sender, instance: FormularioIndexVersion, created, **kwargs):
#     """
#     En cuanto se crea una nueva FormularioIndexVersion, guardamos UNA fila en el historial
#     (formularios_formularios_index_version) usando los nombres reales de columnas:
#       - id_index_version (PK 1:1 con la versión)
#       - id_formulario (FK al formulario)
#     """
#     if not created:
#         return

#     def _do():
#         Formulario_Index_Version.objects.get_or_create(
#             id_index_version=instance,                      
#             defaults={"id_formulario": instance.formulario_id},
#         )

#         try:
#             from django.apps import apps
#             FormularioIndex = apps.get_model("formularios", "FormularioIndex")
#         except Exception:
#             FormularioIndex = None

#         if FormularioIndex:
#             FormularioIndex.objects.update_or_create(
#                 id_formulario=instance.formulario_id,
#                 defaults={"id_index_version": instance},
#             )

#     transaction.on_commit(_do)

@receiver(pre_save, sender=Usuario)
def revoke_tokens_on_flag_disable(sender, instance: Usuario, **kwargs):
    if not instance.pk:
        return

    try:
        prev = sender.objects.get(pk=instance.pk)
    except sender.DoesNotExist:
        return

    turned_off = (prev.acceso_web and not instance.acceso_web) or \
                 (prev.activo and not instance.activo)
    if turned_off:
        AccessToken.objects.filter(user=instance).delete()
        RefreshToken.objects.filter(user=instance).delete()
        