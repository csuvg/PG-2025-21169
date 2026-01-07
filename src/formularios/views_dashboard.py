# views.py
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Count, Value, CharField
from .models import FormularioEntry, Usuario, Formulario, UserFormulario, FuenteDatos, Categoria
from drf_spectacular.utils import extend_schema, OpenApiResponse
from django.db.models.functions import Coalesce, TruncDate, TruncMonth
from django.utils.dateparse import parse_date



@extend_schema(
    tags=["Dashboards"],
    summary="Resumen general de la plataforma",
    description="""
    Devuelve métricas generales del sistema para los dashboards administrativos:
    - Usuarios (activos/inactivos/web)
    - Formularios (por estado/categoría)
    - Asignaciones (cuántos usuarios tienen formularios)
    - Datasets cargados
    """,
    responses={
        200: OpenApiResponse(
            description="Datos consolidados para dashboards",
            response={
                'type': 'object',
                'properties': {
                    'usuarios': {'type': 'object'},
                    'formularios': {'type': 'object'},
                    'asignaciones': {'type': 'object'},
                    'datasets': {'type': 'object'},
                }
            }
        )
    }
)

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def dashboard_resumen(request):
    try:
        # --- Usuarios ---
        total_usuarios = Usuario.objects.count()
        activos = Usuario.objects.filter(activo=True).count()
        web_access = Usuario.objects.filter(acceso_web=True).count()
        inactivos = Usuario.objects.filter(activo=False).count()
        prc_activos = (activos / total_usuarios * 100)

        # --- Formularios ---
        total_forms = Formulario.objects.count()
        forms_por_estado = (
            Formulario.objects.values("estado")
            .annotate(total=Count("id"))
            .order_by("estado")
        )

        # Agrupar por categoría desde Formulario (evita reverse accessor)
        # dentro de dashboard_resumen
        forms_por_categoria = (
            Formulario.objects
            .values(
                categoria_nombre=Coalesce(
                    "categoria__nombre",
                    Value("Sin categoría"),
                    output_field=CharField(),
                )
            )
            .annotate(total=Count("id"))
            .order_by("categoria_nombre")
        )

        # --- Asignaciones ---
        total_asignaciones = UserFormulario.objects.count()
        usuarios_asignados = (
            UserFormulario.objects.values("id_usuario")
            .annotate(c=Count("id_formulario"))
            .count()
        )

        # --- Datasets ---
        total_fuentes = FuenteDatos.objects.count()
        activos_fuentes = FuenteDatos.objects.filter(activo=True).count()

        # -------- Entradas por fecha (formularios recibidos) --------
        # Parametrización
        group = (request.query_params.get("group") or "day").lower()   # 'day' | 'month'
        desde_str = request.query_params.get("desde")
        hasta_str = request.query_params.get("hasta")

        # Punto de fecha a usar: filled_at_local si existe, si no created_at
        fecha_expr = Coalesce("filled_at_local", "created_at")

        qs_entries = FormularioEntry.objects.all()

        # Filtros por rango (inclusivos en día)
        if desde_str:
            d = parse_date(desde_str)
            if d:
                qs_entries = qs_entries.filter(created_at__date__gte=d)
        if hasta_str:
            h = parse_date(hasta_str)
            if h:
                qs_entries = qs_entries.filter(created_at__date__lte=h)

        if group == "month":
            period = TruncMonth(fecha_expr)
        else:
            period = TruncDate(fecha_expr)

        recibidos_por_fecha = (
            qs_entries
            .annotate(periodo=period)
            .values("periodo")
            .annotate(total=Count("id"))
            .order_by("periodo")
        )

        # Serialización simple de fechas (ISO)
        recibidos_por_fecha = [
            {
                "periodo": (row["periodo"].date().isoformat()
                            if hasattr(row["periodo"], "date") else row["periodo"].isoformat()),
                "total": row["total"],
            }
            for row in recibidos_por_fecha
        ]

        data = {
            "usuarios": {
                "total": total_usuarios,
                "activos": activos,
                "inactivos": inactivos,
                "prc_activos": prc_activos,
                "acceso_web": web_access,
            },
            "formularios": {
                "total": total_forms,
                "por_estado": list(forms_por_estado),
                "por_categoria": list(forms_por_categoria),
            },
            "asignaciones": {
                "total": total_asignaciones,
                "usuarios_con_formularios": usuarios_asignados,
            },
            "datasets": {
                "total": total_fuentes,
                "activos": activos_fuentes,
            },
            "entradas": {
                "group": group,  # devuelvo cómo se agrupó
                "recibidos_por_fecha": recibidos_por_fecha,
            },
        }
        return Response(data, status=status.HTTP_200_OK)

    except Exception as e:
        return Response({"detail": f"Error generando resumen: {str(e)}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)