from ntpath import basename
from django.urls import path, include
from formularios import auth_views
from rest_framework.routers import DefaultRouter

from formularios.views_dashboard import dashboard_resumen
from .views import AsignacionViewSet, CategoriaViewSet, EntryExportViewSet, FormularioViewSet, FuenteDatosViewSet, GrupoViewSet, PaginaViewSet, UsuarioViewSet, CampoViewSet, FormularioListViewSet

router = DefaultRouter()
router.register(r'formularios', FormularioViewSet, basename='formulario')
router.register(r'categorias', CategoriaViewSet, basename='categoria')
router.register(r'paginas', PaginaViewSet, basename='pagina')
router.register(r'usuarios', UsuarioViewSet, basename='usuario')
router.register(r"campos",  CampoViewSet,  basename="campos")
router.register(r"formularios-lite", FormularioListViewSet, basename="formularios-lite")
router.register(r'fuentes-datos', FuenteDatosViewSet, basename='fuente-datos')
router.register(r'grupos', GrupoViewSet, basename='grupos')
router.register(r"asignaciones", AsignacionViewSet, basename="asignacion")
router.register(r'entries', EntryExportViewSet, basename='entries-exports')





urlpatterns = [
    path('', include(router.urls)),
    path('auth/login/', auth_views.login, name='login'),
    path('auth/logout/', auth_views.logout, name='logout'),
    path('auth/me/', auth_views.user_info, name='user-info'),
    path('dashboard/resumen/', dashboard_resumen, name='dashboard-resumen'),
]