from django.db import models
import uuid

from formularios.auth_models import UsuarioManager
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin

try:
    from django.db.models import JSONField
except Exception:
    JSONField = None

# Create your models here.
class Categoria(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    nombre = models.CharField(max_length=100)
    descripcion = models.TextField(blank=True)

    def __str__(self):
        return self.nombre

    class Meta:
        # managed = False
        db_table = 'formularios_categoria'

class Usuario(AbstractBaseUser, PermissionsMixin):
    nombre_usuario = models.CharField(max_length=50, primary_key=True, db_column="nombre_usuario")
    nombre = models.CharField(max_length=100)
    correo = models.EmailField(unique=True)
    password = models.CharField(max_length=128)
    activo = models.BooleanField(default=True)
    acceso_web = models.BooleanField(default=False)
    is_staff = models.BooleanField(default=False)
    is_superuser = models.BooleanField(default=False)

    # Agregar estos campos para evitar conflictos
    groups = models.ManyToManyField(
        'auth.Group',
        verbose_name='groups',
        blank=True,
        related_name='formularios_usuarios',
        related_query_name='formularios_usuario',
    )
    user_permissions = models.ManyToManyField(
        'auth.Permission',
        verbose_name='user permissions',
        blank=True,
        related_name='formularios_usuarios',
        related_query_name='formularios_usuario',
    )

    objects = UsuarioManager()

    USERNAME_FIELD = 'nombre_usuario'
    REQUIRED_FIELDS = ['correo', 'nombre']


    class Meta:
        db_table = "formularios_usuario"
        ordering = ("nombre",)

    def __str__(self):
        return self.nombre_usuario

    @property
    def is_active(self):
        return self.activo
    
    def set_password(self, raw_password):
        from .services import hash_password
        self.password = hash_password(raw_password)
        self._password = raw_password
    
    def check_password(self, raw_password):
        from .services import verify_password
        return verify_password(self.password, raw_password)

class Formulario(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    categoria = models.ForeignKey(Categoria, on_delete=models.CASCADE, null=True, blank=True)
    nombre = models.CharField(max_length=100)
    descripcion = models.TextField(blank=True)
    permitir_fotos = models.BooleanField(default=False)
    permitir_gps = models.BooleanField(default=False)

    ESTADO_CHOICES = [
        ('Ingresado', 'Ingresado'),
        ('Activo', 'Activo'),
        ('Suspendido', 'Suspendido'),
        ('Pruebas', 'Pruebas'),
        ('Anulado', 'Anulado'),
    ]

    ENVIO_CHOICES = [
        ('En Linea/fuera Linea', 'En Linea/fuera Linea'),
        ('En Linea', 'En Linea'),
        ('Guardar', 'Guardar'),
    ]

    disponible_desde_fecha = models.DateField()
    disponible_hasta_fecha = models.DateField()
    periodicidad = models.IntegerField(null=True, blank=True)
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES)
    forma_envio = models.CharField(max_length=30, choices=ENVIO_CHOICES)
    es_publico = models.BooleanField(default=False)
    auto_envio = models.BooleanField(default=False)

    class Meta:
        # managed = False
        db_table = 'formularios_formulario'

class UserFormulario(models.Model):
    id_formulario = models.ForeignKey(Formulario, on_delete=models.CASCADE)
    id_usuario = models.ForeignKey(Usuario, on_delete=models.CASCADE)

    class Meta:
        # managed = False
        db_table = "formularios_user_formulario"
        unique_together = (("id_formulario", "id_usuario"),)  # evita duplicados
        indexes = [
            models.Index(fields=["id_usuario", "id_formulario"]),
        ]

class FormularioIndexVersion(models.Model):
    id_index_version = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    class Meta:
        # managed = False
        db_table = "formularios_formularioindexversion" 

class Formulario_Index_Version(models.Model):
    id_index_version = models.OneToOneField(
        FormularioIndexVersion,
        on_delete=models.CASCADE,
        db_column="id_index_version",
        primary_key=True,
        related_name="row_historial",
    )
    id_formulario = models.ForeignKey(
        Formulario,
        on_delete=models.CASCADE,
        db_column="id_formulario",
        related_name="versiones_hist",
    )

    class Meta:
        # managed = False
        db_table = "formularios_formularios_index_version"

class Pagina(models.Model):
    id_pagina = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    # formulario_id = models.ForeignKey(
    #     Formulario,
    #     on_delete=models.CASCADE,
    #     db_column='formulario_id',   
    #     related_name="paginas"
    # )
    secuencia = models.PositiveIntegerField(default=1)
    nombre = models.CharField(max_length=120)
    descripcion = models.TextField(blank=True)

    class Meta:
        ordering = ["secuencia"]
        # managed = False
        db_table = "formularios_pagina"

class Pagina_Index_Version(models.Model):
    id_pagina = models.OneToOneField(
        "formularios.Pagina",
        on_delete=models.CASCADE,
        db_column="id_pagina",
        primary_key=True,
        related_name="puntero_version",
    )
    id_index_version = models.ForeignKey(
        "formularios.FormularioIndexVersion",
        on_delete=models.CASCADE,
        db_column="id_index_version",
        related_name="paginas_puntero",
    )

    class Meta:
        # managed = False
        db_table = "formularios_pagina_index_version"


class PaginaVersion(models.Model):
    id_pagina_version = models.CharField(primary_key=True, max_length=32, db_column="id_pagina_version")
    fecha_creacion = models.DateTimeField(db_column="fecha_creacion")
    # ✨ CAMBIO 1: Convertir a ForeignKey real
    id_pagina = models.ForeignKey(
        Pagina,
        on_delete=models.CASCADE,
        db_column="id_pagina",
        related_name="versiones_pagina",  # pagina.versiones_pagina.all()
        null=True
    )
    class Meta:
        # managed = False
        db_table = "formularios_pagina_version"
        ordering = ['-fecha_creacion']
        indexes = [
            models.Index(fields=['id_pagina', '-fecha_creacion']),
        ]


class ClaseCampo(models.Model):
    clase = models.CharField(primary_key=True, max_length=30, db_column="clase")
    estructura = models.TextField(db_column="estructura", null=True, blank=True)

    class Meta:
        # managed = False
        db_table = "formularios_clase_campo"


class Campo(models.Model):
    id_campo = models.UUIDField(primary_key=True, default=uuid.uuid4, db_column="id_campo")
    tipo = models.CharField(max_length=20, db_column="tipo")
    clase = models.CharField(max_length=30, db_column="clase")
    nombre_campo = models.CharField(max_length=64, db_column="nombre_campo")
    etiqueta = models.CharField(max_length=100, db_column="etiqueta")
    ayuda = models.CharField(max_length=255, db_column="ayuda", null=True, blank=True)
    config = models.TextField(db_column="config", null=True, blank=True)
    requerido = models.BooleanField(db_column="requerido", null=True)

    class Meta:
        # managed = False
        db_table = "formularios_campo"


class PaginaCampo(models.Model):
    id_campo = models.ForeignKey(
        Campo, on_delete=models.CASCADE, db_column="id_campo",
        related_name="enlaces_pagina", primary_key=True
    )
    id_pagina_version = models.ForeignKey(
        PaginaVersion, on_delete=models.CASCADE, db_column="id_pagina_version",
        related_name="campos"
    )
    sequence = models.PositiveIntegerField(db_column="sequence", null=True)

    class Meta:
        # managed = False
        db_table = "formularios_pagina_campo"
        unique_together = (("id_campo", "id_pagina_version"),) 

class FuenteDatos(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    nombre = models.CharField(max_length=200)
    descripcion = models.TextField(blank=True)
    archivo_nombre = models.CharField(max_length=255)  # nombre original
    blob_name = models.CharField(max_length=500)  # nombre en Azure
    blob_url = models.URLField(max_length=1000)
    tipo_archivo = models.CharField(max_length=10, choices=[('excel', 'Excel'), ('csv', 'CSV')])
    columnas = models.JSONField(default=list)  # lista de nombres de columnas
    preview_data = models.JSONField(default=list)  # primeras 5 filas para preview
    fecha_subida = models.DateTimeField(auto_now_add=True)
    activo = models.BooleanField(default=True)
    creado_por = models.ForeignKey(Usuario, on_delete=models.SET_NULL, null=True, related_name='fuentes_datos')

    class Meta:
        db_table = 'formularios_fuente_datos'
        ordering = ['-fecha_subida']

    def __str__(self):
        return self.nombre

class FuenteDatosValor(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    campo = models.ForeignKey("Campo", on_delete=models.CASCADE, db_index=True, related_name="dataset_vals")
    fuente = models.ForeignKey("FuenteDatos", on_delete=models.CASCADE, db_index=True)  # nuevo
    columna = models.CharField(max_length=200, blank=True, default="")
    key_text = models.TextField(blank=True, null=True)
    label_text = models.TextField()
    valor_raw = models.JSONField(default=dict)
    extras = models.JSONField(default=dict)

    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "formularios_fuente_datos_valor"
        indexes = [
            models.Index(fields=["campo", "label_text"]),
            models.Index(fields=["campo", "key_text"]),
        ]
        unique_together = (("campo", "key_text"),)

class Grupo(models.Model):
    id_grupo = models.UUIDField(primary_key=True, default=uuid.uuid4, db_column="id_grupo")
    id_campo_group = models.OneToOneField(
        Campo,
        on_delete=models.CASCADE,
        db_column="id_campo_group",
        related_name="grupo"
    )
    nombre = models.CharField(max_length=150, db_column="nombre")

    class Meta:
        db_table = "formularios_grupo"

    def __str__(self):
        return f"{self.nombre} ({self.id_grupo})"

class CampoGrupo(models.Model):
    id_grupo = models.ForeignKey(
        Grupo,
        on_delete=models.CASCADE,
        db_column="id_grupo",
        related_name="miembros"
    )
    id_campo = models.ForeignKey(
        Campo,
        on_delete=models.CASCADE,
        db_column="id_campo",
        related_name="grupos"
    )

    class Meta:
        db_table = "formularios_campo_grupo"
        unique_together = (("id_grupo", "id_campo"),)

class FormularioEntry(models.Model):
    """
    Mapea la tabla existente: formularios_entry
    """
    id = models.UUIDField(primary_key=True, db_column="id")
    id_usuario = models.CharField(max_length=150, db_column="id_usuario_id", null=True, blank=True)
    form_id = models.UUIDField(db_column="form_id")                              # FK lógico a Formulario.id
    index_version_id = models.UUIDField(db_column="index_version_id")            # FK lógico a FormularioIndexVersion.id_index_version
    form_name = models.CharField(max_length=200, db_column="form_name")
    filled_at_local = models.DateTimeField(null=True, blank=True, db_column="filled_at_local")
    status = models.CharField(max_length=50, db_column="status")
    fill_json = models.JSONField(null=True, blank=True, db_column="fill_json")
    form_json = models.JSONField(null=True, blank=True, db_column="form_json")
    created_at = models.DateTimeField(db_column="created_at")
    updated_at = models.DateTimeField(db_column="updated_at")

    class Meta:
        managed = False
        db_table = "formularios_entry"
        ordering = ("-created_at",)

    def __str__(self):
        return f"{self.form_name} · {self.id}"