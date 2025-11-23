# serializers.py
import json
from .services import _uuid32_no_dashes, fetch_items_from_fdv_by_campo, hash_password
from rest_framework import serializers
from rest_framework.response import Response
from rest_framework import status
from .models import (
    Campo, Categoria, Formulario, FormularioIndexVersion, 
    FuenteDatos, FuenteDatosValor, Grupo, Pagina, 
    Pagina_Index_Version, PaginaCampo, PaginaVersion, 
    UserFormulario, Usuario, Formulario_Index_Version
)
from django.db import models
from django.db.models import Q

class GrupoSerializer(serializers.ModelSerializer):
    id_campo_group = serializers.CharField(source="id_campo_group_id", read_only=True)

    class Meta:
        model = Grupo
        fields = ("id_grupo", "nombre", "id_campo_group")

class CategoriaSerializer(serializers.ModelSerializer):
    class Meta:
        model = Categoria
        fields = '__all__'

class FuenteDatosSerializer(serializers.ModelSerializer):
    creado_por_nombre = serializers.CharField(source='creado_por.nombre', read_only=True)
    archivo = serializers.FileField(write_only=True, required=False)
    
    class Meta:
        model = FuenteDatos
        fields = [
            'id', 'nombre', 'descripcion', 'archivo_nombre', 
            'blob_url', 'tipo_archivo', 'columnas', 'preview_data',
            'fecha_subida', 'activo', 'creado_por', 'creado_por_nombre',
            'archivo'
        ]
        read_only_fields = [
            'id', 'blob_url', 'tipo_archivo', 'columnas', 
            'preview_data', 'fecha_subida', 'blob_name'
        ]
    
    def validate_archivo(self, value):
        """Valida el archivo subido"""
        if value:
            # Validar extensión
            filename = value.name
            extension = filename.split('.')[-1].lower()
            if extension not in ['xlsx', 'xls', 'csv']:
                raise serializers.ValidationError(
                    "Solo se permiten archivos Excel (.xlsx, .xls) o CSV (.csv)"
                )
            
            # Validar tamaño (máx 10MB)
            if value.size > 10 * 1024 * 1024:
                raise serializers.ValidationError(
                    "El archivo no puede superar los 10MB"
                )
        
        return value

class FuenteDatosCreateSerializer(serializers.Serializer):
    nombre = serializers.CharField(max_length=200)
    descripcion = serializers.CharField(required=False, allow_blank=True)
    archivo = serializers.FileField()
    
    def validate_archivo(self, value):
        """Valida el archivo subido"""
        filename = value.name
        extension = filename.split('.')[-1].lower()
        if extension not in ['xlsx', 'xls', 'csv']:
            raise serializers.ValidationError(
                "Solo se permiten archivos Excel (.xlsx, .xls) o CSV (.csv)"
            )
        
        if value.size > 10 * 1024 * 1024:
            raise serializers.ValidationError(
                "El archivo no puede superar los 10MB"
            )
        
        return value
    
    def create(self, validated_data):
        archivo = validated_data.pop("archivo")
        request = self.context.get("request")
        usuario = getattr(request, "user", None)

        instancia = FuenteDatos.objects.create(
            nombre=validated_data.get("nombre"),
            descripcion=validated_data.get("descripcion", ""),
            archivo_nombre=archivo.name,
            creado_por=usuario,
            activo=True,
        )

        return instancia

class FormularioListSerializer(serializers.ModelSerializer):
    categoria_nombre = serializers.SerializerMethodField()
    class Meta:
        model = Formulario
        fields = (
            "id",
            "categoria",            
            "categoria_nombre",   
            "nombre",
            "descripcion",
            "permitir_fotos",
            "permitir_gps",
            "disponible_desde_fecha",
            "disponible_hasta_fecha",
            "estado",
            "forma_envio",
            "es_publico",
            "auto_envio",
        )

    def get_categoria_nombre(self, obj):
        return obj.categoria.nombre if obj.categoria else None

class PaginaSerializer(serializers.ModelSerializer):
    class Meta:
        model = Pagina
        fields = ("id_pagina", "secuencia", "nombre", "descripcion")

def _normalize_dataset_config(config: dict) -> dict:
    """
    Acepta config plano o anidado bajo 'dataset'.
    - Si está plano, lo envuelve en {'dataset': ...}
    - Mapea alias 'file' -> 'fuente_id'
    - Coloca defaults razonables
    """
    if not isinstance(config, dict):
        try:
            config = json.loads(config or "{}")
        except Exception:
            return {}

    if "dataset" in config and isinstance(config["dataset"], dict):
        ds = config["dataset"]
    else:
        ds = dict(config)
        config = {"dataset": ds}

    if "file" in ds and "fuente_id" not in ds:
        ds["fuente_id"] = ds.get("file")

    ds["mode"] = (ds.get("mode") or "pair").lower()
    if "cache_inline" not in ds:
        ds["cache_inline"] = True
    if "max_items_inline" not in ds:
        ds["max_items_inline"] = 300

    return config

class CrearCampoEnPaginaSerializer(serializers.Serializer):
    clase = serializers.CharField()
    nombre_campo = serializers.RegexField(r"^[a-zA-Z0-9_]+$", max_length=64)
    etiqueta = serializers.CharField(max_length=100)
    ayuda = serializers.CharField(max_length=255, required=False, allow_null=True, allow_blank=True)
    requerido = serializers.BooleanField(required=False)
    config = serializers.JSONField(required=False)     
    sequence = serializers.IntegerField(required=False, min_value=1)  
        
    def validate(self, attrs):
        clase = (attrs.get("clase") or "").lower()
        cfg = attrs.get("config")

        if clase == "dataset":
            cfg_norm = _normalize_dataset_config(cfg)
            if not cfg_norm or "dataset" not in cfg_norm:
                raise serializers.ValidationError({"config": "Debe incluir objeto 'dataset'."})

            ds = cfg_norm["dataset"]
            mode = ds.get("mode")

            if not ds.get("fuente_id"):
                raise serializers.ValidationError({"config": {"dataset.fuente_id": "Requerido"}})

            if mode == "single":
                if not ds.get("column"):
                    raise serializers.ValidationError({"config": {"dataset.column": "Requerido en mode=single"}})
            elif mode == "pair":
                ds["key_column"] = ds.get("key_column") or "id"
                if not ds.get("label_column"):
                    raise serializers.ValidationError({"config": {"dataset.key_column/label_column": "Requeridos en mode=pair"}})
            else:
                raise serializers.ValidationError({"config": {"dataset.mode": "Debe ser 'single' o 'pair'"}})

            attrs["config"] = cfg_norm

        return attrs

class PaginaConCamposSerializer(PaginaSerializer):
    campos = serializers.SerializerMethodField()

    class Meta(PaginaSerializer.Meta):
        fields = PaginaSerializer.Meta.fields + ("campos",)

    def get_campos(self, obj):
        # 1) Tomar la última versión de la página (por fecha)
        pv = (PaginaVersion.objects
            .filter(id_pagina=obj)
            .order_by("-fecha_creacion")
            .first())
        if not pv:
            return []

        # 2) Traer links usando FK (puedes pasar el objeto o _id)
        links = (PaginaCampo.objects
                .filter(id_pagina_version=pv)
                .select_related("id_campo")
                .order_by("sequence"))

        import json
        from .models import Grupo, CampoGrupo

        def _cfg_dict(cfg):
            if isinstance(cfg, dict):
                return cfg
        # si es str, intenta parsear, si falla -> {}
            if isinstance(cfg, str):
                try:
                    return json.loads(cfg)
                except Exception:
                    return {}
            return {}

        out = []
        for l in links:
            c = l.id_campo
            cfg = _cfg_dict(c.config)
            d = {
                "id_campo": str(c.id_campo),
                "sequence": l.sequence,
                "nombre_campo": c.nombre_campo,
                "etiqueta": c.etiqueta,
                "clase": c.clase,
                "tipo": c.tipo,
                "requerido": c.requerido,
                "config": cfg,
            }
            if (c.clase or "").lower() == "dataset":
                # Soporta tanto config plano como anidado bajo 'dataset'
                ds = cfg.get("dataset") or {}
                # parámetros opcionales
                fuente_id = ds.get("fuente_id")  # si lo guardas en config, se respeta
                label_col = ds.get("label_column") or ds.get("column")
                mode = (ds.get("mode") or "pair").lower()          # "pair" o "list"
                cache_inline = bool(ds.get("cache_inline", True))
                max_items = ds.get("max_items_inline", 300)

                if cache_inline:
                    items_pair = fetch_items_from_fdv_by_campo(
                        campo_id=str(c.id_campo),
                        label_column=label_col,
                        fuente_id=fuente_id,
                        limit=max_items,
                    )

                    if mode == "pair":
                        # [{"key": "...", "label": "..."}]
                        cfg["items"] = items_pair
                    else:
                        # ["label1", "label2", ...]
                        cfg["items"] = [it["label"] for it in items_pair if it.get("label")]

                # reinyecta dataset normalizado (por si hiciste cambios)
                cfg["dataset"] = ds

            # finalmente
            d["config"] = cfg
            out.append(d)

        # --- grupos (igual que antes, pero sin tocar id_pagina_version como texto)
        index = {d["id_campo"]: d for d in out}
        seq_by_campo = {d["id_campo"]: d.get("sequence", 10**9) for d in out}
        child_ids = set()

        for d in out:
            if (d.get("clase") or "").lower() != "group":
                continue

            try:
                g = Grupo.objects.get(id_campo_group_id=d["id_campo"])
            except Grupo.DoesNotExist:
                d["children"] = []
                continue

            miembros_ids = list(
                CampoGrupo.objects
                .filter(id_grupo=g)
                .values_list("id_campo_id", flat=True)
            )
            hijos = [index[cid] for cid in map(str, miembros_ids) if cid in index]
            hijos.sort(key=lambda h: seq_by_campo.get(h["id_campo"], 10**9))
            d["children"] = hijos
            child_ids.update([h["id_campo"] for h in hijos])

        if child_ids:
            out = [d for d in out if d["id_campo"] not in child_ids]

        return out

class FormularioSerializer(serializers.ModelSerializer):
    categoria_nombre = serializers.SerializerMethodField()
    paginas = serializers.SerializerMethodField()

    class Meta:
        model = Formulario
        fields = "__all__"

    def get_categoria_nombre(self, obj):
        return obj.categoria.nombre if obj.categoria else None

    def get_paginas(self, obj):
        # 1) Última versión por fecha de creación
        link = (
            Formulario_Index_Version.objects
            .filter(id_formulario=obj)
            .select_related("id_index_version")
            .order_by("-id_index_version__fecha_creacion")
            .first()
        )
        if not link:
            return []

        last_version = link.id_index_version  # instancia de FormularioIndexVersion

        # 2) Todas las páginas enlazadas a esa versión
        page_ids = (
            Pagina_Index_Version.objects
            .filter(id_index_version=last_version)
            .values_list("id_pagina", flat=True)
        )

        qs = Pagina.objects.filter(id_pagina__in=list(page_ids)).order_by("secuencia")
        return PaginaConCamposSerializer(qs, many=True, context=self.context).data

class UsuarioDetalleSerializer(serializers.ModelSerializer):
    # usuario = UsuarioCreateSerializer(many=True, read_only=True)

    class Meta:
        model = Usuario
        fields = ("nombre_usuario", "nombre", "correo", "activo", "acceso_web")

class UsuarioCreateSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True, min_length=8, style={"input_type": "password"})

    class Meta:
        model = Usuario
        fields = ("nombre_usuario", "nombre", "correo", "password", "activo", "acceso_web")

    def validate(self, attrs):
        if Usuario.objects.filter(correo=attrs["correo"]).exists():
            raise serializers.ValidationError({"correo": "Ya existe un usuario con este correo."})
        if Usuario.objects.filter(pk=attrs["nombre_usuario"]).exists():
            raise serializers.ValidationError({"nombre_usuario": "Ya existe un usuario con este nombre de usuario."})
        return attrs

    def create(self, validated):
        plain = validated.pop("password")
        validated["password"] = hash_password(plain)

        user = Usuario.objects.create(**validated)
        return user

class UsuarioUpdateSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True, required=False, min_length=8, style={"input_type": "password"})

    class Meta:
        model = Usuario
        fields = ("nombre", "correo", "activo", "acceso_web", "password")
        extra_kwargs = {
            "correo": {"required": False},
            "nombre": {"required": False},
            "activo": {"required": False},
            "acceso_web": {"required": False},
        }

    def update(self, instance, validated):
        pwd = validated.pop("password", None)
        for k, v in validated.items():
            setattr(instance, k, v)
        if pwd:
            instance.set_password(pwd)
        instance.save()
        return instance


class CampoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Campo
        fields = ("id_campo", "tipo", "clase", "nombre_campo",
                  "etiqueta", "ayuda", "config", "requerido")
   
class PaginaUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Pagina
        fields = ("nombre", "descripcion", "secuencia")
        extra_kwargs = {k: {"required": False} for k in fields}
    
    def update(self, instance, validated_data):
        """
        Actualiza la página y crea una nueva versión para mantener el historial.
        """
        from .services import crear_nueva_version_pagina
        from django.db import transaction
        
        with transaction.atomic():
            # 1. Actualizar los datos de la página
            for field, value in validated_data.items():
                setattr(instance, field, value)
            instance.save()
            
            # 2. Crear nueva versión de la página
            request = self.context.get('request')
            usuario = getattr(request, 'user', None) if request else None
            crear_nueva_version_pagina(
                pagina=instance,
                usuario=usuario,
                razon="Actualización de información de página"
            )
        
        return instance


class CampoUpdateSerializer(serializers.ModelSerializer):
    """
    Serializer para actualizar campos existentes.
    
    Cuando se actualiza un campo, automáticamente se versionan TODAS las páginas
    que contienen ese campo. Esto asegura que:
    - Los cambios en campos se reflejen en todas las páginas afectadas
    - Se mantenga un historial completo de modificaciones
    - Se pueda revertir a versiones anteriores si es necesario
    
    Campos actualizables:
    - etiqueta: Texto visible del campo
    - ayuda: Texto de ayuda para el usuario
    - requerido: Si el campo es obligatorio
    - config: Configuración JSON del campo
    
    Config soporta dos modos de actualización:
    - Merge (default): Combina el config existente con el nuevo
    - Replace (?replace_config=true): Reemplaza completamente el config
    """
    
    config = serializers.JSONField(required=False)

    class Meta:
        model = Campo
        fields = ("etiqueta", "ayuda", "requerido", "config")
        extra_kwargs = {k: {"required": False} for k in fields}

    def _deep_merge(self, base: dict, patch: dict) -> dict:
        """
        Combina dos diccionarios de forma recursiva.
        Los valores en 'patch' sobrescriben los de 'base'.
        
        Args:
            base: Diccionario base
            patch: Diccionario con cambios a aplicar
            
        Returns:
            Diccionario combinado
            
        Example:
            >>> base = {"a": 1, "b": {"c": 2, "d": 3}}
            >>> patch = {"b": {"c": 999}, "e": 5}
            >>> result = _deep_merge(base, patch)
            >>> # result = {"a": 1, "b": {"c": 999, "d": 3}, "e": 5}
        """
        for k, v in patch.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                base[k] = self._deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    def update(self, instance, validated):
        """
        Actualiza el campo y versiona todas las páginas que lo contienen.
        
        Proceso:
        1. Actualiza los campos simples (etiqueta, ayuda, requerido)
        2. Procesa el config (merge o replace según query param)
        3. Guarda los cambios en el campo
        4. Identifica todas las páginas que contienen este campo
        5. Crea una nueva versión para cada página afectada
        
        Args:
            instance: La instancia de Campo a actualizar
            validated: Datos validados del request
            
        Returns:
            La instancia de Campo actualizada
            
        Query Parameters:
            ?replace_config=true : Reemplaza completamente el config
            ?replace_config=false : Hace merge del config (default)
        """
        from .services import versionar_paginas_por_campo
        from django.db import transaction
        
        with transaction.atomic():
            # ===== 1. ACTUALIZAR CONFIG (si viene en el request) =====
            cfg_patch = validated.pop("config", None)
            
            # Actualizar campos simples
            for k, v in validated.items():
                setattr(instance, k, v)

            if cfg_patch is not None:
                request = self.context.get("request")
                replace_all = False
                
                # Determinar si se reemplaza o se hace merge del config
                if request:
                    q = request.query_params
                    replace_all = (q.get("replace_config") or "").lower() in ("1", "true", "yes")

                # Parsear config actual
                try:
                    current = json.loads(instance.config) if instance.config else {}
                except Exception:
                    current = {}

                # Aplicar cambios según el modo
                if replace_all:
                    # Modo REPLACE: reemplazar todo el config
                    merged = cfg_patch or {}
                else:
                    # Modo MERGE: combinar configs (default)
                    if not isinstance(cfg_patch, dict):
                        raise serializers.ValidationError({
                            "config": "Debe ser un objeto JSON"
                        })
                    merged = self._deep_merge(
                        current if isinstance(current, dict) else {}, 
                        cfg_patch
                    )

                instance.config = json.dumps(merged, ensure_ascii=False)

            # ===== 2. GUARDAR CAMBIOS EN EL CAMPO =====
            instance.save()
            
            # ===== 3. VERSIONAR TODAS LAS PÁGINAS AFECTADAS =====
            # Esto es CRÍTICO: cada página que contiene este campo debe
            # crear una nueva versión para reflejar el cambio
            request = self.context.get('request')
            usuario = getattr(request, 'user', None) if request else None
            
            # La función versionar_paginas_por_campo se encarga de:
            # - Encontrar todas las páginas que tienen este campo
            # - Crear una nueva PaginaVersion para cada una
            # - Copiar todos los campos de la versión anterior
            versiones_creadas = versionar_paginas_por_campo(instance, usuario)
            
            # Log opcional (puedes comentar esto en producción)
            if versiones_creadas:
                print(f"✓ Campo '{instance.nombre_campo}' actualizado")
                print(f"✓ Se versionaron {len(versiones_creadas)} página(s) afectada(s)")
        
        return instance

class FormularioUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Formulario
        fields = (
            "categoria",              
            "nombre",
            "descripcion",
            "permitir_fotos",
            "permitir_gps",
            "disponible_desde_fecha", 
            "disponible_hasta_fecha", 
            "estado",                 
            "forma_envio",            
            "es_publico",
            "auto_envio",
        )
        extra_kwargs = {f: {"required": False} for f in fields}

    def validate(self, attrs):
        d = attrs.get("disponible_desde_fecha")
        h = attrs.get("disponible_hasta_fecha")
        if d and h and d > h:
            raise serializers.ValidationError(
                {"disponible_hasta_fecha": "Debe ser >= disponible_desde_fecha"}
            )
        return attrs

class UsuarioAsignarFormulariosSerializer(serializers.Serializer):
    formularios = serializers.ListField(
        child=serializers.UUIDField(format="hex_verbose"),
        allow_empty=False
    )
    replace = serializers.BooleanField(required=False, default=False)

    def validate_formularios(self, value):
        return list(dict.fromkeys(value))

class UsuarioLiteSerializer(serializers.ModelSerializer):
    id = serializers.CharField(source="nombre_usuario", read_only=True)

    class Meta:
        model = Usuario
        fields = ("id", "nombre_usuario", "nombre")

class FormularioLiteSerializer(serializers.ModelSerializer):
    categoria_nombre = serializers.CharField(source="categoria.nombre", read_only=True)
    class Meta:
        model = Formulario
        fields = ("id", "nombre", "categoria_nombre")

class UserFormularioSerializer(serializers.ModelSerializer):
    usuario = UsuarioLiteSerializer(source="id_usuario", read_only=True)
    formulario = FormularioLiteSerializer(source="id_formulario", read_only=True)

    class Meta:
        model = UserFormulario
        fields = ("id", "usuario", "formulario")

class AsignacionBulkSerializer(serializers.Serializer):
    """
    Recibe:
      - usuario: username o UUID del usuario
      - formularios: lista de UUIDs de formularios
      - replace (opcional): si True, reemplaza el set (elimina los no incluidos)
    """
    usuario = serializers.CharField(required=True)
    formularios = serializers.ListField(
        child=serializers.UUIDField(format="hex_verbose"),
        allow_empty=False
    )
    replace = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        u_raw = attrs["usuario"]
        form_ids = list(dict.fromkeys(attrs["formularios"]))

        try:
            user = Usuario.objects.get(models.Q(nombre_usuario__iexact=u_raw))
        except Usuario.DoesNotExist:
            raise serializers.ValidationError({"usuario": "Usuario no existe."})

        existentes = set(Formulario.objects.filter(id__in=form_ids).values_list("id", flat=True))
        faltantes = [str(x) for x in form_ids if x not in existentes]
        if faltantes:
            raise serializers.ValidationError({"formularios": f"IDs inexistentes: {', '.join(faltantes)}"})

        attrs["user_obj"] = user
        attrs["form_ids"] = list(existentes)
        return attrs