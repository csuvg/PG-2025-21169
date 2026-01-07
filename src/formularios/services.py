# # services.py
from hashlib import sha256
from io import BytesIO
import json
import uuid
from django.db import transaction, connection

from django.apps import apps
from typing import Dict, Any
from django.db import transaction
from django.core.exceptions import ValidationError
from django.utils import timezone
import re
from argon2.low_level import hash_secret, verify_secret, Type
from os import urandom

from typing import List, Dict, Any, Optional
import pandas as pd

from formularios.azure_storage import AzureBlobStorageService

from .models import (
    Formulario,
    Formulario_Index_Version,
    FormularioIndexVersion,
    FuenteDatos,
    FuenteDatosValor,
    Grupo,
    Pagina,
    PaginaVersion,
    ClaseCampo,
    Campo,
    PaginaCampo,
    Pagina_Index_Version
)
from formularios import models

def _uuid32_no_dashes(s: str) -> str:
    s = s.strip().lower()
    # si ya viene sin guiones (32 hex), devuélvelo
    if re.fullmatch(r"[0-9a-f]{32}", s):
        return s
    # si viene con guiones (8-4-4-4-12), quítalos
    s = s.replace("-", "")
    if re.fullmatch(r"[0-9a-f]{32}", s):
        return s
    raise ValueError("id_pagina inválido: debe ser UUID v4.")

def uuid32(u=None) -> str:
    """
    Convierte uuid.UUID o str a formato 32 chars (sin guiones).
    Si no se pasa argumento, genera un nuevo UUID.
    """
    if u is None:
        u = uuid.uuid4()
    s = str(u)
    return s.replace("-", "").lower()  # 32 chars

def _first_or_same(x):
    if isinstance(x, (list, tuple)) and x:
        return x[0]
    return x

def _ensure_str_uuid():
    return uuid.uuid4().hex  


@transaction.atomic
def activar_version(formulario, nueva_version) -> None:
    Formulario_Index_Version = apps.get_model("formularios", "Formulario_Index_Version")

    Formulario_Index_Version.objects.get_or_create(
        id_index_version=nueva_version,                 
        defaults={"id_formulario": formulario},
    )

    try:
        FormularioIndex = apps.get_model("formularios", "FormularioIndex")
    except LookupError:
        FormularioIndex = None

    if FormularioIndex:
        FormularioIndex.objects.update_or_create(
            id_formulario=formulario,
            defaults={"id_index_version": nueva_version},
        )

    try:
        PaginaIndex = apps.get_model("formularios", "PaginaIndex")
    except LookupError:
        PaginaIndex = None

    if PaginaIndex:
        from .models import Pagina
        for p in Pagina.objects.filter(index_version=nueva_version).only("id_pagina"):
            PaginaIndex.objects.update_or_create(
                id_pagina=p,
                defaults={
                    "id_index_version": nueva_version,
                    "id_formulario": formulario,
                },
            )

_CLASE_A_TIPO = {
    "string":  "texto",
    "text":    "texto",
    "list":    "texto",
    "hour":    "hour",
    "group":   "texto",
    "date":    "date",
    "number":  "numerico",
    "calc":    "numerico",
    "boolean": "booleano",
    "firm":    "imagen",
    "dataset": "texto",
}

def _resolver_tipo_por_clase(clase: str) -> str:
    return _CLASE_A_TIPO.get((clase or "").strip().lower(), "texto")

def _ultima_pagina_version(pagina: Pagina) -> PaginaVersion | None:
    return (PaginaVersion.objects
            .filter(id_pagina=pagina)
            .order_by("-fecha_creacion")
            .first())

@transaction.atomic
def crear_campo_y_versionar_pagina(pagina: Pagina, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea un nuevo campo en una página y genera una nueva versión.
    
    CAMBIO CLAVE: Ahora MUEVE los campos existentes de la versión anterior
    a la nueva versión, en lugar de copiarlos.
    """
    clase = (data.get("clase") or "").strip().lower()
    if not clase:
        raise ValidationError("El campo 'clase' es obligatorio.")
    if not ClaseCampo.objects.filter(clase=clase).exists():
        raise ValidationError(f"La clase '{clase}' no existe en formularios_clase_campo.")
    tipo = _resolver_tipo_por_clase(clase)

    cfg = data.get("config") or {}
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg or "{}")
        except Exception:
            raise ValidationError("El campo 'config' debe ser JSON válido.")

    # 1) Crear el nuevo Campo
    campo = Campo.objects.create(
        tipo=tipo,
        clase=clase,
        nombre_campo=(data.get("nombre_campo") or f"{clase}_{timezone.now().strftime('%H%M%S')}").strip(),
        etiqueta=(data.get("etiqueta") or "").strip(),
        ayuda=(data.get("ayuda") or "").strip(),
        config=cfg,
        requerido=bool(data.get("requerido", False)),
    )

    # 2) Resolver versión/formulario actual de la página
    piv_actual = (Pagina_Index_Version.objects
                  .filter(id_pagina=pagina)
                  .select_related("id_index_version")
                  .order_by("-id_index_version__fecha_creacion")
                  .first())
    if not piv_actual:
        fiv_nueva = FormularioIndexVersion.objects.create()
        Pagina_Index_Version.objects.create(id_pagina=pagina, id_index_version=fiv_nueva)
        piv_actual = Pagina_Index_Version.objects.get(id_pagina=pagina, id_index_version=fiv_nueva)

    fiv_actual = piv_actual.id_index_version

    # 3) Encontrar el formulario al que pertenece esa versión
    f_link = Formulario_Index_Version.objects.filter(id_index_version=fiv_actual).first()
    if not f_link:
        raise ValidationError("No se pudo resolver el formulario para la versión actual de la página.")
    formulario = f_link.id_formulario

    # 4) Crear NUEVA FormularioIndexVersion (publicación v+1)
    fiv_nueva = FormularioIndexVersion.objects.create()
    Formulario_Index_Version.objects.get_or_create(
        id_index_version=fiv_nueva,
        defaults={"id_formulario": formulario},
    )

    # 5) Obtener la última versión de la página para calcular sequence
    prev_pv = _ultima_pagina_version(pagina)
    
    # 6) Crear la NUEVA PaginaVersion
    nueva_pv = PaginaVersion.objects.create(
        id_pagina_version=uuid.uuid4().hex,
        fecha_creacion=timezone.now(),
        id_pagina=pagina,
    )

    # 7) MOVER (no copiar) los campos de la versión anterior a la nueva
    # Esto es CRÍTICO: como id_campo es PK, debemos mover en lugar de copiar
    if prev_pv:
        PaginaCampo.objects.filter(
            id_pagina_version=prev_pv
        ).update(
            id_pagina_version=nueva_pv
        )

    # 8) Calcular sequence para el nuevo campo
    max_seq = (PaginaCampo.objects
               .filter(id_pagina_version=nueva_pv)
               .aggregate(max_seq=models.Max('sequence'))
               .get('max_seq') or 0)
    
    sequence = data.get("sequence")
    try:
        sequence = int(sequence) if sequence is not None else None
    except Exception:
        sequence = None
    if sequence is None:
        sequence = (max_seq + 1) if max_seq else 1

    # 9) Asociar el NUEVO campo a la nueva versión de la página
    PaginaCampo.objects.create(
        id_campo=campo,
        id_pagina_version=nueva_pv,
        sequence=sequence,
    )

    # 10) Actualizar punteros de TODAS las páginas del formulario a la nueva versión
    paginas_del_form = (Pagina_Index_Version.objects
                        .filter(id_index_version=fiv_actual)
                        .values_list("id_pagina", flat=True))
    for pid in paginas_del_form:
        Pagina_Index_Version.objects.update_or_create(
            id_pagina_id=pid,
            defaults={"id_index_version": fiv_nueva},
        )

    return {
        "campo_id": str(campo.id_campo),
        "formulario_id": str(formulario.id),
        "pagina_id": str(pagina.id_pagina),
        "nueva_version_id": str(fiv_nueva.id_index_version),
        "pagina_version_id": str(nueva_pv.id_pagina_version),
        "sequence": sequence,
    }


@transaction.atomic
def versionar_paginas_por_campo(campo: Campo, usuario=None) -> List[PaginaVersion]:
    """
    Crea una nueva versión para TODAS las páginas que contienen un campo específico.
    
    Se usa cuando se actualiza un campo existente (etiqueta, ayuda, config, etc.)
    para reflejar el cambio en todas las páginas que usan ese campo.
    
    IMPORTANTE: Como id_campo es PRIMARY KEY en PaginaCampo, debemos MOVER
    los campos entre versiones en lugar de copiarlos.
    
    Args:
        campo: El campo que fue modificado
        usuario: Usuario que realizó la modificación (opcional)
    
    Returns:
        Lista de PaginaVersion creadas
    """
    versiones_creadas = []
    
    # 1) Encontrar todas las versiones de página que contienen este campo
    versiones_con_campo = (PaginaCampo.objects
                          .filter(id_campo=campo)
                          .select_related('id_pagina_version__id_pagina')
                          .values_list('id_pagina_version__id_pagina', flat=True)
                          .distinct())
    
    # 2) Para cada página que contiene este campo, crear una nueva versión
    for pagina_id in versiones_con_campo:
        try:
            pagina = Pagina.objects.get(id_pagina=pagina_id)
            
            # Obtener la versión actual de la página
            version_actual = (PaginaVersion.objects
                            .filter(id_pagina=pagina)
                            .order_by('-fecha_creacion')
                            .first())
            
            if not version_actual:
                continue
            
            # Crear nueva versión de la página
            nueva_version = PaginaVersion.objects.create(
                id_pagina_version=uuid.uuid4().hex,
                id_pagina=pagina,
                fecha_creacion=timezone.now(),
            )
            
            # MOVER todos los campos de la versión actual a la nueva versión
            # Esto incluye el campo modificado y todos los demás campos
            PaginaCampo.objects.filter(
                id_pagina_version=version_actual
            ).update(
                id_pagina_version=nueva_version
            )
            
            # Actualizar el puntero de versión del formulario
            piv_actual = (Pagina_Index_Version.objects
                         .filter(id_pagina=pagina)
                         .select_related("id_index_version")
                         .first())
            
            if piv_actual:
                fiv_actual = piv_actual.id_index_version
                f_link = Formulario_Index_Version.objects.filter(
                    id_index_version=fiv_actual
                ).first()
                
                if f_link:
                    formulario = f_link.id_formulario
                    fiv_nueva = FormularioIndexVersion.objects.create()
                    
                    Formulario_Index_Version.objects.get_or_create(
                        id_index_version=fiv_nueva,
                        defaults={"id_formulario": formulario},
                    )
                    
                    # Actualizar punteros de TODAS las páginas del formulario
                    paginas_del_form = (Pagina_Index_Version.objects
                                      .filter(id_index_version=fiv_actual)
                                      .values_list("id_pagina", flat=True))
                    
                    for pid in paginas_del_form:
                        Pagina_Index_Version.objects.update_or_create(
                            id_pagina_id=pid,
                            defaults={"id_index_version": fiv_nueva},
                        )
            
            versiones_creadas.append(nueva_version)
            
        except Pagina.DoesNotExist:
            continue
    
    return versiones_creadas
    
TIPO_POR_CLASE = {
    "number": "number",
    "boolean": "boolean",
    "text": "text",
    "string": "text",
    "date": "date",
    "hour": "hour",
    "list": "list",
    "group": "group",
    "calc": "calc",
    "dataset": "dataset",
    "firm": "firm",
}

def _uuid32() -> str:
    return uuid.uuid4().hex

def _pagina_version_actual_o_nueva(id_pagina: str, crear_nueva: bool = False) -> PaginaVersion:
    """
    Obtiene la PaginaVersion más reciente de una página, o crea una nueva si no existe.
    
    Args:
        id_pagina: UUID de la página
        crear_nueva: Si True, SIEMPRE crea una nueva versión y mueve los campos
    
    Returns:
        PaginaVersion (nueva o existente según crear_nueva)
    """
    from .models import Pagina as PaginaModel
    pagina_obj = PaginaModel.objects.get(pk=id_pagina)
    
    # Buscar la versión actual
    pv_actual = (PaginaVersion.objects
                 .filter(id_pagina=pagina_obj)
                 .order_by("-fecha_creacion")
                 .first())
    
    # Si no existe ninguna versión, crear la primera
    if not pv_actual:
        pv = PaginaVersion.objects.create(
            id_pagina_version=uuid.uuid4().hex,
            fecha_creacion=timezone.now(),
            id_pagina=pagina_obj,
        )
        return pv
    
    # Si no se solicita crear nueva, devolver la actual
    if not crear_nueva:
        return pv_actual
    
    # CREAR NUEVA VERSIÓN y mover campos
    pv_nueva = PaginaVersion.objects.create(
        id_pagina_version=uuid.uuid4().hex,
        fecha_creacion=timezone.now(),
        id_pagina=pagina_obj,
    )
    
    # MOVER todos los campos de la versión actual a la nueva
    PaginaCampo.objects.filter(
        id_pagina_version=pv_actual
    ).update(
        id_pagina_version=pv_nueva
    )
    
    # Actualizar punteros del formulario
    _actualizar_punteros_formulario(pagina_obj, pv_nueva)
    
    return pv_nueva


# ============================================================================
# AGREGAR esta nueva función auxiliar (después de _pagina_version_actual_o_nueva)
# ============================================================================

@transaction.atomic
def _actualizar_punteros_formulario(pagina: "Pagina", nueva_version: PaginaVersion):
    """
    Actualiza los punteros del formulario después de crear una nueva versión de página.
    
    Args:
        pagina: Objeto Pagina
        nueva_version: La nueva PaginaVersion creada
    """
    # Obtener el puntero actual de la página
    piv_actual = (Pagina_Index_Version.objects
                 .filter(id_pagina=pagina)
                 .select_related("id_index_version")
                 .first())
    
    if not piv_actual:
        return
    
    fiv_actual = piv_actual.id_index_version
    
    # Encontrar el formulario
    f_link = Formulario_Index_Version.objects.filter(
        id_index_version=fiv_actual
    ).first()
    
    if not f_link:
        return
    
    formulario = f_link.id_formulario
    
    # Crear nueva versión del formulario
    fiv_nueva = FormularioIndexVersion.objects.create()
    
    Formulario_Index_Version.objects.get_or_create(
        id_index_version=fiv_nueva,
        defaults={"id_formulario": formulario},
    )
    
    # Actualizar punteros de TODAS las páginas del formulario
    paginas_del_form = (Pagina_Index_Version.objects
                       .filter(id_index_version=fiv_actual)
                       .values_list("id_pagina", flat=True))
    
    for pid in paginas_del_form:
        Pagina_Index_Version.objects.update_or_create(
            id_pagina_id=pid,
            defaults={"id_index_version": fiv_nueva},
        )


def _siguiente_sequence(id_pagina_version: str) -> int:
    from django.db.models import Max
    mx = (PaginaCampo.objects
        .filter(id_pagina_version=id_pagina_version)
        .aggregate(Max('sequence'))['sequence__max'] or 0)
    return int(mx) + 1

@transaction.atomic
def crear_campo_en_pagina(id_pagina: str, payload: dict) -> dict:
    """
    Crea un Campo en la página (versiona página si no existe PV),
    NO guarda enlaces de grupo en config; el enlace a grupo se hace en la vista
    solo si viene request.data["grupo"].

    - Valida clase contra formularios_clase_campo
    - Para clase 'dataset', materializa opciones en FuenteDatosValor y normaliza config.dataset
    - Para clase 'group', crea/actualiza la fila en formularios_grupo con id_campo_group = este Campo
    """
    # -------- 1) Validaciones y determinación de tipo ----------
    clase = (payload.get("clase") or "").strip().lower()
    if not clase:
        raise ValidationError("El campo 'clase' es obligatorio.")

    if not ClaseCampo.objects.filter(pk=clase).exists():
        raise ValidationError(f"La clase '{clase}' no existe en formularios_clase_campo.")

    tipo = TIPO_POR_CLASE.get(clase, clase)

    nombre_campo = (payload.get("nombre_campo") or "").strip()
    etiqueta     = (payload.get("etiqueta") or "").strip()
    ayuda        = (payload.get("ayuda") or "").strip()
    requerido    = payload.get("requerido", None)

    # -------- 2) Normalizar config del payload a dict (nunca usamos cfg_dict "huérfano") ----------
    raw_cfg = payload.get("config", {})
    cfg_dict = {}
    if isinstance(raw_cfg, dict):
        cfg_dict = raw_cfg
    elif isinstance(raw_cfg, str):
        raw_cfg = raw_cfg.strip()
        if raw_cfg:
            try:
                cfg_dict = json.loads(raw_cfg)
            except Exception:
                cfg_dict = {}
    # si viene otra cosa o None, se queda {}.

    # -------- 3) Crear el Campo ----------
    id_campo = _uuid32()
    campo = Campo.objects.create(
        id_campo=id_campo,
        tipo=tipo,
        clase=clase,
        nombre_campo=nombre_campo or f"{clase}_{timezone.now().strftime('%H%M%S')}",
        etiqueta=etiqueta,
        ayuda=ayuda,
        config=(json.dumps(cfg_dict, ensure_ascii=False) if cfg_dict else None),
        requerido=requerido,
    )

    # -------- 4) Casos especiales por clase ----------
    if clase == "dataset":
        # esperamos cfg_dict["dataset"] con al menos fuente_id y modo válido
        ds = (cfg_dict.get("dataset") or {})
        if not isinstance(ds, dict) or not ds.get("fuente_id"):
            raise ValidationError("config.dataset.fuente_id es requerido para campos dataset")

        # materializar catálogo y normalizar columnas finales (case-insensitive) dentro de cfg_dict
        _materializar_dataset_para_campo(cfg_dict, campo)
        # asegurar que 'version' no quede guardado
        if "dataset" in cfg_dict and isinstance(cfg_dict["dataset"], dict):
            cfg_dict["dataset"].pop("version", None)

        campo.config = json.dumps(cfg_dict, ensure_ascii=False)
        campo.save(update_fields=["config"])

    elif clase == "group":
    # 1) Crear/actualizar el Grupo para este campo "group"
        nombre_grupo = (etiqueta or nombre_campo or "Grupo")[:150]
        g, _created = Grupo.objects.update_or_create(
            id_campo_group=campo,
            defaults={"nombre": nombre_grupo},
        )

        # 2) Asegurar que el config sea dict
        if not isinstance(cfg_dict, dict):
            cfg_dict = {}

        # 3) Inyectar en config los metadatos mínimos del grupo
        #    - id_group siempre debe reflejar el UUID real
        #    - name por conveniencia para el front
        #    - fieldCondition default vacío si no existe
        cfg_dict["id_group"] = str(g.id_grupo)
        cfg_dict.setdefault("name", nombre_grupo)
        cfg_dict.setdefault("fieldCondition", "")

        # 4) Persistir el config actualizado en el Campo
        campo.config = json.dumps(cfg_dict, ensure_ascii=False)
        campo.save(update_fields=["config"])

    # -------- 5) Obtener/crear la PaginaVersion destino y calcular sequence ----------
    pv = _pagina_version_actual_o_nueva(id_pagina, crear_nueva=True)

    seq = payload.get("sequence", None)
    try:
        seq = int(seq) if seq is not None else None
    except Exception:
        seq = None
    if seq is None:
        seq = _siguiente_sequence(pv.id_pagina_version)

    # -------- 6) Insertar link en formularios_pagina_campo ----------
    PaginaCampo.objects.create(
        id_campo=campo,
        id_pagina_version=pv,
        sequence=seq,
    )

    # -------- 7) Respuesta ----------
    return {
        "id_campo": str(campo.id_campo),
        "id_grupo": str(Grupo.objects.filter(id_campo_group=campo).values_list("id_grupo", flat=True).first() or ""),
        "tipo": campo.tipo,
        "clase": campo.clase,
        "nombre_campo": campo.nombre_campo,
        "etiqueta": campo.etiqueta,
        "id_pagina": id_pagina,
        "id_pagina_version": pv.id_pagina_version,
        "sequence": seq,
    }

def hash_password(plain: str) -> str:
    salt = urandom(16)
    phc = hash_secret(
        secret=plain.encode("utf-8"),
        salt=salt,
        time_cost=3,
        memory_cost=65536,   
        parallelism=1,
        hash_len=32,
        type=Type.ID,
        version=19,
    )
    return phc.decode("utf-8")

def verify_password(hash_phc: str, plain: str) -> bool:
    return verify_secret(hash_phc.encode("utf-8"), plain.encode("utf-8"), Type.ID)

@transaction.atomic
def duplicar_formulario(formulario: Formulario, nuevo_nombre: str | None = None) -> Formulario:
    clon = Formulario.objects.create(
        categoria=formulario.categoria,
        nombre=nuevo_nombre or f"{formulario.nombre}_Copia",
        descripcion=formulario.descripcion,
        permitir_fotos=formulario.permitir_fotos,
        permitir_gps=formulario.permitir_gps,
        disponible_desde_fecha=formulario.disponible_desde_fecha,
        disponible_hasta_fecha=formulario.disponible_hasta_fecha,
        estado=formulario.estado,
        forma_envio=formulario.forma_envio,
        es_publico=formulario.es_publico,
        auto_envio=formulario.auto_envio,
    )

    # versión nueva para el clon + historial
    idx_clon = FormularioIndexVersion.objects.create()
    Formulario_Index_Version.objects.create(id_index_version=idx_clon, id_formulario=clon)

    # última versión del original (por fecha)
    link_orig = (Formulario_Index_Version.objects
                 .filter(id_formulario=formulario)
                 .select_related("id_index_version")
                 .order_by("-id_index_version__fecha_creacion")
                 .first())
    paginas_origen = Pagina.objects.none()
    if link_orig:
        page_ids = (Pagina_Index_Version.objects
                    .filter(id_index_version=link_orig.id_index_version)
                    .values_list("id_pagina", flat=True))
        paginas_origen = Pagina.objects.filter(id_pagina__in=list(page_ids)).order_by("secuencia")

    # clonar páginas
    for p in paginas_origen:
        p_nueva = Pagina.objects.create(
            secuencia=p.secuencia,
            nombre=p.nombre,
            descripcion=p.descripcion,
        )
        Pagina_Index_Version.objects.create(id_pagina=p_nueva, id_index_version=idx_clon)

        pv_orig = (PaginaVersion.objects
                   .filter(id_pagina=p)
                   .order_by("-fecha_creacion")
                   .first())

        pv_nueva = PaginaVersion.objects.create(
            id_pagina_version=uuid.uuid4().hex,
            id_pagina=p_nueva,
            fecha_creacion=timezone.now(),
        )

        if pv_orig:
            links = (PaginaCampo.objects
                     .filter(id_pagina_version=pv_orig)
                     .order_by("sequence"))

            for l in links:
                c = l.id_campo
                c_nuevo = Campo.objects.create(
                    id_campo=uuid.uuid4(),
                    tipo=c.tipo,
                    clase=c.clase,
                    nombre_campo=c.nombre_campo,
                    etiqueta=c.etiqueta,
                    ayuda=c.ayuda,
                    config=c.config,
                    requerido=c.requerido,
                )
                PaginaCampo.objects.create(
                    id_pagina_version=pv_nueva,
                    id_campo=c_nuevo,
                    sequence=l.sequence,
                )

    return clon

def versionar_pagina_sin_clonar(pagina) -> PaginaVersion:
    prev = (PaginaVersion.objects
            .filter(id_pagina=pagina)
            .order_by('-fecha_creacion')
            .first())

    nueva_pv = PaginaVersion.objects.create(
        id_pagina_version=uuid.uuid4().hex,
        id_pagina=pagina,
        fecha_creacion=timezone.now(),
    )

    if prev:
        links = (PaginaCampo.objects
                 .filter(id_pagina_version=prev)
                 .order_by('sequence'))
        PaginaCampo.objects.bulk_create([
            PaginaCampo(
                id_pagina_version=nueva_pv,
                id_campo=l.id_campo,
                sequence=l.sequence
            )
            for l in links
        ])
    return nueva_pv

@transaction.atomic
def _materializar_dataset_para_campo(cfg: dict, campo):
    """
    Lee el blob de FuenteDatos y llena FuenteDatosValor para ESTE campo.
    **Sin versiones**: borra lo existente y re-materializa.
    Retorna rows_insertadas (int).
    """
    ds = (cfg or {}).get("dataset") or {}
    fuente_id = ds.get("fuente_id")
    mode = (ds.get("mode") or "pair").lower()  # "pair" o "single"
    alias = ds.get("column") or ds.get("label_column") or "dataset"

    if not fuente_id:
        raise ValidationError("dataset.fuente_id es requerido")

    f = FuenteDatos.objects.get(pk=fuente_id)

    storage = AzureBlobStorageService()
    content = storage.download_file(f.blob_name)
    ext = (f.archivo_nombre or f.blob_name).split(".")[-1].lower()
    file_obj = BytesIO(content)

    # Lee Excel/CSV como texto
    if ext in ("xlsx", "xls"):
        df = pd.read_excel(file_obj, dtype=str)
    else:
        df = pd.read_csv(file_obj, dtype=str)

    df = df.fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    # Índice case-insensitive de columnas + chequeo de colisiones (p.ej. 'ID' y 'id')
    lower_idx = {}
    for c in df.columns:
        k = c.lower()
        if k in lower_idx and lower_idx[k] != c:
            raise ValidationError(
                f"Columnas duplicadas que solo difieren por mayúsculas/minúsculas: "
                f"'{lower_idx[k]}' y '{c}'. Renombra en la fuente."
            )
        lower_idx[k] = c

    def resolve_col(name: str | None, default: str | None = None) -> str:
        """
        Devuelve el nombre EXACTO presente en el DF, resolviendo case-insensitive.
        Si name es None, usa default. Lanza error si no existe.
        """
        target = (name or default or "").strip()
        if not target:
            raise ValidationError("No se especificó columna requerida.")
        real = lower_idx.get(target.lower())
        if not real:
            raise ValidationError(
                f"Columna '{name or default}' no existe en la fuente. "
                f"Disponibles: {sorted(df.columns)}"
            )
        return real

    # cols = set(map(str, df.columns))
    # if mode == "single":
    #     col = ds.get("column")
    #     if not col or col not in cols:
    #         raise ValidationError(
    #             f"Columna '{col}' no existe en la fuente. Disponibles: {sorted(cols)}"
    #         )
    # else:  # pair
    #     kcol, lcol = ds.get("key_column"), ds.get("label_column")
    #     missing = [x for x in (kcol, lcol) if not x or x not in cols]
    #     if missing:
    #         raise ValidationError(
    #             f"Columnas faltantes en la fuente: {missing}. Disponibles: {sorted(cols)}"
    #         )

    # --- (2) Resolver columnas según el modo (case-insensitive) ---
    if mode == "single":
        col_real = resolve_col(ds.get("column"))
        # Persistimos el nombre real de la columna en el config
        ds["column"] = col_real
        alias = col_real  # alias útil para rastrear en FuenteDatosValor.columna
    elif mode == "pair":
        # default 'id' si no viene key_column; resolverá 'ID', 'Id', etc.
        kcol_real = resolve_col(ds.get("key_column"), default="id")
        lcol_real = resolve_col(ds.get("label_column"))
        ds["key_column"] = kcol_real
        ds["label_column"] = lcol_real
        # Usamos label_column como alias por defecto (o puedes mantener tu criterio original)
        ds["column"] = lcol_real
        alias = ds.get("label_column") or "dataset"
    else:
        raise ValidationError("dataset.mode debe ser 'single' o 'pair'")

    # Trim de todas las columnas
    for c in df.columns:
        df[c] = df[c].astype(str).map(lambda x: x.strip())

    # Limpia valores previos del campo
    FuenteDatosValor.objects.filter(campo=campo).delete()

    # Construye filas
    rows = []
    if mode == "single":
        col = ds["column"]
        # únicos + ordenados; evita vacíos
        serie = (
            df[col]
            .dropna()
            .map(lambda x: x.strip())
            .loc[lambda s: s != ""]
            .drop_duplicates()
            .sort_values()
        )
        for v in serie:
            rows.append(
                FuenteDatosValor(
                    campo=campo,
                    fuente=f,               # <-- requiere tener FK fuente en el modelo
                    columna=alias,
                    key_text=None,
                    label_text=v,
                    valor_raw={"value": v},
                    extras={},
                )
            )
    else:
        kcol, lcol = ds["key_column"], ds["label_column"]
        tmp = (
            df[[kcol, lcol]]
            .dropna()
            .assign(
                **{
                    kcol: df[kcol].map(lambda x: (x or "").strip()),
                    lcol: df[lcol].map(lambda x: (x or "").strip()),
                }
            )
            .loc[lambda d: (d[kcol] != "") & (d[lcol] != "")]
            .drop_duplicates()
            .sort_values(by=[lcol, kcol])
        )

        for _, r in tmp.iterrows():
            k, l = r[kcol], r[lcol]
            rows.append(
                FuenteDatosValor(
                    campo=campo,
                    fuente=f,               # <-- requiere tener FK fuente en el modelo
                    columna=alias,
                    key_text=k,
                    label_text=l,
                    valor_raw={kcol: k, lcol: l},
                    extras={},
                )
            )

    if rows:
        # ajusta batch_size si manejas catálogos muy grandes
        FuenteDatosValor.objects.bulk_create(rows, batch_size=5000)

    # Limpia cualquier rastro viejo de versión en el config
    ds.pop("version", None)
    cfg["dataset"] = ds

    return len(rows)

def fetch_items_from_fdv_by_campo(
    campo_id: str,
    label_column: Optional[str] = None,
    fuente_id: Optional[str] = None,
    limit: Optional[int] = 300,
) -> List[Dict[str, Any]]:
    """
    Devuelve pares {key, label} desde formularios_fuente_datos_valor
    filtrando por campo_id y, si se especifica, por fuente_id y/o columna.
    """
    qs = FuenteDatosValor.objects.filter(campo_id=str(campo_id))
    if fuente_id:
        qs = qs.filter(fuente_id=str(fuente_id))
    if label_column:
        qs = qs.filter(columna=label_column)

    qs = qs.values("key_text", "label_text").order_by("label_text")
    if limit and limit > 0:
        qs = qs[:limit]

    # normaliza a pares {key,label}
    out = [{"key": r["key_text"], "label": r["label_text"]} for r in qs]
    return out

@transaction.atomic
def crear_nueva_version_pagina(pagina: Pagina, usuario=None, razon: str = None) -> PaginaVersion:
    """
    Crea una nueva versión de una página MOVIENDO los campos de la versión anterior.
    
    IMPORTANTE: Como id_campo es PRIMARY KEY en PaginaCampo, un campo solo puede 
    estar asociado a UNA versión de página a la vez. Por lo tanto, MOVEMOS los 
    campos de la versión anterior a la nueva versión en lugar de copiarlos.
    
    Esto significa que:
    - La versión anterior quedará SIN campos (vacía)
    - La nueva versión tendrá TODOS los campos que tenía la anterior
    - Se mantiene el constraint de PRIMARY KEY de id_campo
    - El historial se mantiene porque PaginaVersion sigue existiendo
    """
    # 1) Obtener la última versión de la página
    ultima_version = (PaginaVersion.objects
                     .filter(id_pagina=pagina)
                     .order_by("-fecha_creacion")
                     .first())
    
    # 2) Crear la nueva versión
    nueva_version = PaginaVersion.objects.create(
        id_pagina_version=uuid.uuid4().hex,
        id_pagina=pagina,
        fecha_creacion=timezone.now(),
    )
    
    # 3) MOVER (no copiar) los campos de la versión anterior a la nueva
    # Como id_campo es PRIMARY KEY, un campo solo puede estar en UNA versión
    if ultima_version:
        # Actualizar todos los PaginaCampo para que apunten a la nueva versión
        # Esto automáticamente los "mueve" de la versión anterior a la nueva
        PaginaCampo.objects.filter(
            id_pagina_version=ultima_version
        ).update(
            id_pagina_version=nueva_version
        )
    
    # 4) Actualizar el puntero de versión del formulario
    piv_actual = (Pagina_Index_Version.objects
                 .filter(id_pagina=pagina)
                 .select_related("id_index_version")
                 .first())
    
    if piv_actual:
        fiv_actual = piv_actual.id_index_version
        f_link = Formulario_Index_Version.objects.filter(
            id_index_version=fiv_actual
        ).first()
        
        if f_link:
            formulario = f_link.id_formulario
            fiv_nueva = FormularioIndexVersion.objects.create()
            
            Formulario_Index_Version.objects.get_or_create(
                id_index_version=fiv_nueva,
                defaults={"id_formulario": formulario},
            )
            
            paginas_del_form = (Pagina_Index_Version.objects
                              .filter(id_index_version=fiv_actual)
                              .values_list("id_pagina", flat=True))
            
            for pid in paginas_del_form:
                Pagina_Index_Version.objects.update_or_create(
                    id_pagina_id=pid,
                    defaults={"id_index_version": fiv_nueva},
                )
    
    return nueva_version