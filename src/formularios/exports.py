# exports.py - Versión corregida para cargar campos de grupo desde BD
from collections import OrderedDict
from io import BytesIO
import pandas as pd
from django.utils.timezone import localtime
from zipfile import ZipFile, ZIP_DEFLATED
from django.utils import timezone
from .models import FormularioEntry, Grupo, CampoGrupo, Campo
import json


def _to_naive_local(dt):
    """
    Convierte un datetime (aware o naive) a naive en hora local.
    """
    if dt is None:
        return None
    if timezone.is_naive(dt):
        return dt
    return timezone.localtime(dt).replace(tzinfo=None)


def _sanitize_filename(name: str, maxlen: int = 60) -> str:
    safe = "".join(ch if ch.isalnum() or ch in " _-.,()" else "_" for ch in (name or ""))
    return safe[:maxlen] or "export"


def _obtener_campos_grupo_desde_bd(id_grupo):
    """
    Obtiene los campos de un grupo desde la base de datos.
    """
    try:
        grupo = Grupo.objects.filter(id_grupo=id_grupo).first()
        if not grupo:
            return []
        
        campos_grupo = []
        relaciones = CampoGrupo.objects.filter(id_grupo=grupo).select_related('id_campo').all()
        
        for relacion in relaciones:
            campo = relacion.id_campo
            campos_grupo.append({
                "nombre_interno": campo.nombre_campo,
                "etiqueta": campo.etiqueta,
                "clase": (campo.clase or "").lower(),
                "tipo": (campo.tipo or "").lower(),
                "id_campo": str(campo.id_campo)
            })
        
        return campos_grupo
    except Exception as e:
        print(f"Error cargando campos de grupo desde BD: {e}")
        return []


def _build_field_catalog(form_json: dict) -> list[dict]:
    """
    Del form_json construye un catálogo de campos con:
    id_pagina, id_campo, nombre_interno, etiqueta, clase, requerido, sequence
    """
    out = []
    if not isinstance(form_json, dict):
        return out
    
    paginas = (form_json.get("paginas") or []) if isinstance(form_json.get("paginas"), list) else []
    
    for p in paginas:
        pid = p.get("id_pagina")
        campos = p.get("campos") or []
        
        for c in campos:
            # Extraer información básica del campo
            campo_info = {
                "id_pagina": pid,
                "id_campo": c.get("id_campo"),
                "nombre_interno": c.get("nombre_interno"),
                "etiqueta": c.get("etiqueta"),
                "clase": (c.get("clase") or "").lower(),
                "tipo": (c.get("tipo") or "").lower(),
                "requerido": bool(c.get("requerido")),
                "sequence": c.get("sequence") or 0,
            }
            
            # Si es un grupo, obtener sus campos desde la BD usando id_group del config
            if campo_info["clase"] == "group":
                config = c.get("config", {})
                
                # Si config es string JSON, parsearlo
                if isinstance(config, str):
                    try:
                        config = json.loads(config)
                    except:
                        config = {}
                
                # Obtener id_group del config
                id_grupo = config.get("id_group") if isinstance(config, dict) else None
                
                campos_grupo = []
                if id_grupo:
                    # Cargar campos desde la base de datos
                    campos_grupo = _obtener_campos_grupo_desde_bd(id_grupo)
                
                campo_info["campos_grupo"] = campos_grupo
                campo_info["id_grupo"] = id_grupo
            
            out.append(campo_info)
    
    out.sort(key=lambda r: r.get("sequence") or 0)
    return out


def _tiene_grupos(catalog: list[dict]) -> bool:
    """Verifica si el formulario tiene campos de tipo grupo"""
    return any(c.get("clase") == "group" for c in catalog)


def _extraer_campos_normales(catalog: list[dict]) -> list[dict]:
    """Retorna solo los campos que NO son de tipo group"""
    return [c for c in catalog if c.get("clase") != "group"]


def _extraer_campos_grupo(catalog: list[dict]) -> dict:
    """
    Retorna un diccionario con la estructura de los grupos:
    {
        'nombre_interno_grupo': {
            'etiqueta': 'Etiqueta del Grupo',
            'campos': [lista de campos internos]
        }
    }
    """
    grupos = {}
    for meta in catalog:
        if meta.get("clase") == "group":
            nombre_interno = meta.get("nombre_interno")
            
            grupos[nombre_interno] = {
                "etiqueta": meta.get("etiqueta") or nombre_interno,
                "id_campo": meta.get("id_campo"),
                "id_grupo": meta.get("id_grupo"),
                "campos": meta.get("campos_grupo", [])
            }
    return grupos


def _normalizar_valor(valor, clase):
    """Normaliza un valor según su clase"""
    if clase == "boolean":
        if isinstance(valor, str):
            valor = valor.strip().lower() in ("1", "true", "t", "yes", "si", "sí")
        return bool(valor) if valor is not None else None
    
    if clase == "dataset":
        if isinstance(valor, dict):
            return valor.get("label") or valor.get("label_text") or valor.get("value") or valor.get("id") or None
        elif isinstance(valor, (list, tuple)):
            return ", ".join([(v.get("label") if isinstance(v, dict) else str(v)) for v in valor])
    
    return valor


def _flatten_entry_row(entry: FormularioEntry) -> dict:
    """
    Convierte 1 registro de formularios_entry en una fila plana (dict) con:
    metadatos + columnas de respuestas (con etiqueta legible).
    NO incluye campos de tipo group.
    """
    base = OrderedDict()
    base["ID_Respuesta"] = str(entry.id)
    base["Nombre Formulario"] = entry.form_name
    base["Usuario"] = entry.id_usuario
    base["Status"] = entry.status
    base["Llenado"] = _to_naive_local(entry.filled_at_local)
    base["Actualizado"] = _to_naive_local(entry.updated_at)

    form_json = entry.form_json or {}
    fill_json = entry.fill_json or {}

    catalog = _build_field_catalog(form_json)
    campos_normales = _extraer_campos_normales(catalog)

    for meta in campos_normales:
        etiqueta = meta.get("etiqueta") or meta.get("nombre_interno") or meta.get("id_campo")
        etiqueta_col = str(etiqueta).strip()
        pid = meta.get("id_pagina")
        nombre_interno = meta.get("nombre_interno")
        clase = meta.get("clase")

        valor = None
        if pid and nombre_interno:
            page_dict = fill_json.get(str(pid)) or fill_json.get(pid)
            if isinstance(page_dict, dict):
                valor = page_dict.get(nombre_interno)

        valor = _normalizar_valor(valor, clase)
        base[etiqueta_col] = valor

    return base


def _flatten_grupos_entries(entry: FormularioEntry) -> list[dict]:
    """
    Extrae las filas de los grupos en formato normalizado.
    Retorna una lista de diccionarios, uno por cada registro de grupo.
    Cada diccionario contiene:
    - ID_Respuesta (relación con la tabla principal)
    - Nombre_Grupo
    - Campos del grupo
    """
    form_json = entry.form_json or {}
    fill_json = entry.fill_json or {}
    
    catalog = _build_field_catalog(form_json)
    grupos_estructura = _extraer_campos_grupo(catalog)
    
    if not grupos_estructura:
        return []
    
    rows_grupos = []
    
    for nombre_grupo, grupo_info in grupos_estructura.items():
        campos_grupo = grupo_info.get("campos", [])
        
        # Buscar los valores del grupo en fill_json
        valores_grupo = None
        for pid in fill_json.keys():
            page_dict = fill_json.get(str(pid)) or fill_json.get(pid)
            if isinstance(page_dict, dict) and nombre_grupo in page_dict:
                valores_grupo = page_dict.get(nombre_grupo)
                break
        
        # Si no hay valores, continuar
        if not valores_grupo or not isinstance(valores_grupo, list):
            continue
        
        # Procesar cada registro del grupo
        for idx, registro in enumerate(valores_grupo, start=1):
            if not isinstance(registro, dict):
                continue
            
            row = OrderedDict()
            row["ID_Respuesta"] = str(entry.id)
            row["Nombre_Grupo"] = grupo_info["etiqueta"]
            
            # Si no hay estructura de campos, usar las claves del registro
            if not campos_grupo:
                # Usar directamente las claves del registro
                for nombre_campo, valor in registro.items():
                    # Limpiar el nombre del campo para usar como etiqueta
                    etiqueta_campo = nombre_campo.replace("_", " ").title()
                    row[etiqueta_campo] = valor
            else:
                # Agregar los campos del grupo usando la estructura desde BD
                for campo_grupo_info in campos_grupo:
                    nombre_campo = campo_grupo_info.get("nombre_interno")
                    etiqueta_campo = campo_grupo_info.get("etiqueta") or nombre_campo
                    clase_campo = campo_grupo_info.get("clase", "").lower()
                    
                    valor = registro.get(nombre_campo)
                    valor = _normalizar_valor(valor, clase_campo)
                    
                    row[etiqueta_campo] = valor
            
            rows_grupos.append(row)
    
    return rows_grupos


def dataframe_por_form(form_id) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Devuelve dos DataFrames:
    1. DataFrame principal con respuestas (sin grupos)
    2. DataFrame de grupos normalizados (si existen)
    """
    qs = (FormularioEntry.objects
          .filter(form_id=form_id)
          .order_by("filled_at_local", "created_at"))
    
    # DataFrame principal
    rows_principal = [_flatten_entry_row(e) for e in qs]
    df_principal = pd.DataFrame(rows_principal) if rows_principal else pd.DataFrame()
    
    # Verificar si hay grupos
    if qs.exists():
        catalog = _build_field_catalog(qs.first().form_json or {})
        tiene_grupos = _tiene_grupos(catalog)
    else:
        tiene_grupos = False
    
    # DataFrame de grupos
    df_grupos = pd.DataFrame()
    if tiene_grupos:
        rows_grupos = []
        for entry in qs:
            rows_grupos.extend(_flatten_grupos_entries(entry))
        
        if rows_grupos:
            df_grupos = pd.DataFrame(rows_grupos)
    
    # Ordenar columnas del DataFrame principal
    if not df_principal.empty:
        meta_cols = ["ID_Respuesta", "Nombre Formulario", "Usuario", "Status", "Llenado", "Actualizado"]
        other_cols = [c for c in df_principal.columns if c not in meta_cols]
        df_principal = df_principal[meta_cols + other_cols]
    
    return df_principal, df_grupos


def _cleanup_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Quita tz y formatea columnas datetime."""
    if df is None or df.empty:
        return df
    for col in df.columns:
        s = df[col]
        try:
            if hasattr(s, "dt") and getattr(s.dt, "tz", None) is not None:
                df[col] = s.dt.tz_localize(None)
        except Exception:
            pass
    for c in ("filled_at_local", "created_at", "updated_at", "Llenado", "Actualizado"):
        if c in df.columns:
            try:
                df[c] = pd.to_datetime(df[c]).dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
    return df


def excel_bytes_para_un_form(form_id) -> tuple[str, bytes]:
    """
    Crea 1 Excel con hojas:
    - 'Respuestas': Datos principales (sin grupos)
    - 'Grupos': Datos normalizados de grupos (si existen)
    - 'Diccionario': Catálogo de campos
    
    Retorna (filename, bytes).
    """
    qs = FormularioEntry.objects.filter(form_id=form_id).order_by("-created_at")
    if not qs.exists():
        return (f"{form_id}.xlsx", b"")
    
    form_name = (qs.first().form_name or str(form_id)).strip()

    # DataFrames de respuestas y grupos
    df_principal, df_grupos = dataframe_por_form(form_id)
    
    # Limpiar DataFrames
    df_principal = _cleanup_df_for_excel(df_principal.copy()) if not df_principal.empty else df_principal
    df_grupos = _cleanup_df_for_excel(df_grupos.copy()) if not df_grupos.empty else df_grupos

    # Diccionario de datos
    cat = _build_field_catalog(qs.first().form_json or {})
    df_dict = pd.DataFrame(cat) if cat else pd.DataFrame(
        columns=["id_pagina", "id_campo", "nombre_interno", "etiqueta", "clase", "tipo", "requerido", "sequence"]
    )

    # Crear Excel
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        # Hoja principal
        (df_principal if df_principal is not None else pd.DataFrame()).to_excel(
            xw, index=False, sheet_name="Respuestas"
        )
        
        # Hoja de grupos (solo si existen)
        if not df_grupos.empty:
            df_grupos.to_excel(xw, index=False, sheet_name="Grupos")
        
        # Hoja de diccionario
        df_dict.to_excel(xw, index=False, sheet_name="Diccionario")
    
    buf.seek(0)
    safe_name = _sanitize_filename(form_name)
    fname = f"{safe_name}__{form_id}.xlsx"
    return (fname, buf.read())


def content_bytes_para_un_form(form_id, fmt: str = "xlsx"):
    """
    Devuelve (filename, bytes, mimetype) del formulario en formato elegido.
    - xlsx: incluye hojas 'Respuestas', 'Grupos' (si aplica) y 'Diccionario'
    - csv: genera un ZIP con 2 archivos CSV: respuestas.csv y grupos.csv (si aplica)
    - json: genera JSON con estructura anidada {respuestas: [...], grupos: [...]}
    """
    fmt = (fmt or "xlsx").lower()
    qs = FormularioEntry.objects.filter(form_id=form_id).order_by("-created_at")
    if not qs.exists():
        return (f"{form_id}.{fmt}", b"", "application/octet-stream")

    form_name = (qs.first().form_name or str(form_id)).strip()
    safe_name = _sanitize_filename(form_name)

    if fmt == "xlsx":
        fname, content = excel_bytes_para_un_form(form_id)
        return (fname, content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    elif fmt == "csv":
        # CSV: Un solo archivo con secciones separadas para respuestas y grupos
        df_principal, df_grupos = dataframe_por_form(form_id)
        df_principal = _cleanup_df_for_excel(df_principal.copy()) if not df_principal.empty else df_principal
        df_grupos = _cleanup_df_for_excel(df_grupos.copy()) if not df_grupos.empty else df_grupos
        
        # Construir CSV con secciones
        csv_parts = []
        
        # Sección 1: Respuestas
        csv_parts.append("# RESPUESTAS")
        csv_respuestas = df_principal.to_csv(index=False)
        csv_parts.append(csv_respuestas)
        
        # Sección 2: Grupos (si existen)
        if not df_grupos.empty:
            csv_parts.append("\n# GRUPOS")
            csv_grupos = df_grupos.to_csv(index=False)
            csv_parts.append(csv_grupos)
        
        # Unir todo
        csv_completo = "\n".join(csv_parts)
        out = csv_completo.encode("utf-8-sig")
        
        return (f"{safe_name}__{form_id}.csv", out, "text/csv")

    elif fmt == "json":
        # JSON: Estructura con respuestas y grupos anidados
        df_principal, df_grupos = dataframe_por_form(form_id)
        
        # Convertir DataFrames a formato JSON-serializable
        # Reemplazar NaN, NaT y otros valores no serializables
        if not df_principal.empty:
            df_principal = df_principal.fillna('')
            # Convertir fechas y timestamps a strings
            for col in df_principal.columns:
                if pd.api.types.is_datetime64_any_dtype(df_principal[col]):
                    df_principal[col] = df_principal[col].astype(str).replace('NaT', '')
        
        if not df_grupos.empty:
            df_grupos = df_grupos.fillna('')
            # Convertir fechas y timestamps a strings
            for col in df_grupos.columns:
                if pd.api.types.is_datetime64_any_dtype(df_grupos[col]):
                    df_grupos[col] = df_grupos[col].astype(str).replace('NaT', '')
        
        resultado = {
            "formulario": {
                "id": str(form_id),
                "nombre": form_name
            },
            "respuestas": df_principal.to_dict(orient="records") if not df_principal.empty else [],
            "grupos": df_grupos.to_dict(orient="records") if not df_grupos.empty else []
        }
        
        out = json.dumps(resultado, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        return (f"{safe_name}__{form_id}.json", out, "application/json")

    else:
        # fallback: xlsx
        return content_bytes_para_un_form(form_id, "xlsx")


def zip_bytes_todos_los_forms(fmt: str = "xlsx"):
    """
    Genera un ZIP con 1 archivo por form_id en el formato elegido.
    Retorna (filename, bytes)
    """
    ids = (FormularioEntry.objects.values_list("form_id", flat=True).distinct())
    mem = BytesIO()
    with ZipFile(mem, mode="w", compression=ZIP_DEFLATED) as zf:
        for fid in ids:
            fname, content, _mime = content_bytes_para_un_form(fid, fmt)
            if content:
                zf.writestr(fname, content)
    mem.seek(0)
    return (f"formularios_respuestas_{fmt}.zip", mem.read())
    