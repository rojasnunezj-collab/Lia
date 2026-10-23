# ====================================================================
# --- MÓDULO DE COTIZACIONES - EPMI SAC (LÍA) ---
# ====================================================================
import os
import json
import time
import asyncio
import threading
from datetime import datetime, timezone, timedelta
import gspread
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

from config.settings import logger, SHEET_ID, DRIVE_FOLDER_ID
from core.sheets_client import obtener_credenciales

PET = timezone(timedelta(hours=-5))
_COTIZACIONES_LOCK = threading.Lock()
PLANTILLAS_COTIZACION = {
    "COMERCIALIZACION": {
        "id": os.getenv("TEMPLATE_COTIZACION_ID", "1-Umk538IEr1MCYvWHf2Pp7KmEPHNcTpxSvTxbDZk4oQ"),
        "nombre": "Comercialización",
        "asunto_defecto": "Servicio de recolección, transporte y comercialización de residuos solidos",
        "igv_defecto": "Los precios incluyen IGV.",
        "personal_defecto": "El servicio incluye 01 conductor y 02 operarios.",
        "validez_defecto": "Propuesta económica válida por 30 días calendario.",
        "num_tablas": 1
    },
    "FINAL_PETRAMAS": {
        "id": os.getenv("TEMPLATE_COTIZACION_PETRAMAS_ID", "1TJqmMv952814kX0PnFGBn6g6n51MCDpKtvyOPVS10Xg"),
        "nombre": "Disposición Final - Petramas",
        "asunto_defecto": "Servicio de recolección, transporte y disposición final de residuos solidos",
        "igv_defecto": "Los precios no incluyen IGV.",
        "personal_defecto": "El servicio incluye 01 conductor y 01 operarios.",
        "validez_defecto": "Propuesta económica válida por 30 días calendario.",
        "num_tablas": 2
    },
    "FINAL_ARE_YAKU": {
        "id": os.getenv("TEMPLATE_COTIZACION_ARE_YAKU_ID", "1keT6UcYmjd8L5VUdc04qPTCLa6lkWX-mE20618bEyQw"),
        "nombre": "Disposición Final - Are Yaku Pachas",
        "asunto_defecto": "Servicio de recolección, transporte y disposición final de residuos solidos",
        "igv_defecto": "Los precios no incluyen IGV.",
        "personal_defecto": "El servicio incluye 01 conductor y 02 operarios.",
        "validez_defecto": "Propuesta económica válida por 30 días calendario.",
        "num_tablas": 2
    },
    "FINAL_LOS_OLIVOS": {
        "id": os.getenv("TEMPLATE_COTIZACION_LOS_OLIVOS_ID", "1Mca2q0c4ioBvi_3Nq32yNLWjGMJhnkooDuQv1wCC7-Q"),
        "nombre": "Disposición Final - Los Olivos",
        "asunto_defecto": "Servicio de recolección, transporte y disposición final de residuos sólidos peligrosos",
        "igv_defecto": "Los precios no incluyen IGV.",
        "personal_defecto": "El servicio incluye 01 conductor y 02 operarios.",
        "validez_defecto": "Propuesta económica válida por 30 días calendario.",
        "num_tablas": 2
    }
}
TEMPLATE_DOC_ID = PLANTILLAS_COTIZACION["COMERCIALIZACION"]["id"]
DRIVE_FOLDER_COTIZACIONES = os.getenv("DRIVE_FOLDER_COTIZACIONES", "")

MESES_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
}

def obtener_url_webapp(correlativo=None, datos_edicion=None, user_id=None, vista=None):
    """Construye la URL segura HTTPS para abrir la Telegram Mini App."""
    import urllib.parse
    import base64
    from config.settings import WEBAPP_COTIZACION_URL, RENDER_EXTERNAL_URL

    base_url = WEBAPP_COTIZACION_URL
    if not base_url and RENDER_EXTERNAL_URL:
        base_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/cotizaciones"
    if not base_url:
        base_url = "https://lia-w2yb.onrender.com/cotizaciones"

    params = {'v': str(int(time.time()))}
    if correlativo:
        params['corr'] = str(correlativo)
    if user_id:
        params['uid'] = str(user_id)
    if vista:
        params['view'] = str(vista)
    if datos_edicion:
        json_str = json.dumps(datos_edicion, ensure_ascii=False)
        params['data'] = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

    sep = '&' if '?' in base_url else '?'
    return f"{base_url}{sep}{urllib.parse.urlencode(params)}"

def get_fecha_formato_peru(dt=None):
    if dt is None:
        dt = datetime.now(PET)
    mes = MESES_ES.get(dt.month, "")
    return f"{dt.day:02d} de {mes} del {dt.year}"

# ====================================================================
# --- GESTIÓN DE CARPETA DRIVE PARA COTIZACIONES ---
# ====================================================================
def obtener_o_crear_carpeta_cotizaciones(drive_service):
    global DRIVE_FOLDER_COTIZACIONES
    if DRIVE_FOLDER_COTIZACIONES:
        return DRIVE_FOLDER_COTIZACIONES

    parent_folder = DRIVE_FOLDER_ID
    try:
        # Buscar si ya existe la carpeta 'Cotizaciones EPMI'
        q = f"name='Cotizaciones EPMI' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_folder:
            q += f" and '{parent_folder}' in parents"
        res = drive_service.files().list(
            q=q,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = res.get('files', [])
        if files:
            DRIVE_FOLDER_COTIZACIONES = files[0]['id']
            return DRIVE_FOLDER_COTIZACIONES

        # Si no existe, crearla
        meta = {
            'name': 'Cotizaciones EPMI',
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_folder:
            meta['parents'] = [parent_folder]
        folder = drive_service.files().create(body=meta, fields='id', supportsAllDrives=True).execute()
        DRIVE_FOLDER_COTIZACIONES = folder['id']
        logger.info(f"📁 Carpeta 'Cotizaciones EPMI' creada con ID: {DRIVE_FOLDER_COTIZACIONES}")
        return DRIVE_FOLDER_COTIZACIONES
    except Exception as e:
        logger.error(f"Error obteniendo carpeta cotizaciones: {e}")
        return parent_folder

# ====================================================================
# --- GESTIÓN DE PESTAÑA SHEET 'Cotizaciones' ---
# ====================================================================
COTIZACIONES_HEADERS = [
    "Correlativo",
    "Codigo",
    "Fecha Emision",
    "RUC",
    "Cliente",
    "Direccion",
    "Responsable",
    "Cargo",
    "Items Resumen",
    "Link Doc",
    "Link PDF",
    "Estado",
    "Ultima Edicion",
    "Datos_JSON"
]

def obtener_o_crear_sheet_cotizaciones():
    creds = obtener_credenciales()
    gc = gspread.authorize(creds)
    book = gc.open_by_key(SHEET_ID)
    try:
        ws = book.worksheet("Cotizaciones")
    except gspread.exceptions.WorksheetNotFound:
        ws = book.add_worksheet(title="Cotizaciones", rows="1000", cols="20")
        ws.append_row(COTIZACIONES_HEADERS)
        logger.info("📊 Pestaña 'Cotizaciones' creada en Google Sheet.")
    return ws

def obtener_siguiente_correlativo():
    try:
        ws = obtener_o_crear_sheet_cotizaciones()
        vals = ws.col_values(1)  # Columna Correlativo
        numeros = []
        for v in vals[1:]:
            v_str = str(v).strip()
            if v_str.isdigit():
                numeros.append(int(v_str))
        if numeros:
            sig = max(numeros) + 1
        else:
            sig = 80  # Comenzar en 080 según plantilla
        return f"{sig:03d}"
    except Exception as e:
        logger.error(f"Error obteniendo siguiente correlativo: {e}")
        return "080"

_COTIZACIONES_CACHE = None
_COTIZACIONES_CACHE_TIME = 0
COTIZACIONES_CACHE_TTL = 60  # 60 segundos de caché

def invalidar_cache_cotizaciones():
    global _COTIZACIONES_CACHE, _COTIZACIONES_CACHE_TIME
    _COTIZACIONES_CACHE = None
    _COTIZACIONES_CACHE_TIME = 0

_CATALOGO_CACHE = None
_CATALOGO_CACHE_TIME = 0
CATALOGO_CACHE_TTL = 900  # 15 minutos de caché

def obtener_catalogo_clientes(force_refresh=False):
    """Obtiene la lista de clientes registrados en la pestaña CLIENTES y EMPRESAS con caché."""
    global _CATALOGO_CACHE, _CATALOGO_CACHE_TIME
    now = time.time()
    if not force_refresh and _CATALOGO_CACHE is not None and (now - _CATALOGO_CACHE_TIME < CATALOGO_CACHE_TTL):
        return _CATALOGO_CACHE

    try:
        creds = obtener_credenciales()
        gc = gspread.authorize(creds)
        book = gc.open_by_key(SHEET_ID)
        clientes = []

        # 1. Leer pestaña CLIENTES
        try:
            ws = book.worksheet("CLIENTES")
            records = ws.get_all_records()
            for r in records:
                empresa = str(r.get("EMPRESA", "")).strip()
                if not empresa:
                    continue
                ruc = str(r.get("RUC", "")).strip()
                direccion = (
                    str(r.get("DOMICILIO FISCAL", "")).strip() or
                    str(r.get("DIRECCION 1", "")).strip() or
                    str(r.get("DIRECCION CERTIFICADO", "")).strip() or
                    str(r.get("DIRECCION 2", "")).strip()
                )
                clientes.append({
                    "empresa": empresa,
                    "ruc": ruc,
                    "direccion": direccion
                })
        except Exception as e:
            logger.warning(f"No se pudo leer pestaña CLIENTES: {e}")

        # 2. Leer pestaña EMPRESAS (EPMI, INECOVE, etc.)
        try:
            ws_emp = book.worksheet("EMPRESAS")
            for r in ws_emp.get_all_records():
                emp = str(r.get("EMPRESA", "")).strip()
                ruc_emp = str(r.get("RUC", "")).strip()
                if emp and ruc_emp:
                    if not any(c["empresa"].upper() == emp.upper() for c in clientes):
                        clientes.append({
                            "empresa": emp,
                            "ruc": ruc_emp,
                            "direccion": ""
                        })
        except Exception as e:
            logger.debug(f"No se pudo leer pestaña EMPRESAS: {e}")

        if clientes:
            _CATALOGO_CACHE = clientes
            _CATALOGO_CACHE_TIME = now
            return clientes
        return _CATALOGO_CACHE if _CATALOGO_CACHE is not None else []
    except Exception as e:
        logger.error(f"Error cargando catálogo de clientes: {e}")
        return _CATALOGO_CACHE if _CATALOGO_CACHE is not None else []

def buscar_ruc_por_empresa(nombre_empresa: str):
    """
    Busca el RUC de una empresa en el catálogo de CLIENTES y EMPRESAS
    usando coincidencia flexible (match_company_flexible).
    Retorna (ruc, nombre_oficial) o (None, None).
    """
    if not nombre_empresa:
        return None, None
    nombre_clean = str(nombre_empresa).strip()
    if not nombre_clean or nombre_clean.upper() in ["S/D", "NONE", "-", "", "NULL", "DESCONOCIDO"]:
        return None, None

    from utils.helpers import match_company_flexible

    catalogo = obtener_catalogo_clientes()
    best_item = None
    best_score = 0.0

    for c in catalogo:
        empresa_cat = str(c.get("empresa", "")).strip()
        ruc_cat = str(c.get("ruc", "")).strip()
        if not ruc_cat:
            continue
        is_match, score = match_company_flexible(nombre_clean, empresa_cat)
        if is_match and score > best_score:
            best_score = score
            best_item = c

    if best_item and best_score >= 0.70:
        return str(best_item.get("ruc", "")).strip(), str(best_item.get("empresa", "")).strip()

    return None, None

async def async_buscar_ruc_por_empresa(nombre_empresa: str):
    return await asyncio.to_thread(buscar_ruc_por_empresa, nombre_empresa)

# ====================================================================
# --- GENERACIÓN DE DOCUMENTO Y PDF ---
# ====================================================================
def poblar_tabla_en_doc(docs_service, doc_id, target_table_idx, items):
    """Inserta filas en la tabla especificada de un documento Google Docs mapeando encabezados dinámicamente."""
    if not items:
        return
    doc = docs_service.documents().get(documentId=doc_id).execute()
    tables = [el for el in doc.get('body', {}).get('content', []) if 'table' in el]
    if target_table_idx >= len(tables):
        logger.warning(f"Tabla con índice {target_table_idx} no encontrada en documento {doc_id}")
        return

    tbl_element = tables[target_table_idx]
    tbl = tbl_element['table']
    table_start = tbl_element['startIndex']
    num_cols = len(tbl['tableRows'][0]['tableCells'])

    header_texts = []
    for cell in tbl['tableRows'][0]['tableCells']:
        txt = ""
        for cp in cell.get('content', []):
            for cpe in cp.get('paragraph', {}).get('elements', []):
                txt += cpe.get('textRun', {}).get('content', '')
        header_texts.append(txt.strip().upper())

    # 1. Insertar filas vacías debajo del encabezado
    insert_reqs = []
    for _ in range(len(items)):
        insert_reqs.append({
            'insertTableRow': {
                'tableCellLocation': {
                    'tableStartLocation': {'index': table_start},
                    'rowIndex': 0,
                    'columnIndex': 0
                },
                'insertBelow': True
            }
        })
    insert_reqs.append({
        'updateTableCellStyle': {
            'tableRange': {
                'tableCellLocation': {
                    'tableStartLocation': {'index': table_start},
                    'rowIndex': 1,
                    'columnIndex': 0
                },
                'rowSpan': len(items),
                'columnSpan': num_cols
            },
            'tableCellStyle': {},
            'fields': 'backgroundColor'
        }
    })
    docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': insert_reqs}).execute()

    # 2. Re-obtener el documento para conocer los nuevos índices de celdas
    doc_updated = docs_service.documents().get(documentId=doc_id).execute()
    tables_updated = [el for el in doc_updated.get('body', {}).get('content', []) if 'table' in el]
    tbl_u = tables_updated[target_table_idx]['table']

    cell_updates = []
    for r_idx, itm in enumerate(items, start=1):
        if r_idx >= len(tbl_u['tableRows']):
            break
        row = tbl_u['tableRows'][r_idx]
        for c_idx, h in enumerate(header_texts):
            if c_idx >= len(row['tableCells']):
                continue
            val = ""
            if 'ITEM' in h:
                val = str(itm.get("item", r_idx))
            elif 'DESCRIP' in h:
                val = str(itm.get("descripcion", "")).upper()
            elif 'VEHIC' in h:
                val = str(itm.get("vehiculo", "")).upper()
            elif 'CANT' in h:
                val = str(itm.get("cantidad", "")).upper()
            elif 'UNIDAD' in h or 'MEDIDA' in h:
                val = str(itm.get("unidad", itm.get("unidad_medida", "KG"))).upper()
            elif 'PRECIO' in h:
                p = str(itm.get("precio", "")).strip()
                if p and not p.startswith("S/"):
                    p = f"S/. {p}"
                val = p
            cell = row['tableCells'][c_idx]
            cell_index = cell['startIndex'] + 1
            cell_updates.append((cell_index, val))

    cell_updates.sort(key=lambda x: x[0], reverse=True)
    text_reqs = [{'insertText': {'location': {'index': idx}, 'text': text}} for idx, text in cell_updates]
    docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': text_reqs}).execute()

    # 3. Quitar negrita en filas de datos
    try:
        doc_after = docs_service.documents().get(documentId=doc_id).execute()
        t_after = [el for el in doc_after.get('body', {}).get('content', []) if 'table' in el][target_table_idx]['table']
        style_reqs = []
        for r_idx in range(1, len(t_after['tableRows'])):
            row = t_after['tableRows'][r_idx]
            style_reqs.append({
                'updateTextStyle': {
                    'range': {'startIndex': row['startIndex'], 'endIndex': row['endIndex']},
                    'textStyle': {
                        'bold': False,
                        'foregroundColor': {
                            'color': {
                                'rgbColor': {'red': 0.1, 'green': 0.1, 'blue': 0.1}
                            }
                        }
                    },
                    'fields': 'bold,foregroundColor'
                }
            })
        if style_reqs:
            docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': style_reqs}).execute()
    except Exception as e_st:
        logger.warning(f"Aviso reseteando negrita en tabla {target_table_idx}: {e_st}")

def procesar_generacion_cotizacion(datos):
    with _COTIZACIONES_LOCK:
        return _ejecutar_generacion_cotizacion(datos)

def _ejecutar_generacion_cotizacion(datos):
    """
    datos: dict con:
      - correlativo: '081' (o se autocalcula)
      - fecha: '01 de octubre del 2026' (o auto)
      - ruc: '20505688903'
      - cliente: 'AGRICOLA ANDREA S.A.C.'
      - direccion: 'PISCO - ICA'
      - asunto: (opcional, personalizado)
      - personal: (opcional, ej: '01 conductor y 02 operarios')
      - condicion_igv: (opcional, ej: 'Los precios incluyen IGV.')
      - validez: (opcional, ej: '30 días calendario')
      - responsable: 'Alexander Chamochumbi Chávez'
      - cargo: 'Responsable Técnico'
      - tipo_servicio: 'COMERCIALIZACION' o 'DISPOSICION_FINAL'
      - subtipo_final: 'PETRAMAS', 'ARE_YAKU', 'LOS_OLIVOS'
      - template_key: (opcional directo)
      - items: [...] (para Comercialización)
      - items_transporte: [...] (para Disposición Final Tabla 1)
      - items_disposicion: [...] (para Disposición Final Tabla 2)
      - doc_id / pdf_id: (opcional, si es para edición de documento previo)
    """
    creds = obtener_credenciales()
    drive = build('drive', 'v3', credentials=creds)
    docs = build('docs', 'v1', credentials=creds)

    correlativo = str(datos.get("correlativo") or obtener_siguiente_correlativo()).strip()
    if correlativo.isdigit():
        correlativo = f"{int(correlativo):03d}"
    
    anio = str(datetime.now(PET).year)
    codigo_cotizacion = f"{correlativo}-{anio}-EO-RS"

    fecha = datos.get("fecha") or get_fecha_formato_peru()
    cliente = str(datos.get("cliente", "")).strip().upper()
    if not cliente:
        raise ValueError("El cliente o razón social es obligatorio para generar la cotización.")
    ruc = str(datos.get("ruc", "")).strip()
    direccion = str(datos.get("direccion", "")).strip().upper()
    responsable = str(datos.get("responsable") or "Alexander Chamochumbi Chávez").strip()
    cargo = str(datos.get("cargo") or "Responsable Técnico").strip()

    # Resolver plantilla
    template_key = datos.get("template_key")
    tipo_servicio = str(datos.get("tipo_servicio") or datos.get("tipo_cotizacion") or "COMERCIALIZACION").upper()
    subtipo_final = str(datos.get("subtipo_final") or datos.get("subtipo") or "").upper()

    if not template_key:
        if "FINAL" in tipo_servicio or "DISPOSICION" in tipo_servicio:
            tipo_servicio = "DISPOSICION_FINAL"
            if "YAKU" in subtipo_final or "ARE" in subtipo_final:
                subtipo_final = "ARE_YAKU"
                template_key = "FINAL_ARE_YAKU"
            elif "OLIVOS" in subtipo_final:
                subtipo_final = "LOS_OLIVOS"
                template_key = "FINAL_LOS_OLIVOS"
            else:
                subtipo_final = "PETRAMAS"
                template_key = "FINAL_PETRAMAS"
        else:
            tipo_servicio = "COMERCIALIZACION"
            subtipo_final = ""
            template_key = "COMERCIALIZACION"

    cfg_plantilla = PLANTILLAS_COTIZACION.get(template_key, PLANTILLAS_COTIZACION["COMERCIALIZACION"])
    template_doc_id = cfg_plantilla["id"]

    asunto = str(datos.get("asunto") or cfg_plantilla["asunto_defecto"]).strip()
    personal = str(datos.get("personal") or cfg_plantilla["personal_defecto"]).strip()
    condicion_igv = str(datos.get("condicion_igv") or cfg_plantilla["igv_defecto"]).strip()
    validez = str(datos.get("validez") or cfg_plantilla["validez_defecto"]).strip()

    items = datos.get("items", [])
    items_transporte = datos.get("items_transporte", [])
    items_disposicion = datos.get("items_disposicion", [])

    # Normalizar formato de precios en items
    import re
    def _normalizar_precio_item(it_list):
        for it in it_list:
            p_val = str(it.get("precio", "")).strip()
            if p_val:
                m_p = re.search(r"(\d+(?:[.,]\d+)?)", p_val)
                if m_p:
                    num_f = float(m_p.group(1).replace(",", "."))
                    it["precio"] = f"S/. {num_f:.2f}"
                elif not p_val.startswith("S/"):
                    it["precio"] = f"S/. {p_val}"

    _normalizar_precio_item(items)
    _normalizar_precio_item(items_transporte)
    _normalizar_precio_item(items_disposicion)

    # Compatibilidad: si en disposición final enviaron solo 'items'
    if template_key != "COMERCIALIZACION" and not items_transporte and not items_disposicion and items:
        for it in items:
            if it.get("vehiculo"):
                items_transporte.append(it)
            else:
                items_disposicion.append(it)
        if not items_transporte and not items_disposicion:
            items_disposicion = items

    folder_id = obtener_o_crear_carpeta_cotizaciones(drive)

    # Detectar si es una modificación de cotización previa
    coti_existente = buscar_cotizacion_por_correlativo(correlativo)
    doc_id_previo = datos.get("doc_id") or (coti_existente.get("doc_id") if coti_existente else None)
    pdf_id_previo = datos.get("pdf_id") or (coti_existente.get("pdf_id") if coti_existente else None)

    # 1. Crear copia del documento en Google Drive
    nombre_doc = f"Cotización N°{codigo_cotizacion} - {cliente}"
    meta = {
        'name': nombre_doc,
        'parents': [folder_id]
    }
    copy_file = drive.files().copy(fileId=template_doc_id, body=meta, supportsAllDrives=True).execute()
    doc_id = copy_file['id']

    try:
        # 2. Reemplazos de texto y configuración de márgenes para no superponer encabezado
        requests = [
            {'replaceAllText': {'containsText': {'text': '{{COTIZACION}}', 'matchCase': True}, 'replaceText': correlativo}},
            {'replaceAllText': {'containsText': {'text': '{{CLIENTE}}', 'matchCase': True}, 'replaceText': cliente}},
            {'replaceAllText': {'containsText': {'text': '{{RUC}}', 'matchCase': True}, 'replaceText': ruc}},
            {'replaceAllText': {'containsText': {'text': '{{DIRECCION}}', 'matchCase': True}, 'replaceText': direccion}},
            {'replaceAllText': {'containsText': {'text': '{{FECHA}}', 'matchCase': True}, 'replaceText': fecha}},
            {'replaceAllText': {'containsText': {'text': 'Alexander Chamochumbi Chávez', 'matchCase': True}, 'replaceText': responsable}},
            {'replaceAllText': {'containsText': {'text': 'Responsable Técnico', 'matchCase': True}, 'replaceText': cargo}},
        ]

        # Reemplazo de Asunto si fue editado
        if asunto:
            requests.append({
                'replaceAllText': {'containsText': {'text': cfg_plantilla['asunto_defecto'], 'matchCase': False}, 'replaceText': asunto}
            })
            for k, o_cfg in PLANTILLAS_COTIZACION.items():
                if o_cfg['asunto_defecto'] != cfg_plantilla['asunto_defecto']:
                    requests.append({
                        'replaceAllText': {'containsText': {'text': o_cfg['asunto_defecto'], 'matchCase': False}, 'replaceText': asunto}
                    })

        # Reemplazo de Personal / Operarios si fue editado
        if personal and personal != cfg_plantilla['personal_defecto']:
            texto_p = personal if personal.lower().startswith("el servicio incluye") else f"El servicio incluye {personal}."
            requests.append({
                'replaceAllText': {'containsText': {'text': cfg_plantilla['personal_defecto'], 'matchCase': False}, 'replaceText': texto_p}
            })

        # Reemplazo de Condición IGV si fue editada
        if condicion_igv and condicion_igv != cfg_plantilla['igv_defecto']:
            requests.append({
                'replaceAllText': {'containsText': {'text': cfg_plantilla['igv_defecto'], 'matchCase': False}, 'replaceText': condicion_igv}
            })

        # Reemplazo de Validez si fue editada
        if validez and validez != cfg_plantilla['validez_defecto']:
            texto_v = validez if validez.lower().startswith("propuesta") else f"Propuesta económica válida por {validez}."
            requests.append({
                'replaceAllText': {'containsText': {'text': cfg_plantilla['validez_defecto'], 'matchCase': False}, 'replaceText': texto_v}
            })

        # Margen superior
        requests.append({
            'updateDocumentStyle': {
                'documentStyle': {
                    'marginHeader': {'magnitude': 14.17, 'unit': 'PT'},
                    'marginTop': {'magnitude': 105, 'unit': 'PT'}
                },
                'fields': 'marginHeader,marginTop'
            }
        })
        docs.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()

        # 3. Llenado de tablas
        if template_key == "COMERCIALIZACION":
            if items:
                poblar_tabla_en_doc(docs, doc_id, 0, items)
        else:
            if items_transporte:
                poblar_tabla_en_doc(docs, doc_id, 0, items_transporte)
            if items_disposicion:
                poblar_tabla_en_doc(docs, doc_id, 1, items_disposicion)

        # 4. Exportar a PDF
        pdf_bytes = drive.files().export(fileId=doc_id, mimeType='application/pdf').execute()

        # 5. Guardar o Actualizar el archivo PDF en Google Drive (sobreescribiendo si ya existe)
        nombre_pdf = f"Cotización N°{codigo_cotizacion} - {cliente}.pdf"
        media = MediaInMemoryUpload(pdf_bytes, mimetype='application/pdf', resumable=False)
        pdf_id = None
        pdf_link = None

        if pdf_id_previo:
            try:
                pdf_file = drive.files().update(
                    fileId=pdf_id_previo,
                    body={'name': nombre_pdf},
                    media_body=media,
                    fields='id, webViewLink',
                    supportsAllDrives=True
                ).execute()
                pdf_id = pdf_file.get('id')
                pdf_link = pdf_file.get('webViewLink') or f"https://drive.google.com/file/d/{pdf_id}/view"
                logger.info(f"🔄 Archivo PDF {pdf_id} actualizado en Google Drive con título: {nombre_pdf}")
            except Exception as e_up:
                logger.warning(f"No se pudo actualizar PDF previo {pdf_id_previo}: {e_up}. Se creará nuevo.")
                pdf_id = None

        if not pdf_id:
            pdf_file = drive.files().create(
                body={'name': nombre_pdf, 'parents': [folder_id]},
                media_body=media,
                fields='id, webViewLink',
                supportsAllDrives=True
            ).execute()
            pdf_id = pdf_file.get('id')
            pdf_link = pdf_file.get('webViewLink')
            try:
                drive.permissions().create(fileId=pdf_id, body={'type': 'anyone', 'role': 'reader'}, supportsAllDrives=True).execute()
            except Exception as e_p:
                logger.warning(f"No se pudo asignar permiso público a PDF: {e_p}")

        try:
            drive.permissions().create(fileId=doc_id, body={'type': 'anyone', 'role': 'writer'}, supportsAllDrives=True).execute()
        except Exception as e_d:
            logger.warning(f"No se pudo asignar permiso público a Doc: {e_d}")

        doc_link = f"https://docs.google.com/document/d/{doc_id}/edit"

        # 6. Registrar o actualizar en Google Sheets
        if template_key == "COMERCIALIZACION":
            resumen_items = "; ".join([f"{it.get('descripcion')} ({it.get('precio')})" for it in items[:4]])
            if len(items) > 4:
                resumen_items += f" y {len(items)-4} más..."
        else:
            partes = []
            for it in items_transporte[:2]:
                partes.append(f"Transp: {it.get('descripcion')} ({it.get('precio')})")
            for it in items_disposicion[:2]:
                partes.append(f"Disp: {it.get('descripcion')} ({it.get('precio')})")
            resumen_items = "; ".join(partes)
            total_it = len(items_transporte) + len(items_disposicion)
            if total_it > 4:
                resumen_items += f" y {total_it-4} más..."

        datos_completos = {
            "correlativo": correlativo,
            "codigo": codigo_cotizacion,
            "tipo_servicio": tipo_servicio,
            "subtipo_final": subtipo_final,
            "template_key": template_key,
            "fecha": fecha,
            "cliente": cliente,
            "ruc": ruc,
            "direccion": direccion,
            "asunto": asunto,
            "personal": personal,
            "condicion_igv": condicion_igv,
            "validez": validez,
            "responsable": responsable,
            "cargo": cargo,
            "items": items,
            "items_transporte": items_transporte,
            "items_disposicion": items_disposicion,
            "doc_id": doc_id,
            "pdf_id": pdf_id
        }

        timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M:%S")
        ws = obtener_o_crear_sheet_cotizaciones()
        
        # Buscar si ya existe este correlativo (normalizando ceros a la izquierda)
        col_correlativos = ws.col_values(1)
        corr_clean = str(correlativo).strip().lstrip('0') or '0'
        
        row_indices = []
        for idx, val in enumerate(col_correlativos[1:], start=2):
            v_clean = str(val).strip().lstrip('0') or '0'
            if v_clean == corr_clean:
                row_indices.append(idx)

        es_modificacion = len(row_indices) > 0
        corr_cell = f"'{correlativo}" if str(correlativo).isdigit() else str(correlativo)
        
        row_data = [
            corr_cell,
            codigo_cotizacion,
            fecha,
            ruc,
            cliente,
            direccion,
            responsable,
            cargo,
            resumen_items,
            doc_link,
            pdf_link,
            "MODIFICADA" if es_modificacion else "EMITIDA",
            timestamp,
            json.dumps(datos_completos, ensure_ascii=False)
        ]

        if es_modificacion:
            target_row = row_indices[0]
            ws.update(values=[row_data], range_name=f"A{target_row}:N{target_row}", value_input_option='USER_ENTERED')
            logger.info(f"🔄 Cotización N°{codigo_cotizacion} actualizada en fila {target_row}.")
            # Eliminar posibles filas duplicadas previas
            if len(row_indices) > 1:
                for dup_row in sorted(row_indices[1:], reverse=True):
                    try:
                        ws.delete_rows(dup_row)
                        logger.info(f"🗑️ Fila duplicada {dup_row} eliminada en Sheet.")
                    except Exception as e_dup:
                        logger.warning(f"No se pudo eliminar fila duplicada {dup_row}: {e_dup}")
        else:
            ws.append_row(row_data, value_input_option='USER_ENTERED')
            logger.info(f"✅ Cotización N°{codigo_cotizacion} registrada en Google Sheets.")

        # Si había un Doc previo y es diferente al recién creado, eliminar el anterior en Drive
        if doc_id_previo and str(doc_id_previo) != str(doc_id):
            try:
                drive.files().delete(fileId=doc_id_previo, supportsAllDrives=True).execute()
                logger.info(f"🗑️ Documento Google Docs anterior {doc_id_previo} eliminado en Drive para evitar duplicados.")
            except Exception as e_del:
                logger.warning(f"No se pudo eliminar Doc previo {doc_id_previo}: {e_del}")

        invalidar_cache_cotizaciones()
        return {
            "success": True,
            "correlativo": correlativo,
            "codigo": codigo_cotizacion,
            "cliente": cliente,
            "fecha": fecha,
            "tipo_servicio": tipo_servicio,
            "subtipo_final": subtipo_final,
            "tipo_nombre": cfg_plantilla["nombre"],
            "total_items": len(items) if template_key == "COMERCIALIZACION" else (len(items_transporte) + len(items_disposicion)),
            "pdf_bytes": pdf_bytes,
            "pdf_link": pdf_link,
            "doc_link": doc_link,
            "doc_id": doc_id,
            "pdf_id": pdf_id,
            "nombre_archivo": nombre_pdf,
            "datos_json": datos_completos
        }
    except Exception as e:
        logger.error(f"❌ Error durante el procesamiento de la cotización: {e}")
        # Si falló, intentar limpiar la copia de Docs
        try:
            drive.files().delete(fileId=doc_id).execute()
        except Exception:
            pass
        raise e

async def async_generar_cotizacion(datos):
    return await asyncio.to_thread(procesar_generacion_cotizacion, datos)

def buscar_cotizacion_por_correlativo(correlativo):
    """Busca una cotización por su número correlativo para ver datos o permitir edición."""
    import re
    try:
        ws = obtener_o_crear_sheet_cotizaciones()
        records = ws.get_all_records()
        corr_clean = str(correlativo).strip().lstrip('0') or '0'
        # Buscar en orden inverso (de abajo hacia arriba) para obtener siempre la versión más reciente
        for r in reversed(records):
            r_clean = str(r.get("Correlativo", "")).strip().lstrip('0') or '0'
            if r_clean == corr_clean:
                raw_json = r.get("Datos_JSON", "")
                if raw_json:
                    try:
                        parsed = json.loads(raw_json)
                        if isinstance(parsed, dict):
                            return parsed
                    except Exception:
                        pass
                doc_l = str(r.get("Link Doc", ""))
                pdf_l = str(r.get("Link PDF", ""))
                m_doc = re.search(r"/document/d/([a-zA-Z0-9_-]+)", doc_l)
                m_pdf = re.search(r"/(?:file/d/|id=)([a-zA-Z0-9_-]+)", pdf_l)
                return {
                    "correlativo": str(r.get("Correlativo")),
                    "codigo": str(r.get("Codigo")),
                    "fecha": str(r.get("Fecha Emision")),
                    "ruc": str(r.get("RUC")),
                    "cliente": str(r.get("Cliente")),
                    "direccion": str(r.get("Direccion")),
                    "responsable": str(r.get("Responsable")),
                    "cargo": str(r.get("Cargo")),
                    "doc_link": doc_l,
                    "pdf_link": pdf_l,
                    "doc_id": m_doc.group(1) if m_doc else None,
                    "pdf_id": m_pdf.group(1) if m_pdf else None,
                    "items": []
                }
    except Exception as e:
        logger.error(f"Error buscando cotización: {e}")
    return None

async def async_buscar_cotizacion_por_correlativo(correlativo):
    return await asyncio.to_thread(buscar_cotizacion_por_correlativo, correlativo)

def obtener_todas_cotizaciones(limite=None, filtro_empresa=None, force_refresh=False):
    """
    Obtiene las cotizaciones registradas en Google Sheets ordenadas de más reciente a más antigua.
    Soporta filtro por empresa/RUC/código y límite (ej: 5 para Telegram).
    """
    global _COTIZACIONES_CACHE, _COTIZACIONES_CACHE_TIME
    import re
    now = time.time()
    if force_refresh or _COTIZACIONES_CACHE is None or (now - _COTIZACIONES_CACHE_TIME > COTIZACIONES_CACHE_TTL):
        try:
            ws = obtener_o_crear_sheet_cotizaciones()
            records = ws.get_all_records()
            lista = []
            for r in reversed(records):
                corr = str(r.get("Correlativo", "")).strip()
                if not corr:
                    continue
                raw_json = r.get("Datos_JSON", "")
                parsed = None
                if raw_json:
                    try:
                        parsed = json.loads(raw_json)
                    except Exception:
                        pass

                doc_l = str(r.get("Link Doc", "")).strip()
                pdf_l = str(r.get("Link PDF", "")).strip()
                m_doc = re.search(r"/document/d/([a-zA-Z0-9_-]+)", doc_l)
                m_pdf = re.search(r"/(?:file/d/|id=)([a-zA-Z0-9_-]+)", pdf_l)

                item = {
                    "correlativo": corr,
                    "codigo": str(r.get("Codigo", f"{corr}-2026-EO-RS")),
                    "fecha": str(r.get("Fecha Emision", "")),
                    "ruc": str(r.get("RUC", "")),
                    "cliente": str(r.get("Cliente", "")),
                    "direccion": str(r.get("Direccion", "")),
                    "responsable": str(r.get("Responsable", "")),
                    "cargo": str(r.get("Cargo", "")),
                    "items_resumen": str(r.get("Items Resumen", "")),
                    "estado": str(r.get("Estado", "EMITIDA")),
                    "ultima_edicion": str(r.get("Ultima Edicion", "")),
                    "doc_link": doc_l,
                    "pdf_link": pdf_l,
                    "doc_id": m_doc.group(1) if m_doc else (parsed.get("doc_id") if parsed else None),
                    "pdf_id": m_pdf.group(1) if m_pdf else (parsed.get("pdf_id") if parsed else None),
                    "datos_json": parsed
                }
                lista.append(item)
            _COTIZACIONES_CACHE = lista
            _COTIZACIONES_CACHE_TIME = now
        except Exception as e:
            logger.error(f"Error obteniendo todas las cotizaciones: {e}")
            if _COTIZACIONES_CACHE is not None:
                return _COTIZACIONES_CACHE
            return []

    resultado = list(_COTIZACIONES_CACHE)
    if filtro_empresa:
        q = str(filtro_empresa).strip().lower()
        def _match(c):
            cli = str(c.get("cliente", "")).lower()
            ruc = str(c.get("ruc", "")).lower()
            cod = str(c.get("codigo", "")).lower()
            corr = str(c.get("correlativo", "")).lower()
            return q in cli or q in ruc or q in cod or q == corr
        resultado = [c for c in resultado if _match(c)]

    if limite and isinstance(limite, int) and limite > 0:
        resultado = resultado[:limite]

    return resultado

async def async_obtener_todas_cotizaciones(limite=None, filtro_empresa=None, force_refresh=False):
    return await asyncio.to_thread(obtener_todas_cotizaciones, limite, filtro_empresa, force_refresh)


# ====================================================================
# --- SINCRONIZAR PDF DESDE GOOGLE DOCS ---
# ====================================================================
def sincronizar_pdf_desde_doc(correlativo):
    """
    Toma los cambios hechos directamente en Google Docs para una cotización existente,
    re-exporta el PDF y actualiza el archivo PDF existente en Google Drive.
    """
    creds = obtener_credenciales()
    drive = build('drive', 'v3', credentials=creds)

    coti = buscar_cotizacion_por_correlativo(correlativo)
    if not coti:
        raise ValueError(f"No se encontró la cotización N°{correlativo}")

    doc_id = coti.get("doc_id")
    pdf_id = coti.get("pdf_id")
    codigo = coti.get("codigo", f"{correlativo}-{datetime.now(PET).year}-EO-RS")
    cliente = coti.get("cliente", "")
    nombre_pdf = f"Cotización N°{codigo} - {cliente}.pdf"

    if not doc_id:
        raise ValueError("La cotización no tiene registrado el ID del documento Google Docs.")

    # 1. Exportar Doc actual a PDF
    try:
        pdf_bytes = drive.files().export(fileId=doc_id, mimeType='application/pdf').execute()
    except Exception as e_exp:
        if "404" in str(e_exp) or "File not found" in str(e_exp):
            raise ValueError(f"El documento Google Docs de esta cotización no se encuentra en Drive o fue eliminado (ID: {doc_id}).")
        raise e_exp

    media = MediaInMemoryUpload(pdf_bytes, mimetype='application/pdf', resumable=False)

    folder_id = obtener_o_crear_carpeta_cotizaciones(drive)
    if pdf_id:
        try:
            pdf_file = drive.files().update(
                fileId=pdf_id,
                body={'name': nombre_pdf},
                media_body=media,
                fields='id, webViewLink',
                supportsAllDrives=True
            ).execute()
            pdf_link = pdf_file.get('webViewLink') or f"https://drive.google.com/file/d/{pdf_id}/view"
            logger.info(f"🔄 PDF existente {pdf_id} actualizado desde cambios en Doc.")
        except Exception as e:
            logger.warning(f"Error actualizando PDF previo {pdf_id}: {e}. Creando nuevo.")
            pdf_id = None

    if not pdf_id:
        pdf_file = drive.files().create(
            body={'name': nombre_pdf, 'parents': [folder_id]},
            media_body=media,
            fields='id, webViewLink',
            supportsAllDrives=True
        ).execute()
        pdf_id = pdf_file.get('id')
        pdf_link = pdf_file.get('webViewLink')
        try:
            drive.permissions().create(fileId=pdf_id, body={'type': 'anyone', 'role': 'reader'}, supportsAllDrives=True).execute()
        except Exception:
            pass

    # Actualizar estado y fecha en Google Sheets
    try:
        ws = obtener_o_crear_sheet_cotizaciones()
        col_corr = ws.col_values(1)
        corr_clean = str(correlativo).strip().lstrip('0') or '0'
        for idx, val in enumerate(col_corr[1:], start=2):
            v_clean = str(val).strip().lstrip('0') or '0'
            if v_clean == corr_clean:
                ws.update_cell(idx, 11, pdf_link)
                ws.update_cell(idx, 12, "MODIFICADA (DOC SYNC)")
                ws.update_cell(idx, 13, datetime.now(PET).strftime("%d/%m/%Y %H:%M:%S"))
                break
    except Exception as e_sh:
        logger.warning(f"Error actualizando Sheets al sincronizar PDF: {e_sh}")

    invalidar_cache_cotizaciones()
    return {
        "success": True,
        "correlativo": correlativo,
        "codigo": codigo,
        "cliente": cliente,
        "pdf_bytes": pdf_bytes,
        "pdf_link": pdf_link,
        "doc_id": doc_id,
        "pdf_id": pdf_id,
        "nombre_archivo": nombre_pdf
    }

async def async_sincronizar_pdf_desde_doc(correlativo):
    return await asyncio.to_thread(sincronizar_pdf_desde_doc, correlativo)

# ====================================================================
# --- ASISTENTE IA PARA COTIZACIONES (TEXTO O VOZ) ---
# ====================================================================
async def interpretar_cotizacion_ia(texto=None, file_path=None, mime_type=None):
    """
    Interpreta una solicitud de cotización en lenguaje natural (texto o audio)
    usando Gemini y el catálogo de clientes de EPMI.
    """
    import re
    from google.genai import types
    from core.ai_client import generar_con_reintento

    catalogo = obtener_catalogo_clientes()
    catalogo_resumen = "\n".join([f"- {c.get('empresa')} | RUC: {c.get('ruc', '')} | Dir: {c.get('direccion', '')}" for c in catalogo[:40]])

    prompt = f"""Eres Lía, la asistente de operaciones y cotizaciones de la empresa EPMI S.A.C.
Tu tarea es interpretar la solicitud del usuario (dictada por nota de voz o escrita en un mensaje) y extraer los datos requeridos para emitir una COTIZACIÓN TÉCNICO-ECONÓMICA.

CATÁLOGO DE CLIENTES FRECUENTES DE EPMI:
{catalogo_resumen}

INSTRUCCIONES DE EXTRACCIÓN:
1. "cliente": Razón Social de la empresa. Si el usuario menciona un nombre abreviado o informal (ej. "Andrea", "Exalmar", "Beta", "Villacurí", "Larán"), asócialo con la razón social oficial del catálogo. Si es una empresa nueva no registrada en el catálogo, usa el nombre que dijo el usuario en MAYÚSCULAS.
2. "ruc": RUC del cliente (11 dígitos). Si coincide con el catálogo, úsalo. Si el usuario lo dio, úsalo. Si no se conoce, déjalo vacío "".
3. "direccion": Dirección o fundo del cliente. Si coincide con el catálogo, úsala. Si no se conoce, déjala vacía "".
4. "items": Lista de residuos/ítems cotizados. Para cada residuo:
   - "descripcion": Nombre formal del residuo en MAYÚSCULAS (ej: CARTÓN, PLÁSTICO FILM, CHATARRA METÁLICA, PARIHUELAS DE MADERA, GALONERAS, ACEITE USADO, etc.).
   - "unidad": Una de las siguientes unidades válidas: "KG", "TN", "UNID", "M3", "GL". Si no especificó unidad o mencionó kilos, usa "KG". Si mencionó toneladas, "TN". Si mencionó unidades, parihuelas, cilindros o sacos, "UNID". Si metros cúbicos, "M3". Si galones, "GL".
   - "precio": Formato estándar con moneda "S/. 0.00" o "S/.0.00" (ej. "S/.0.10", "S/.450.00", "S/.15.00"). Si el usuario indica un número como 0.2 o 0.20 junto a un residuo (ej. "plástico 0.2"), dicho número corresponde estrictamente al precio unitario ("S/. 0.20") y NUNCA debe incluirse en la descripción del residuo.
5. "observaciones": Notas relevantes o aclaraciones breves si faltó algún dato.

RESPONDE ÚNICAMENTE CON UN OBJETO JSON VÁLIDO CON ESTA ESTRUCTURA EXACTA:
{{
  "cliente": "...",
  "ruc": "...",
  "direccion": "...",
  "items": [
    {{
      "descripcion": "...",
      "unidad": "KG",
      "precio": "S/.0.00"
    }}
  ],
  "observaciones": "..."
}}
"""
    partes = []
    if file_path and os.path.exists(file_path):
        with open(file_path, "rb") as f:
            audio_bytes = f.read()
        part = types.Part.from_bytes(data=audio_bytes, mime_type=mime_type or "audio/ogg")
        partes.append(part)

    texto_final = prompt
    if texto:
        texto_final += f"\n\nMENSAJE DEL USUARIO:\n\"{texto}\""

    try:
        response = await generar_con_reintento(partes, texto_final, None, is_json=True)
        raw_text = response.text.strip()
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```[a-zA-Z]*\n?", "", raw_text)
            raw_text = re.sub(r"\n?```$", "", raw_text)
        data = json.loads(raw_text.strip())

        # Completar RUC y dirección desde catálogo si coincide el cliente
        cli_nombre = str(data.get("cliente", "")).upper().strip()
        if cli_nombre:
            for c in catalogo:
                emp = c.get("empresa", "").upper()
                if emp == cli_nombre or cli_nombre in emp or emp in cli_nombre:
                    if not data.get("ruc"):
                        data["ruc"] = c.get("ruc", "")
                    if not data.get("direccion"):
                        data["direccion"] = c.get("direccion", "")
                    data["cliente"] = c.get("empresa", "")
                    break
        return data
    except Exception as e:
        logger.error(f"Error interpretando cotización con IA: {e}")
        return None
