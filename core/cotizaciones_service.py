# ====================================================================
# --- MÓDULO DE COTIZACIONES - EPMI SAC (LÍA) ---
# ====================================================================
import os
import json
import time
import asyncio
from datetime import datetime, timezone, timedelta
import gspread
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

from config.settings import logger, SHEET_ID, DRIVE_FOLDER_ID
from core.sheets_client import obtener_credenciales

PET = timezone(timedelta(hours=-5))
TEMPLATE_DOC_ID = os.getenv("TEMPLATE_COTIZACION_ID", "1-Umk538IEr1MCYvWHf2Pp7KmEPHNcTpxSvTxbDZk4oQ")
DRIVE_FOLDER_COTIZACIONES = os.getenv("DRIVE_FOLDER_COTIZACIONES", "")

MESES_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
}

def obtener_url_webapp(correlativo=None, datos_edicion=None, user_id=None):
    """Construye la URL segura HTTPS para abrir la Telegram Mini App."""
    import urllib.parse
    import base64
    from config.settings import WEBAPP_COTIZACION_URL, RENDER_EXTERNAL_URL

    base_url = WEBAPP_COTIZACION_URL
    if not base_url and RENDER_EXTERNAL_URL:
        base_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/cotizaciones"
    if not base_url:
        # Fallback para GitHub Pages oficial del repo Lia
        base_url = "https://rojasnunezj-collab.github.io/Lia/webapp/cotizaciones.html"

    params = {'v': str(int(time.time()))}
    if correlativo:
        params['corr'] = str(correlativo)
    if user_id:
        params['uid'] = str(user_id)
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

def obtener_catalogo_clientes():
    """Obtiene la lista de clientes registrados en la pestaña CLIENTES para el autocompletado."""
    try:
        creds = obtener_credenciales()
        gc = gspread.authorize(creds)
        book = gc.open_by_key(SHEET_ID)
        try:
            ws = book.worksheet("CLIENTES")
            records = ws.get_all_records()
            clientes = []
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
            return clientes
        except Exception as e:
            logger.warning(f"No se pudo leer pestaña CLIENTES: {e}")
            return []
    except Exception as e:
        logger.error(f"Error cargando catálogo de clientes: {e}")
        return []

# ====================================================================
# --- GENERACIÓN DE DOCUMENTO Y PDF ---
# ====================================================================
def procesar_generacion_cotizacion(datos):
    """
    datos: dict con:
      - correlativo: '081' (o se autocalcula)
      - fecha: '01 de octubre del 2026' (o auto)
      - ruc: '20505688903'
      - cliente: 'AGRICOLA ANDREA S.A.C.'
      - direccion: 'PISCO - ICA'
      - responsable: 'Alexander Chamochumbi Chávez'
      - cargo: 'Responsable Técnico'
      - items: [
          {"item": "1", "descripcion": "CARTÓN", "unidad": "KG", "precio": "S/.0,10"}, ...
        ]
      - doc_id_existente: (opcional, si es para edición de documento previo)
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
    ruc = str(datos.get("ruc", "")).strip()
    direccion = str(datos.get("direccion", "")).strip().upper()
    responsable = str(datos.get("responsable") or "Alexander Chamochumbi Chávez").strip()
    cargo = str(datos.get("cargo") or "Responsable Técnico").strip()
    items = datos.get("items", [])

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
    copy_file = drive.files().copy(fileId=TEMPLATE_DOC_ID, body=meta, supportsAllDrives=True).execute()
    doc_id = copy_file['id']

    try:
        # 2. Reemplazos de texto
        requests = [
            {'replaceAllText': {'containsText': {'text': '{{COTIZACION}}', 'matchCase': True}, 'replaceText': correlativo}},
            {'replaceAllText': {'containsText': {'text': '{{CLIENTE}}', 'matchCase': True}, 'replaceText': cliente}},
            {'replaceAllText': {'containsText': {'text': '{{RUC}}', 'matchCase': True}, 'replaceText': ruc}},
            {'replaceAllText': {'containsText': {'text': '{{DIRECCION}}', 'matchCase': True}, 'replaceText': direccion}},
            {'replaceAllText': {'containsText': {'text': '{{FECHA}}', 'matchCase': True}, 'replaceText': fecha}},
            {'replaceAllText': {'containsText': {'text': 'Alexander Chamochumbi Chávez', 'matchCase': True}, 'replaceText': responsable}},
            {'replaceAllText': {'containsText': {'text': 'Responsable Técnico', 'matchCase': True}, 'replaceText': cargo}},
        ]
        docs.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()

        # 3. Llenado de tabla si hay ítems
        if items:
            doc = docs.documents().get(documentId=doc_id).execute()
            table_element = None
            for el in doc.get('body', {}).get('content', []):
                if 'table' in el:
                    table_element = el
                    break

            if table_element:
                table_start = table_element['startIndex']
                insert_rows_requests = []
                for _ in range(len(items)):
                    insert_rows_requests.append({
                        'insertTableRow': {
                            'tableCellLocation': {
                                'tableStartLocation': {'index': table_start},
                                'rowIndex': 0,
                                'columnIndex': 0
                            },
                            'insertBelow': True
                        }
                    })
                # Quitar el color verde heredado de los encabezados en las nuevas filas de ítems
                insert_rows_requests.append({
                    'updateTableCellStyle': {
                        'tableRange': {
                            'tableCellLocation': {
                                'tableStartLocation': {'index': table_start},
                                'rowIndex': 1,
                                'columnIndex': 0
                            },
                            'rowSpan': len(items),
                            'columnSpan': 4
                        },
                        'tableCellStyle': {},
                        'fields': 'backgroundColor'
                    }
                })
                docs.documents().batchUpdate(documentId=doc_id, body={'requests': insert_rows_requests}).execute()

                # Re-leer documento para ubicar las posiciones exactas de las nuevas celdas
                doc_updated = docs.documents().get(documentId=doc_id).execute()
                for el in doc_updated.get('body', {}).get('content', []):
                    if 'table' in el:
                        table_element = el
                        break

                table = table_element['table']
                cell_updates = []
                for r_idx, itm in enumerate(items, start=1):
                    if r_idx >= len(table['tableRows']):
                        break
                    row = table['tableRows'][r_idx]
                    valores = [
                        str(itm.get("item", r_idx)),
                        str(itm.get("descripcion", "")).upper(),
                        str(itm.get("unidad", "KG")).upper(),
                        str(itm.get("precio", "S/.0.00"))
                    ]
                    for c_idx, val in enumerate(valores):
                        cell = row['tableCells'][c_idx]
                        cell_index = cell['startIndex'] + 1
                        cell_updates.append((cell_index, val))

                # Ordenar descendente para que la inserción de texto no altere índices precedentes
                cell_updates.sort(key=lambda x: x[0], reverse=True)
                text_requests = [
                    {'insertText': {'location': {'index': idx}, 'text': text}}
                    for idx, text in cell_updates
                ]
                docs.documents().batchUpdate(documentId=doc_id, body={'requests': text_requests}).execute()

                # Quitar formato negrita en los ítems para que solo los encabezados queden en negrita
                try:
                    doc_after_text = docs.documents().get(documentId=doc_id).execute()
                    for el in doc_after_text.get('body', {}).get('content', []):
                        if 'table' in el:
                            t_after = el['table']
                            break
                    style_requests = []
                    for r_idx in range(1, len(t_after['tableRows'])):
                        row = t_after['tableRows'][r_idx]
                        style_requests.append({
                            'updateTextStyle': {
                                'range': {
                                    'startIndex': row['startIndex'],
                                    'endIndex': row['endIndex']
                                },
                                'textStyle': {'bold': False},
                                'fields': 'bold'
                            }
                        })
                    if style_requests:
                        docs.documents().batchUpdate(documentId=doc_id, body={'requests': style_requests}).execute()
                except Exception as e_style:
                    logger.warning(f"No se pudo resetear negrita en ítems: {e_style}")

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
        resumen_items = "; ".join([f"{it.get('descripcion')} ({it.get('precio')})" for it in items[:4]])
        if len(items) > 4:
            resumen_items += f" y {len(items)-4} más..."

        datos_completos = {
            "correlativo": correlativo,
            "codigo": codigo_cotizacion,
            "fecha": fecha,
            "cliente": cliente,
            "ruc": ruc,
            "direccion": direccion,
            "responsable": responsable,
            "cargo": cargo,
            "items": items,
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

        return {
            "success": True,
            "correlativo": correlativo,
            "codigo": codigo_cotizacion,
            "cliente": cliente,
            "fecha": fecha,
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
   - "precio": Formato estándar con moneda "S/. 0.00" o "S/.0.00" (ej. "S/.0.10", "S/.450.00", "S/.15.00").
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
