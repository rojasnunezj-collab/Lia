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

def obtener_url_webapp(correlativo=None, datos_edicion=None):
    """Construye la URL segura HTTPS para abrir la Telegram Mini App."""
    import urllib.parse
    import base64
    from config.settings import WEBAPP_COTIZACION_URL, RENDER_EXTERNAL_URL

    base_url = WEBAPP_COTIZACION_URL
    if not base_url and RENDER_EXTERNAL_URL:
        base_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/cotizaciones"
    if not base_url:
        # Fallback genérico para GitHub Pages o render
        base_url = "https://rojasnunezj.github.io/Bot_lia_guias_notas/webapp/cotizaciones.html"

    params = {}
    if correlativo:
        params['corr'] = str(correlativo)
    if datos_edicion:
        json_str = json.dumps(datos_edicion, ensure_ascii=False)
        params['data'] = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

    if params:
        sep = '&' if '?' in base_url else '?'
        return f"{base_url}{sep}{urllib.parse.urlencode(params)}"
    return base_url

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
        res = drive_service.files().list(q=q, fields="files(id, name)").execute()
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
        folder = drive_service.files().create(body=meta, fields='id').execute()
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

    # 1. Crear copia del documento en Google Drive
    nombre_doc = f"Cotización N°{codigo_cotizacion} - {cliente}"
    meta = {
        'name': nombre_doc,
        'parents': [folder_id]
    }
    copy_file = drive.files().copy(fileId=TEMPLATE_DOC_ID, body=meta).execute()
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

        # 4. Exportar a PDF
        pdf_bytes = drive.files().export(fileId=doc_id, mimeType='application/pdf').execute()

        # 5. Guardar el archivo PDF en Google Drive
        nombre_pdf = f"Cotización N°{codigo_cotizacion} - {cliente}.pdf"
        media = MediaInMemoryUpload(pdf_bytes, mimetype='application/pdf', resumable=False)
        pdf_file = drive.files().create(
            body={'name': nombre_pdf, 'parents': [folder_id]},
            media_body=media,
            fields='id, webViewLink'
        ).execute()
        drive.permissions().create(fileId=pdf_file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
        pdf_link = pdf_file.get('webViewLink')

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
            "pdf_id": pdf_file.get('id')
        }

        timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M:%S")
        ws = obtener_o_crear_sheet_cotizaciones()
        
        # Verificar si ya existe este correlativo para actualizar o insertar
        col_correlativos = ws.col_values(1)
        row_data = [
            correlativo,
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
            "EMITIDA" if str(correlativo) not in col_correlativos else "MODIFICADA",
            timestamp,
            json.dumps(datos_completos, ensure_ascii=False)
        ]

        if str(correlativo) in col_correlativos:
            row_idx = col_correlativos.index(str(correlativo)) + 1
            ws.update(values=[row_data], range_name=f"A{row_idx}")
            logger.info(f"🔄 Cotización N°{codigo_cotizacion} actualizada en fila {row_idx}.")
        else:
            ws.append_row(row_data, value_input_option='USER_ENTERED')
            logger.info(f"✅ Cotización N°{codigo_cotizacion} registrada en Google Sheets.")

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
            "pdf_id": pdf_file.get('id'),
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
    try:
        ws = obtener_o_crear_sheet_cotizaciones()
        records = ws.get_all_records()
        corr_str = str(correlativo).strip()
        for r in records:
            if str(r.get("Correlativo", "")).strip().zfill(3) == corr_str.zfill(3) or str(r.get("Correlativo", "")).strip() == corr_str:
                raw_json = r.get("Datos_JSON", "")
                if raw_json:
                    try:
                        return json.loads(raw_json)
                    except Exception:
                        pass
                return {
                    "correlativo": str(r.get("Correlativo")),
                    "codigo": str(r.get("Codigo")),
                    "fecha": str(r.get("Fecha Emision")),
                    "ruc": str(r.get("RUC")),
                    "cliente": str(r.get("Cliente")),
                    "direccion": str(r.get("Direccion")),
                    "responsable": str(r.get("Responsable")),
                    "cargo": str(r.get("Cargo")),
                    "doc_link": str(r.get("Link Doc")),
                    "pdf_link": str(r.get("Link PDF")),
                    "items": []
                }
    except Exception as e:
        logger.error(f"Error buscando cotización: {e}")
    return None

async def async_buscar_cotizacion_por_correlativo(correlativo):
    return await asyncio.to_thread(buscar_cotizacion_por_correlativo, correlativo)
