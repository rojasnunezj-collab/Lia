# ====================================================================
# --- MÓDULO CORE: SISTEMA DE CERTIFICADOS IA (LÍA) ---
# ====================================================================
import os
import io
import re
import json
import time
import asyncio
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple

from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import qn
from docx.oxml.simpletypes import ST_TwipsMeasure, Twips
from docxtpl import DocxTemplate
from pypdf import PdfReader, PdfWriter
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google import genai
from google.genai import types

from config.settings import logger, SHEET_ID, DRIVE_FOLDER_ID, PROJECT_ID, REGION_ESTABLE, MODEL_NAME, FALLBACK_MODELS
from core.sheets_client import obtener_credenciales

# Zona horaria Perú (UTC-5)
PET = timezone(timedelta(hours=-5))

# ====================================================================
# --- BLOQUE 1: CONSTANTES Y CONFIGURACIÓN DRIVE / SHEETS ---
# ====================================================================
ID_SHEET_CONTROL = SHEET_ID or "14As5bCpZi56V5Nq1DRs0xl6R1LuOXLvRRoV26nI50NU"
ID_SHEET_REPOSITORIO = SHEET_ID or "14As5bCpZi56V5Nq1DRs0xl6R1LuOXLvRRoV26nI50NU"

CARPETA_PLANTILLAS_NORMALES = '1EwbYAbyv2uMsSn0yXZd0vTPuCoPMbzKs'
CARPETA_PLANTILLAS_MODELO = '1_kY1h6PwlhDPl8BjG7u0fbGMT1AWyn3n'

CARPETAS_DESTINO: Dict[str, Dict[str, str]] = {
    "EPMI S.A.C.": {
        "Comercialización": "1NZc-nfGHw5bnkCAv0TdQYW_bPM_UkKC-",
        "Disposición Final 1": "12PMJ1d-CSWo64m7aNQRQj2yGHFdp9B9S",
        "Disposición Final 2": "12PMJ1d-CSWo64m7aNQRQj2yGHFdp9B9S"
    },
    "INECOVE S.A.C.": {
        "Comercialización": "1NZc-nfGHw5bnkCAv0TdQYW_bPM_UkKC-",
        "Disposición Final 1": "12PMJ1d-CSWo64m7aNQRQj2yGHFdp9B9S",
        "Disposición Final 2": "12PMJ1d-CSWo64m7aNQRQj2yGHFdp9B9S"
    }
}

PLANTILLAS_FALLBACK: Dict[str, Dict[str, str]] = {
    "EPMI S.A.C.": {
        "Comercialización": "1d09vmlBlW_4yjrrz5M1XM8WpCvzTI4f11pERDbxFvNE",
        "Disposición Final 1": "1QqqVJ2vCiAjiKKGt_zEpaImUB-q3aRurSiXjMEU--eg",
        "Disposición Final 2": "1fpdZef3Fe3tl00yAuM0Cehx2_o3AusrErcOJisBtdBM"
    },
    "INECOVE S.A.C.": {
        "Comercialización": os.getenv("TEMPLATE_INECOVE_ID", "1MPzCwxR538osP3_br4VrTDybplqpTBtB08Jo"),
        "Disposición Final 1": os.getenv("TEMPLATE_INECOVE_PELIGROSO_ID", "1W-HyVSivqug13gBRBclBuICAOSBUHm1WN5cnqtMQcZY"),
        "Disposición Final 2": os.getenv("TEMPLATE_INECOVE_PELIGROSO_ID", "1W-HyVSivqug13gBRBclBuICAOSBUHm1WN5cnqtMQcZY")
    }
}

MESES_ES = {
    "01": "Enero", "02": "Febrero", "03": "Marzo", "04": "Abril",
    "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
    "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
}

# ====================================================================
# --- BLOQUE 2: PARCHE TÉCNICO DOCX (TWIPS MEASURE) ---
# ====================================================================
try:
    _orig_convert_from_xml = ST_TwipsMeasure.convert_from_xml

    @classmethod
    def _patch_convert_from_xml(cls, str_value):
        try:
            return Twips(int(str_value))
        except ValueError:
            try:
                return Twips(int(float(str_value)))
            except Exception:
                return _orig_convert_from_xml(str_value)

    ST_TwipsMeasure.convert_from_xml = _patch_convert_from_xml
except Exception as _e:
    logger.debug(f"Aviso parche TwipsMeasure: {_e}")

# ====================================================================
# --- BLOQUE 3: CONEXIÓN A SERVICIOS GOOGLE WORKSPACE ---
# ====================================================================
def obtener_servicios_google():
    """Retorna clientes autenticados de Drive y Sheets."""
    creds = obtener_credenciales()
    if not creds:
        raise ValueError("No se pudieron obtener credenciales válidas de Google.")
    drive = build('drive', 'v3', credentials=creds)
    sheets = build('sheets', 'v4', credentials=creds)
    return drive, sheets

# ====================================================================
# --- BLOQUE 4: UTILIDADES DE FORMATEO Y CADENAS ---
# ====================================================================
def limpiar_monto(valor: Any) -> float:
    """Convierte string numérico a float limpiando comas y espacios."""
    if not valor:
        return 0.0
    s = str(valor).strip().replace(',', '.')
    if s.count('.') > 1:
        parts = s.split('.')
        s = "".join(parts[:-1]) + '.' + parts[-1]
    s = re.sub(r'[^\d.]', '', s)
    try:
        return float(s)
    except Exception:
        return 0.0

def formato_inteligente(valor: Any) -> str:
    """Formatea números quitando decimales superfluos (100.0 -> 100)."""
    try:
        f = float(valor)
        return str(int(f)) if f.is_integer() else f"{f}"
    except Exception:
        return str(valor)

def normalizar_fecha(fecha_str: str) -> str:
    """Normaliza formatos de fecha a dd/mm/yyyy."""
    if not fecha_str:
        return datetime.now(PET).strftime("%d/%m/%Y")
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
        try:
            return datetime.strptime(fecha_str.strip(), fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    return str(fecha_str).strip()

def normalizar_texto_sin_tildes(texto: Any) -> str:
    """Elimina tildes y diacríticos para comparaciones robustas."""
    t = str(texto or '').lower()
    return ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')

def limpiar_descripcion(texto: Any) -> str:
    """Limpia prefijos de residuos como 'VEN - AMB -'."""
    if not texto:
        return ""
    return re.sub(r'VEN\s*-\s*AMB\s*-\s*', '', str(texto).strip(), flags=re.IGNORECASE).strip().upper()

def es_formato_guia(valor: Any) -> bool:
    """Determina si un valor tiene formato característico de Guía de Remisión."""
    if not valor:
        return False
    s = str(valor).strip().upper()
    if s in ['', 'NONE', 'NAN', 'S/N', 'SIN DATOS']:
        return False
    if '-' in s:
        partes = s.split('-', 1)
        serie = partes[0].strip()
        correlativo = partes[1].strip()
        if re.match(r'^(T\d{2,3}|EG\d{2}|E\d{3}|V\d{3}|GR\d{2}|[A-Z]\d{3})$', serie) and correlativo.isdigit():
            return True
        if re.match(r'^\d{3,4}$', serie) and correlativo.isdigit():
            return True
        if re.match(r'^[A-Z0-9]{3,4}$', serie) and correlativo.isdigit() and len(correlativo) >= 4:
            return True
    return False

def es_formato_placa(valor: Any) -> bool:
    """Determina si un valor corresponde al patrón de una placa vehicular peruana."""
    if not valor:
        return False
    s = str(valor).strip().upper()
    if s in ['', 'NONE', 'NAN', 'S/N', 'SIN DATOS']:
        return False
    s_limpio = s.replace('-', '').replace(' ', '')
    if len(s_limpio) not in [5, 6]:
        return False
    if re.match(r'^(T\d{3}|EG\d{2}|E\d{3}|V\d{3}|GR\d{2})$', s_limpio[:4]):
        return False
    if re.match(r'^[A-Z0-9]{3}[A-Z0-9]{3}$', s_limpio):
        tiene_letras = bool(re.search(r'[A-Z]', s_limpio))
        tiene_numeros = bool(re.search(r'\d', s_limpio))
        if tiene_letras and tiene_numeros:
            return True
    if re.match(r'^[A-Z]{2}\d{4}$', s_limpio):
        return True
    return False

def extraer_id_drive(valor: Any) -> Optional[str]:
    """Extrae de forma robusta el fileId de URLs de Google Drive o Docs."""
    if not valor:
        return None
    val_str = str(valor).strip()
    if re.match(r'^[a-zA-Z0-9_-]{25,65}$', val_str):
        return val_str
    patrones = [
        r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
        r'docs\.google\.com/document/d/([a-zA-Z0-9_-]+)',
        r'drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)'
    ]
    for p in patrones:
        m = re.search(p, val_str)
        if m:
            return m.group(1)
    return None

def obtener_link_archivo_drive(servicio_drive, nombre_o_id: str) -> Optional[str]:
    """Obtiene webViewLink público/corporativo para un archivo por ID o nombre."""
    if not servicio_drive or not nombre_o_id:
        return None
    try:
        f_id = extraer_id_drive(nombre_o_id)
        if f_id:
            meta = servicio_drive.files().get(
                fileId=f_id, fields='webViewLink', supportsAllDrives=True
            ).execute()
            return meta.get('webViewLink')
        query = f"name contains '{nombre_o_id}' and trashed = false"
        res = servicio_drive.files().list(
            q=query, spaces='drive', fields='files(id, webViewLink)', supportsAllDrives=True
        ).execute()
        files = res.get('files', [])
        return files[0].get('webViewLink') if files else None
    except Exception as e:
        logger.error(f"Error resolviendo link en Drive para '{nombre_o_id}': {e}")
        return None

def descargar_archivo_drive_por_id_o_nombre(nombre_o_id: str, drive_service=None) -> Optional[bytes]:
    """
    Descarga el contenido binario de un archivo de Google Drive
    identificado por ID, URL o nombre de archivo.
    """
    if not drive_service:
        drive_service, _ = obtener_servicios_google()
    if not nombre_o_id:
        return None

    archivo_id = extraer_id_drive(nombre_o_id)
    es_posible_id = bool(archivo_id) or bool(re.match(r'^[a-zA-Z0-9_-]{25,65}$', str(nombre_o_id).strip()))
    file_id_final = archivo_id or (str(nombre_o_id).strip() if es_posible_id else None)

    try:
        if file_id_final:
            meta = drive_service.files().get(
                fileId=file_id_final, fields='id, name, mimeType', supportsAllDrives=True
            ).execute()
            mime_type = meta.get('mimeType', '')
            if mime_type == 'application/vnd.google-apps.document':
                req = drive_service.files().export_media(fileId=file_id_final, mimeType='application/pdf')
            else:
                req = drive_service.files().get_media(fileId=file_id_final, supportsAllDrives=True)
            fh = io.BytesIO()
            dl = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
            return fh.getvalue()
        else:
            nombre_limpio = str(nombre_o_id).strip()
            q_term = nombre_limpio.replace("'", "\\'")
            query = f"name contains '{q_term}' and trashed = false"
            res = drive_service.files().list(
                q=query,
                spaces='drive',
                corpora='allDrives',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields='files(id, name, mimeType)'
            ).execute()
            files = res.get('files', [])

            if not files and '.' in nombre_limpio:
                sin_ext = nombre_limpio.rsplit('.', 1)[0].replace("'", "\\'")
                query2 = f"name contains '{sin_ext}' and trashed = false"
                res2 = drive_service.files().list(
                    q=query2,
                    spaces='drive',
                    corpora='allDrives',
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields='files(id, name, mimeType)'
                ).execute()
                files = res2.get('files', [])

            if files:
                f_id = files[0]['id']
                m_type = files[0].get('mimeType', '')
                if m_type == 'application/vnd.google-apps.document':
                    req = drive_service.files().export_media(fileId=f_id, mimeType='application/pdf')
                else:
                    req = drive_service.files().get_media(fileId=f_id, supportsAllDrives=True)
                fh = io.BytesIO()
                dl = MediaIoBaseDownload(fh, req)
                done = False
                while not done:
                    _, done = dl.next_chunk()
                return fh.getvalue()
            else:
                logger.warning(f"No se encontró archivo en Drive con nombre '{nombre_o_id}'")
                return None
    except Exception as e:
        logger.error(f"Error descargando archivo '{nombre_o_id}' desde Drive: {e}")
        return None

# ====================================================================
# --- BLOQUE 5: MANIPULACIÓN DOCUMENTAL (WORD & PDF) ---
# ====================================================================
def _set_table_borders(table):
    tblPr = table._tbl.tblPr
    borders = OxmlElement('w:tblBorders')
    for border_name in ['top', 'left', 'bottom', 'right', 'insideH', 'insideV']:
        b = OxmlElement(f'w:{border_name}')
        b.set(qn('w:val'), 'single')
        b.set(qn('w:sz'), '4')
        b.set(qn('w:space'), '0')
        b.set(qn('w:color'), '000000')
        borders.append(b)
    tblPr.append(borders)

def _set_cell_background(cell, color_hex: str):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = parse_xml(f'<w:shd xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" w:fill="{color_hex}"/>')
    tcPr.append(shd)

def _set_table_margins(table, top=72, bottom=72, left=30, right=30):
    tblPr = table._tbl.tblPr
    tblCellMar = parse_xml(f'''
    <w:tblCellMar xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
        <w:top w:w="{top}" w:type="dxa"/>
        <w:left w:w="{left}" w:type="dxa"/>
        <w:bottom w:w="{bottom}" w:type="dxa"/>
        <w:right w:w="{right}" w:type="dxa"/>
    </w:tblCellMar>
    ''')
    tblPr.append(tblCellMar)

def inyectar_tabla_en_docx(doc_io: io.BytesIO, data_items: List[Dict[str, Any]]) -> bytes:
    """
    Inyecta la tabla formateada en el marcador [[TABLA_NOTAS]] del documento Word.
    """
    doc = Document(doc_io)
    target_paragraph = None
    for p in doc.paragraphs:
        if '[[TABLA_NOTAS]]' in p.text:
            target_paragraph = p
            break

    if target_paragraph:
        target_paragraph.text = target_paragraph.text.replace('[[TABLA_NOTAS]]', '')
        for p in doc.paragraphs[:5]:
            if "CERTIFICADO" in p.text.upper():
                p.paragraph_format.space_after = Pt(0)

        table = doc.add_table(rows=1, cols=7)
        try:
            table.style = 'Table Grid'
        except Exception:
            _set_table_borders(table)

        table.autofit = False
        table.allow_autofit = False
        _set_table_margins(table, top=72, bottom=72, left=30, right=30)

        widths = [Inches(0.75), Inches(0.75), Inches(0.75), Inches(3.0), Inches(0.75), Inches(0.75), Inches(0.75)]
        for i, col in enumerate(table.columns):
            col.width = widths[i]

        encabezados = ['Fecha', 'Placa', 'N° Guía', 'Descripción', 'Cantidad', 'Medida', 'Peso']
        hdr_cells = table.rows[0].cells
        for i, nombre in enumerate(encabezados):
            cell = hdr_cells[i]
            cell.text = nombre
            cell.width = widths[i]
            cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            _set_cell_background(cell, "70ad47")

            for p in cell.paragraphs:
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                p.paragraph_format.space_before = Pt(0)
                p.paragraph_format.space_after = Pt(0)
                p.paragraph_format.line_spacing = 1
                run = p.runs[0] if p.runs else p.add_run(nombre)
                run.font.bold = True
                run.font.color.rgb = RGBColor(0, 0, 0)
                run.font.name = 'Calibri'
                run.font.size = Pt(9)

        for item in data_items:
            row_cells = table.add_row().cells
            vals = [
                str(item.get('fecha_origen', '')),
                str(item.get('placa_origen', '')),
                str(item.get('guia_origen', '')),
                str(item.get('desc', '')),
                str(item.get('cant', '')),
                str(item.get('um', '')).upper(),
                str(item.get('peso', ''))
            ]
            for idx, valor in enumerate(vals):
                cell = row_cells[idx]
                cell.text = valor
                cell.width = widths[idx]
                cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
                for p in cell.paragraphs:
                    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    p.paragraph_format.space_before = Pt(0)
                    p.paragraph_format.space_after = Pt(0)
                    p.paragraph_format.line_spacing = 1
                    run = p.runs[0] if p.runs else p.add_run(valor)
                    run.font.name = 'Calibri'
                    run.font.size = Pt(9)

        tbl, p_elem = table._tbl, target_paragraph._p
        p_elem.addnext(tbl)

    new_buffer = io.BytesIO()
    doc.save(new_buffer)
    return new_buffer.getvalue()

def convertir_docx_a_pdf(docx_bytes: bytes, drive_service=None) -> bytes:
    """
    Convierte un documento .docx a PDF usando la API de Google Drive
    (sin requerir Microsoft Word ni LibreOffice en el host).
    """
    if not drive_service:
        drive_service, _ = obtener_servicios_google()

    try:
        media = MediaIoBaseUpload(
            io.BytesIO(docx_bytes),
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            resumable=False
        )
        temp_file = drive_service.files().create(
            body={'name': f"temp_conv_{int(time.time())}", 'mimeType': 'application/vnd.google-apps.document'},
            media_body=media,
            fields='id'
        ).execute()
        temp_id = temp_file.get('id')

        req = drive_service.files().export_media(fileId=temp_id, mimeType='application/pdf')
        fh = io.BytesIO()
        dl = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = dl.next_chunk()

        # Limpiar archivo temporal en Drive
        try:
            drive_service.files().delete(fileId=temp_id).execute()
        except Exception:
            pass

        return fh.getvalue()
    except Exception as e:
        logger.error(f"Error convirtiendo DOCX a PDF vía Drive API: {e}")
        raise RuntimeError(f"Falla convirtiendo Word a PDF: {e}")

def unir_tres_documentos_pdf(cert_pdf_bytes: bytes, remision_bytes: Optional[bytes] = None, transporte_bytes: Optional[bytes] = None) -> bytes:
    """
    Une en orden estricto los documentos:
    1. Certificado (Página inicial)
    2. Guía de Remisión (Páginas posteriores)
    3. Guía de Transporte (Opcional)
    """
    writer = PdfWriter()
    reader_cert = PdfReader(io.BytesIO(cert_pdf_bytes))
    for page in reader_cert.pages:
        writer.add_page(page)

    if remision_bytes:
        try:
            reader_rem = PdfReader(io.BytesIO(remision_bytes))
            for page in reader_rem.pages:
                writer.add_page(page)
        except Exception as e:
            logger.warning(f"No se pudo unir remisión como PDF: {e}")

    if transporte_bytes:
        try:
            reader_trans = PdfReader(io.BytesIO(transporte_bytes))
            for page in reader_trans.pages:
                writer.add_page(page)
        except Exception as e:
            logger.warning(f"No se pudo unir transporte como PDF: {e}")

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    return output.getvalue()

def sustituir_certificado_en_pdf(pdf_unificado_bytes: bytes, nuevo_cert_pdf_bytes: bytes, num_paginas_reemplazar: int = 1) -> bytes:
    """
    Sustituye quirúrgicamente la(s) primera(s) página(s) de un PDF consolidado
    conservando intactas todas las páginas de guías originales posteriores.
    """
    reader_unido = PdfReader(io.BytesIO(pdf_unificado_bytes))
    reader_nuevo = PdfReader(io.BytesIO(nuevo_cert_pdf_bytes))
    writer = PdfWriter()

    # 1. Agregar páginas del nuevo certificado
    for p in reader_nuevo.pages:
        writer.add_page(p)

    # 2. Agregar las páginas restantes del PDF unificado original (las guías)
    total_unido = len(reader_unido.pages)
    paginas_a_saltar = min(num_paginas_reemplazar, total_unido)
    for p in reader_unido.pages[paginas_a_saltar:]:
        writer.add_page(p)

    out_io = io.BytesIO()
    writer.write(out_io)
    out_io.seek(0)
    return out_io.getvalue()

# ====================================================================
# --- BLOQUE 6: MOTOR COGNITIVO IA (VERTEX AI / GEMINI OCR) ---
# ====================================================================
PROMPT_EXTRACCION_CERTIFICADO = """
INSTRUCCIÓN DE SISTEMA: Eres un extractor de datos OCR estricto. Tu única tarea es extraer datos del PDF adjunto y devolverlos ÚNICAMENTE en formato JSON válido. Tienes PROHIBIDO inventar datos, alucinar información o incluir texto fuera del JSON (como ```json o explicaciones).

ESTRUCTURA JSON EXACTA Y REGLAS DE NEGOCIO OBLIGATORIAS:
{
    "cliente": "Razón Social exacta del REMITENTE. Regla Estricta: NO extraer la empresa de transportes, NO extraer nombres de conductores.",
    "ruc_cliente": "Número de RUC del Remitente o Cliente Emisor.",
    "fecha": "dd/mm/yyyy", 
    "serie": "Serie-Numero completo de la guía. Ejemplo: T001-000000", 
    "vehiculo": "PLACA del vehículo. Busca en todo el documento. Obligatorio.", 
    
    "punto_partida": "REGLA DE ORO OBLIGATORIA: Lee primero el bloque 'Observaciones' u 'Observación' de la guía. Todo dato como 'Fundo Casuarinas', 'Planta...', u otro predio que aparezca ahí TIENE QUE SER EXTRAÍDO SÍ O SÍ. Concatena la dirección base de partida con ese dato usando un guion. Ejemplo de Salida Exacta: 'Direccion Base - Fundo Casuarinas' o 'Av Sur - PLANTA EMPACADORA'. Si dice textualmente 'Fundo Casuarinas', debe salir 'Fundo Casuarinas'. NUNCA dejes fuera la información de 'Observaciones'. Si este campo está vacío entonces devuelve solo la dirección de partida base. NUNCA deduzcas ni inventes basándote en la empresa.", 
    
    "punto_llegada": "Dirección Completa exacta de Llegada. IMPORTANTE: Si en el documento (especialmente para la empresa Los Olivos de Villacuri) el destino o planta se indica simplemente como 'EMPACADORA', debes extraer la palabra 'EMPACADORA' y asignarla obligatoriamente a este campo. No lo dejes vacío.", 
    "destinatario": "Razón Social Completa del Destinatario", 
    
    "items": [
        {
            "desc": "Descripción literal del bien", 
            "cant": "Número", 
            "um": "Unidad de medida (KG, UNID, GLN)", 
            "peso": "Peso numérico explícito (o 0.00 si no existe)"
        }
    ]
}
"""

def procesar_guia_ia_vertex(pdf_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Procesamiento OCR síncrono con Gemini vía Vertex AI / Google GenAI SDK."""
    creds = obtener_credenciales()
    regiones = [REGION_ESTABLE or "us-central1", "us-west1", "us-east4", "southamerica-east1"]
    modelos = [MODEL_NAME or "gemini-2.5-flash"] + FALLBACK_MODELS

    pdf_part = types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")
    errores = []

    for region in regiones:
        try:
            client = genai.Client(vertexai=True, project=PROJECT_ID, location=region, credentials=creds)
            for m_name in modelos:
                try:
                    response = client.models.generate_content(
                        model=m_name,
                        contents=[pdf_part, PROMPT_EXTRACCION_CERTIFICADO],
                        config=types.GenerateContentConfig(response_mime_type="application/json")
                    )
                    datos = json.loads(response.text)
                    if datos.get("destinatario") or len(datos.get("vehiculo", "")) >= 3 or datos.get("cliente"):
                        return datos
                except Exception as e:
                    if "404" not in str(e):
                        errores.append(f"{region}/{m_name}: {e}")
                    continue
        except Exception as e:
            errores.append(f"Init {region}: {e}")
            continue

    logger.error(f"Falla OCR de Vertex AI en todas las regiones: {errores}")
    return None

async def async_procesar_guia_ia_vertex(pdf_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Envoltorio asíncrono para no bloquear el bucle de eventos del Bot."""
    return await asyncio.to_thread(procesar_guia_ia_vertex, pdf_bytes)

# ====================================================================
# --- BLOQUE 7: LECTURA DE CATÁLOGOS Y CORRELATIVOS ---
# ====================================================================
def obtener_catalogo_empresas(sheets_service=None) -> Dict[str, Dict[str, str]]:
    """Lee la pestaña 'EMPRESAS' (A=Nombre, B=RUC, C=Registro)."""
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL, range="EMPRESAS!A2:C"
        ).execute()
        filas = res.get('values', [])
        catalogo = {}
        for fila in filas:
            if len(fila) >= 2:
                nombre = str(fila[0]).strip().upper()
                ruc = str(fila[1]).strip()
                reg = str(fila[2]).strip() if len(fila) >= 3 else "Pendiente"
                if nombre:
                    catalogo[nombre] = {"ruc": ruc, "reg": reg}
        return catalogo
    except Exception as e:
        logger.error(f"Error leyendo catálogo EMPRESAS: {e}")
        return {
            "EPMI S.A.C.": {"ruc": "20601369792", "reg": "EO-RS-00043-2021-MINAM"},
            "INECOVE S.A.C.": {"ruc": "20607730101", "reg": "EO-RS-00109-2022-MINAM"}
        }

def obtener_catalogo_servicios(sheets_service=None) -> Dict[str, Dict[str, List[str]]]:
    """Lee y agrupa la pestaña 'SERVICIOS' (Comercialización vs Servicios)."""
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    secciones = {
        "COMERCIALIZACION": {"titulos": [], "servicios": [], "residuos": []},
        "SERVICIOS": {"titulos": [], "servicios": [], "residuos": []}
    }
    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL, range="SERVICIOS!A1:C500"
        ).execute()
        filas = res.get('values', [])
        if not filas:
            return secciones

        seccion_actual = "COMERCIALIZACION"
        KEYWORDS_HEADER = ["COMERCIALIZACION", "COMERCIALIZACIÓN", "SERVICIOS", "DISPOSICION FINAL", "DISPOSICIÓN FINAL"]

        for row in filas:
            c0 = str(row[0]).strip() if len(row) > 0 else ""
            c1 = str(row[1]).strip() if len(row) > 1 else ""
            c2 = str(row[2]).strip() if len(row) > 2 else ""

            c0_norm = normalizar_texto_sin_tildes(c0).upper()
            if any(k in c0_norm for k in KEYWORDS_HEADER):
                seccion_actual = "COMERCIALIZACION" if "COMERC" in c0_norm else "SERVICIOS"
                continue

            if seccion_actual in secciones:
                if c0 and c0 not in secciones[seccion_actual]["titulos"]:
                    secciones[seccion_actual]["titulos"].append(c0)
                if c1 and c1 not in secciones[seccion_actual]["servicios"]:
                    secciones[seccion_actual]["servicios"].append(c1)
                if c2 and c2 not in secciones[seccion_actual]["residuos"]:
                    secciones[seccion_actual]["residuos"].append(c2)
        return secciones
    except Exception as e:
        logger.error(f"Error leyendo pestaña SERVICIOS: {e}")
        return secciones

def obtener_catalogo_clientes_certificados(sheets_service=None) -> Dict[str, Dict[str, str]]:
    """Lee la pestaña 'CLIENTES' (A=Empresa, B=RUC, C=Registro, D=Dirección Certificado, E=Domicilio Fiscal)."""
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL, range="CLIENTES!A2:E"
        ).execute()
        filas = res.get('values', [])
        catalogo = {}
        for fila in filas:
            if len(fila) >= 1:
                empresa = str(fila[0]).strip().upper()
                if not empresa or empresa in ["S/D", "EMPRESA"]:
                    continue
                ruc = str(fila[1]).strip() if len(fila) > 1 else ""
                registro = str(fila[2]).strip() if len(fila) > 2 else ""
                dir_cert = str(fila[3]).strip() if len(fila) > 3 else ""
                dom_fisc = str(fila[4]).strip() if len(fila) > 4 else ""
                catalogo[empresa] = {
                    "empresa": empresa,
                    "ruc": ruc,
                    "registro": registro,
                    "direccion": dir_cert or dom_fisc
                }
        return catalogo
    except Exception as e:
        logger.error(f"Error leyendo catálogo CLIENTES: {e}")
        return {}

def obtener_catalogo_direcciones_certificados(sheets_service=None) -> Dict[str, Dict[str, str]]:
    """Lee la pestaña 'Direcciones' (A=Empresa, B=Fundo/Planta, C=Palabra Clave, D=Dirección)."""
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL, range="Direcciones!A2:D"
        ).execute()
        filas = res.get('values', [])
        catalogo = {}
        for fila in filas:
            if len(fila) >= 4:
                empresa = str(fila[0]).strip().upper()
                fundo = str(fila[1]).strip().upper()
                direccion = str(fila[3]).strip()
                if empresa and fundo and direccion:
                    if empresa not in catalogo:
                        catalogo[empresa] = {}
                    catalogo[empresa][fundo] = direccion
        return catalogo
    except Exception as e:
        logger.error(f"Error leyendo catálogo Direcciones: {e}")
        return {}

def obtener_siguiente_correlativo_cert(tipo_flujo: str = "Comercialización", sheets_service=None) -> str:
    """Calcula el siguiente número correlativo sugerido leyendo 'historial'."""
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL, range="'historial'!D2:E"
        ).execute()
        filas = res.get('values', [])
        max_num = 0
        tipo_filtro = "COM" if "comercializa" in str(tipo_flujo).lower() else "SER"

        for row in filas:
            corr_val = str(row[0]).strip() if len(row) > 0 else ""
            tipo_val = str(row[1]).strip() if len(row) > 1 else ""

            # Si el tipo coincide o no está especificado
            es_com = "comercializa" in tipo_val.lower() or "com" in tipo_val.lower()
            if (tipo_filtro == "COM" and es_com) or (tipo_filtro != "COM" and not es_com):
                nums = re.findall(r'\d+', corr_val)
                if nums:
                    num_int = int(nums[-1])
                    if num_int > max_num:
                        max_num = num_int

        sig = max_num + 1 if max_num > 0 else 1
        return f"{sig:03d}"
    except Exception as e:
        logger.error(f"Error calculando siguiente correlativo: {e}")
        return "001"

# ====================================================================
# --- BLOQUE 8: GESTIÓN DE PLANTILLAS Y SUBIDA A DRIVE ---
# ====================================================================
def resolver_plantilla_drive(empresa_nombre: str, tipo_certificado: str, drive_service, es_modelo: bool = False) -> str:
    """
    Busca la plantilla .docx en la carpeta de Drive o retorna el ID fallback.
    """
    carpeta_id = CARPETA_PLANTILLAS_MODELO if es_modelo else CARPETA_PLANTILLAS_NORMALES
    palabra_flujo = "Comercializacion" if "comercializa" in str(tipo_certificado).lower() else "Final"
    empresa_limpia = "INECOVE" if "INECOVE" in empresa_nombre.upper() else "EPMI"

    try:
        query = (
            f"'{carpeta_id}' in parents "
            f"and name contains '{empresa_limpia}' "
            f"and name contains '{palabra_flujo}' "
            f"and trashed = false"
        )
        res = drive_service.files().list(
            q=query, spaces='drive', fields='files(id, name, mimeType)', supportsAllDrives=True
        ).execute()
        files = res.get('files', [])
        if files:
            return files[0]['id']
    except Exception as e:
        logger.warning(f"Búsqueda dinámica de plantilla falló ({e}). Usando fallback.")

    # Fallback predeterminado
    emp_key = "INECOVE S.A.C." if "INECOVE" in empresa_nombre.upper() else "EPMI S.A.C."
    flujo_key = "Comercialización" if "comercializa" in str(tipo_certificado).lower() else "Disposición Final 1"
    return PLANTILLAS_FALLBACK.get(emp_key, {}).get(flujo_key, "1d09vmlBlW_4yjrrz5M1XM8WpCvzTI4f11pERDbxFvNE")

def descargar_plantilla_docx(plantilla_id: str, drive_service) -> bytes:
    """Descarga los bytes de una plantilla de Drive (Doc o Docx)."""
    meta = drive_service.files().get(fileId=plantilla_id, fields='mimeType', supportsAllDrives=True).execute()
    mime = meta.get('mimeType', '')
    if mime == 'application/vnd.google-apps.document':
        req = drive_service.files().export_media(
            fileId=plantilla_id,
            mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
    else:
        req = drive_service.files().get_media(fileId=plantilla_id, supportsAllDrives=True)

    fh = io.BytesIO()
    dl = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return fh.getvalue()

def subir_a_drive_certificado(contenido_bytes: bytes, nombre_archivo: str, tipo_flujo: str, empresa_firma: str, es_modelo: bool = False, mime_type: str = 'application/pdf') -> Tuple[Optional[str], Optional[str]]:
    """
    Sube el archivo generado a Google Drive en la carpeta de destino correspondiente.
    Retorna (file_id, webViewLink).
    """
    drive_service, _ = obtener_servicios_google()
    if es_modelo:
        carpeta_id = "1LUErbILxjVHnzuHkdWaeAMI4HnLg1c7E"
    else:
        emp_key = "INECOVE S.A.C." if "INECOVE" in empresa_firma.upper() else "EPMI S.A.C."
        flujo_key = tipo_flujo if tipo_flujo in CARPETAS_DESTINO.get(emp_key, {}) else "Comercialización"
        carpeta_id = CARPETAS_DESTINO.get(emp_key, {}).get(flujo_key, "1NZc-nfGHw5bnkCAv0TdQYW_bPM_UkKC-")

    try:
        media = MediaIoBaseUpload(io.BytesIO(contenido_bytes), mimetype=mime_type, resumable=True)
        file_metadata = {
            'name': nombre_archivo,
            'parents': [carpeta_id]
        }
        res = drive_service.files().create(
            body=file_metadata, media_body=media, fields='id, webViewLink', supportsAllDrives=True
        ).execute()
        return res.get('id'), res.get('webViewLink')
    except Exception as e:
        logger.error(f"Error subiendo archivo a Drive: {e}")
        return None, None

def sobrescribir_o_subir_pdf_drive(file_id_existente: Optional[str], contenido_bytes: bytes, nombre_archivo: str, tipo_flujo: str, empresa_firma: str) -> Optional[str]:
    """
    Actualiza in-place el archivo PDF consolidado en Google Drive para preservar su ID y URL pública.
    """
    drive_service, _ = obtener_servicios_google()
    if file_id_existente:
        try:
            media = MediaIoBaseUpload(io.BytesIO(contenido_bytes), mimetype='application/pdf', resumable=True)
            body = {'name': nombre_archivo} if nombre_archivo else {}
            res = drive_service.files().update(
                fileId=file_id_existente,
                body=body,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            link = res.get('webViewLink')
            if link:
                return link
        except Exception as e:
            logger.warning(f"Falla actualización in-place en Drive ({e}). Subiendo nuevo archivo...")

    _, link_nuevo = subir_a_drive_certificado(contenido_bytes, nombre_archivo, tipo_flujo, empresa_firma, mime_type='application/pdf')
    return link_nuevo

def registrar_en_historial_sheets(datos_fila: List[Any], sheets_service=None) -> Optional[int]:
    """Inserta una fila en la pestaña 'historial' (A:J)."""
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    try:
        res = sheets_service.spreadsheets().values().append(
            spreadsheetId=ID_SHEET_CONTROL,
            range="'historial'!A:J",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [datos_fila]}
        ).execute()
        updates = res.get('updates', {})
        updated_range = updates.get('updatedRange', '')
        m = re.search(r'(\d+)', updated_range.split('!')[-1])
        return int(m.group(1)) if m else None
    except Exception as e:
        logger.error(f"Error registrando en 'historial': {e}")
        return None

def registrar_edicion_en_historial(num_fila: int, link_pdf: str, usuario_editor: str = "Usuario", obs_extra: str = "", sheets_service=None) -> bool:
    """Actualiza columnas I y J de 'historial' tras una edición quirúrgica."""
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    if not num_fila or num_fila < 2:
        return False
    try:
        ahora_pe = (datetime.now(PET)).strftime("%d/%m/%Y %H:%M")
        nota_audit = f"Actualizado el {ahora_pe} por {usuario_editor}"
        if obs_extra:
            nota_audit += f" | {obs_extra}"
        body = {"values": [[link_pdf, nota_audit]]}
        sheets_service.spreadsheets().values().update(
            spreadsheetId=ID_SHEET_CONTROL,
            range=f"'historial'!I{num_fila}:J{num_fila}",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        return True
    except Exception as e:
        logger.error(f"Error registrando edición en historial fila {num_fila}: {e}")
        return False

# ====================================================================
# --- BLOQUE 9: BÚSQUEDA DE CERTIFICADOS PARA MODIFICACIÓN ---
# ====================================================================
def buscar_datos_certificado_en_historial(correlativo: str, sheets_service=None, drive_service=None) -> List[Dict[str, Any]]:
    """
    Busca certificados en 'historial' por número correlativo (ej. '045' o '45').
    Resuelve los enlaces de Google Docs y PDF en Drive.
    """
    if not sheets_service or not drive_service:
        drive_service, sheets_service = obtener_servicios_google()

    if not correlativo:
        return []

    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL, range="'historial'!A:J"
        ).execute()
        filas = res.get('values', [])
        if len(filas) < 2:
            return []

        corr_query = str(correlativo).strip().upper()
        corr_num = int(corr_query) if corr_query.isdigit() else None
        resultados = []

        for idx, row in enumerate(filas[1:]):
            num_fila = idx + 2
            corr_row = str(row[3]).strip().upper() if len(row) > 3 else ""

            match = False
            if corr_row and corr_row == corr_query:
                match = True
            elif corr_num is not None and corr_row.isdigit() and int(corr_row) == corr_num:
                match = True
            elif corr_query and corr_query in corr_row:
                match = True

            if match:
                raw_guia = str(row[6]).strip() if len(row) > 6 else ""
                raw_doc = str(row[7]).strip() if len(row) > 7 else ""
                raw_pdf = str(row[8]).strip() if len(row) > 8 else ""

                url_doc = raw_doc
                if raw_doc and not raw_doc.startswith(('http://', 'https://')):
                    link_d = obtener_link_archivo_drive(drive_service, raw_doc)
                    if link_d:
                        url_doc = link_d

                url_pdf = raw_pdf
                if raw_pdf and not raw_pdf.startswith(('http://', 'https://')):
                    link_p = obtener_link_archivo_drive(drive_service, raw_pdf)
                    if link_p:
                        url_pdf = link_p

                resultados.append({
                    "fila": num_fila,
                    "fecha": str(row[0]).strip() if len(row) > 0 else "",
                    "empresa": str(row[1]).strip() if len(row) > 1 else "",
                    "fundo": str(row[2]).strip() if len(row) > 2 else "",
                    "correlativo": corr_row,
                    "tipo_cert": str(row[4]).strip() if len(row) > 4 else "",
                    "guias": str(row[5]).strip() if len(row) > 5 else "",
                    "link_guia": raw_guia,
                    "link_doc": url_doc,
                    "raw_doc": raw_doc,
                    "link_pdf": url_pdf,
                    "raw_pdf": raw_pdf,
                    "observacion": str(row[9]).strip() if len(row) > 9 else ""
                })

        return resultados
    except Exception as e:
        logger.error(f"Error buscando certificado en 'historial': {e}")
        return []

# ====================================================================
# --- BLOQUE 10: GENERACIÓN COMPLETA DE CERTIFICADOS ---
# ====================================================================
def procesar_generacion_certificado(payload: Dict[str, Any], remision_bytes: Optional[bytes] = None, transporte_bytes: Optional[bytes] = None) -> Dict[str, Any]:
    """
    Orquesta la emisión de un certificado completo:
    1. Resuelve plantilla de Drive
    2. Renderiza DocxTemplate con datos de contexto
    3. Inyecta tabla [[TABLA_NOTAS]]
    4. Convierte a PDF vía Drive API
    5. Consolida PDF unificado (Certificado + Guías)
    6. Sube Word y PDF a Drive
    7. Registra fila en 'historial' de Google Sheets
    """
    drive_service, sheets_service = obtener_servicios_google()

    empresa_firma = payload.get("empresa_firma", "EPMI S.A.C.")
    tipo_flujo = payload.get("tipo_flujo", "Comercialización")
    es_modelo = bool(payload.get("es_modelo", False))

    v_corr = str(payload.get("correlativo", "")).strip()
    v_tit = str(payload.get("titulo", "CERTIFICADO DE OPERACIÓN")).strip()
    v_cli = str(payload.get("cliente", "")).strip()
    v_ruc_c = str(payload.get("ruc_cliente", "")).strip()
    v_serv = str(payload.get("servicio", "COMERCIALIZACIÓN")).strip()
    v_res = str(payload.get("tipo_residuo", "RESIDUOS NO PELIGROSOS")).strip()
    v_partida = str(payload.get("punto_partida", "")).strip()
    v_llegada = str(payload.get("punto_llegada", "")).strip()
    v_fec_emis = str(payload.get("fecha_emision", datetime.now(PET).strftime("%d/%m/%Y"))).strip()
    items = payload.get("items", [])

    # Obtener RUC y registro de la empresa firmante
    cat_empresas = obtener_catalogo_empresas(sheets_service)
    emisor_info = cat_empresas.get(empresa_firma.upper(), {})
    emisor_ruc = emisor_info.get("ruc", "20601369792")
    emisor_reg = emisor_info.get("reg", "EO-RS-00043-2021-MINAM")
    emisor_nombre = empresa_firma

    # 1. Descargar plantilla
    plantilla_id = resolver_plantilla_drive(empresa_firma, tipo_flujo, drive_service, es_modelo)
    plantilla_bytes = descargar_plantilla_docx(plantilla_id, drive_service)

    # 2. Renderizar DocxTemplate
    doc = DocxTemplate(io.BytesIO(plantilla_bytes))
    ctx = {
        "CORRELATIVO": v_corr,
        "TITULO": v_tit,
        "REGISTRO": emisor_reg,
        "CLIENTE": v_cli,
        "RUC_CLIENTE": v_ruc_c,
        "RAZON_SOCIAL_CLIENTE": v_cli,
        "SERVICIO_O_COMPRA": v_serv,
        "TIPO_DE_RESIDUO": v_res,
        "PUNTO_PARTIDA": v_partida,
        "DIRECCION_EMPRESA": v_llegada,
        "DIRECCION_LLEGADA": v_llegada,
        "LLEGADA": v_llegada,
        "EMPRESA_2": emisor_nombre,
        "FECHA_EMISION": v_fec_emis,
        "DESTINATARIO_FINAL": emisor_nombre,
        "EMPRESA": emisor_nombre,
        "RUC_EMPRESA": emisor_ruc,
        "RUC": emisor_ruc,
        "EMISOR": emisor_nombre,
        "RUC_EMISOR": emisor_ruc
    }
    doc.render(ctx)
    buf_tpl = io.BytesIO()
    doc.save(buf_tpl)

    # 3. Inyectar tabla de ítems
    docx_final_bytes = inyectar_tabla_en_docx(io.BytesIO(buf_tpl.getvalue()), items)

    # 4. Convertir Certificado a PDF
    cert_pdf_bytes = convertir_docx_a_pdf(docx_final_bytes, drive_service)

    # 5. Unificar con guías si existen
    if remision_bytes or transporte_bytes:
        pdf_unificado_bytes = unir_tres_documentos_pdf(cert_pdf_bytes, remision_bytes, transporte_bytes)
    else:
        pdf_unificado_bytes = cert_pdf_bytes

    # 6. Nombres de archivo y destino
    tipo_cod = "COM" if "comercializa" in str(tipo_flujo).lower() else "SER"
    destino_raw = str(v_partida).split(' - ')[-1].strip()
    destino_raw = re.sub(r'(?i)^(Av\.|Avenida|Calle|Jr\.|Jirón|Pasaje|Carretera|Panamericana)\s+', '', destino_raw).strip()
    destino_limpio = re.sub(r'(?i)^(Planta|Fundo|Sede|Sucursal|Predio)\s+', '', destino_raw).strip().upper()
    if not destino_limpio or destino_limpio == "NAN":
        destino_final = str(v_cli).strip().upper()
    else:
        destino_final = destino_limpio

    if ' - ' not in str(v_partida) and len(destino_final.split()) > 1:
        destino_final = destino_final.split()[0]

    nombre_base = f"CERT-{tipo_cod}-{v_corr}-{destino_final}"
    nombre_docx = f"{nombre_base}.docx"
    nombre_pdf = f"{nombre_base}.pdf"

    # 7. Subir Word a Drive
    file_id_doc, link_doc = subir_a_drive_certificado(
        docx_final_bytes, nombre_docx, tipo_flujo, empresa_firma, es_modelo,
        mime_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    )

    # 8. Subir PDF a Drive
    file_id_pdf, link_pdf = subir_a_drive_certificado(
        pdf_unificado_bytes, nombre_pdf, tipo_flujo, empresa_firma, es_modelo,
        mime_type='application/pdf'
    )

    # 9. Extraer guías consolidadas para la bitácora
    guias_lista = [str(it.get('guia_origen', '')).strip().upper() for it in items if str(it.get('guia_origen', '')).strip()]
    val_guia_completa = ", ".join(sorted(list(set(guias_lista)))) if guias_lista else str(payload.get('guia', '')).strip().upper()

    val_cert = "COMERCIALIZACIÓN" if "comercializa" in str(tipo_flujo).lower() else "FINAL"
    if es_modelo:
        val_cert = f"M-{val_cert[:3]}"

    fecha_registro = datetime.now(PET).strftime("%d/%m/%Y")
    datos_log = [
        fecha_registro,
        str(v_cli).strip().upper(),
        str(destino_final).strip().upper(),
        v_corr,
        val_cert,
        val_guia_completa,
        "",  # Link guía
        link_doc or "",
        link_pdf or "",
        f"Emitido desde Telegram Mini App ({payload.get('usuario_email', 'Bot')})"
    ]

    fila_historial = registrar_en_historial_sheets(datos_log, sheets_service)

    return {
        "status": "success",
        "correlativo": v_corr,
        "nombre_archivo": nombre_pdf,
        "doc_link": link_doc,
        "pdf_link": link_pdf,
        "file_id_pdf": file_id_pdf,
        "file_id_doc": file_id_doc,
        "fila_historial": fila_historial,
        "pdf_bytes": pdf_unificado_bytes,
        "cliente": v_cli,
        "fecha": v_fec_emis,
        "tipo_cod": tipo_cod
    }

# ====================================================================
# --- BLOQUE 11: REGENERACIÓN QUIRÚRGICA DEL EXPEDIENTE ---
# ====================================================================
def procesar_regeneracion_expediente(
    correlativo: str,
    nuevo_doc_bytes: Optional[bytes] = None,
    nuevo_pdf_bytes: Optional[bytes] = None,
    nombre_pdf_usuario: str = "",
    obs_extra: str = "",
    usuario_editor: str = "Usuario MiniApp"
) -> Dict[str, Any]:
    """
    Ejecuta la sustitución quirúrgica de la carátula en el PDF consolidado existente en Drive:
    - Conserva intactas las guías escaneadas anexas.
    - Sobrescribe in-place en Drive conservando el enlace webViewLink público.
    - Actualiza columnas I y J de la pestaña 'historial'.
    """
    drive_service, sheets_service = obtener_servicios_google()

    # 1. Localizar el certificado en Historial
    certificados = buscar_datos_certificado_en_historial(correlativo, sheets_service, drive_service)
    if not certificados:
        raise ValueError(f"No se encontró ningún certificado con el correlativo '{correlativo}' en Historial.")

    cert_sel = certificados[0]
    num_fila = cert_sel['fila']
    link_doc = cert_sel.get('link_doc') or cert_sel.get('raw_doc')
    link_pdf = cert_sel.get('link_pdf') or cert_sel.get('raw_pdf')

    if not link_pdf:
        raise ValueError(f"El certificado #{correlativo} no tiene un enlace a PDF consolidado en Historial.")

    # 2. Obtener bytes del nuevo certificado (Página 1)
    if nuevo_pdf_bytes:
        nuevo_cert_pdf_bytes = nuevo_pdf_bytes
    elif nuevo_doc_bytes:
        nuevo_cert_pdf_bytes = convertir_docx_a_pdf(nuevo_doc_bytes, drive_service)
    else:
        # Descargar Word actualizado directamente desde Google Drive
        doc_id = extraer_id_drive(link_doc)
        if not doc_id:
            raise ValueError(f"No se pudo resolver el ID del documento Word para #{correlativo}.")
        doc_bytes = descargar_plantilla_docx(doc_id, drive_service)
        nuevo_cert_pdf_bytes = convertir_docx_a_pdf(doc_bytes, drive_service)

    # 3. Descargar PDF unificado existente de Drive
    pdf_id = extraer_id_drive(link_pdf)
    if not pdf_id:
        raise ValueError(f"No se pudo resolver el ID de Drive del PDF consolidado para #{correlativo}.")

    req = drive_service.files().get_media(fileId=pdf_id, supportsAllDrives=True)
    fh = io.BytesIO()
    dl = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    pdf_unido_existente_bytes = fh.getvalue()

    # 4. Sustitución quirúrgica de la carátula
    pdf_actualizado_bytes = sustituir_certificado_en_pdf(
        pdf_unificado_existente_bytes, nuevo_cert_pdf_bytes, num_paginas_reemplazar=1
    )

    # 5. Nombre final
    nombre_final = nombre_pdf_usuario.strip() if nombre_pdf_usuario else f"CERT-{correlativo}-ACTUALIZADO.pdf"
    if not nombre_final.lower().endswith('.pdf'):
        nombre_final += '.pdf'

    # 6. Sobrescribir in-place en Drive
    nuevo_link_drive = sobrescribir_o_subir_pdf_drive(
        pdf_id, pdf_actualizado_bytes, nombre_final,
        cert_sel.get('tipo_cert', 'Comercialización'), cert_sel.get('empresa', 'EPMI S.A.C.')
    )

    # 7. Registrar auditoría en Sheets
    link_para_historial = nuevo_link_drive or link_pdf
    registrar_edicion_en_historial(
        num_fila, link_para_historial, usuario_editor=usuario_editor,
        obs_extra=obs_extra, sheets_service=sheets_service
    )

    return {
        "status": "success",
        "correlativo": correlativo,
        "fila": num_fila,
        "pdf_link": link_para_historial,
        "nombre_archivo": nombre_final,
        "pdf_bytes": pdf_actualizado_bytes
    }

# ====================================================================
# --- BLOQUE 12: CONSULTA DE GUÍAS PENDIENTES (REPOSITORIO) ---
# ====================================================================
def obtener_guias_pendientes_repositorio(sheets_service=None) -> List[Dict[str, Any]]:
    """
    Retorna la lista de guías registradas en 'Guias_recibidas' que aún no tienen certificado emitido.
    Cruza contra 'historial' y contra columna H para evitar duplicados.
    Resuelve RUC y Dirección desde los catálogos de CLIENTES y Direcciones.
    """
    if not sheets_service:
        _, sheets_service = obtener_servicios_google()
    try:
        # 1. Cargar guías emitidas en 'historial'
        res_hist = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL, range="'historial'!F2:F"
        ).execute()
        filas_hist = res_hist.get('values', [])
        guias_emitidas_hist = [str(r[0]).strip().upper() for r in filas_hist if r and len(r) > 0 and str(r[0]).strip()]

        # 2. Cargar catálogos para enriquecer cada guía
        cat_clientes = obtener_catalogo_clientes_certificados(sheets_service)
        cat_direcciones = obtener_catalogo_direcciones_certificados(sheets_service)

        # 3. Importar normalizador / matcher
        from bot.handlers import match_guia_en_texto

        # 4. Leer 'Guias_recibidas'
        res_recib = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_REPOSITORIO, range="'Guias_recibidas'!A2:K"
        ).execute()
        filas_recib = res_recib.get('values', [])

        pendientes = []
        for i, fila in enumerate(filas_recib):
            fila_num = i + 2
            if len(fila) >= 4:
                fecha_str = str(fila[0]).strip() if len(fila) > 0 else ""
                guia_num = str(fila[1]).strip() if len(fila) > 1 else ""
                tipo_guia = str(fila[2]).strip() if len(fila) > 2 else ""
                empresa = str(fila[3]).strip() if len(fila) > 3 else ""
                fundo = str(fila[4]).strip() if len(fila) > 4 else ""
                archivo = str(fila[5]).strip() if len(fila) > 5 else ""
                col_cert = str(fila[7]).strip() if len(fila) > 7 else ""
                tipo_cert = str(fila[9]).strip() if len(fila) > 9 else ""

                if not guia_num or not empresa or empresa.upper() in ["S/D", "EMPRESA"]:
                    continue

                # Si ya tiene un certificado emitido explícito en col H:
                if col_cert and any(term in col_cert.upper() for term in ["EMITIDO", "CERT-", "FINAL", "COMERCIALIZACION"]):
                    continue

                # Si ya está registrado en la pestaña historial:
                if any(match_guia_en_texto(guia_num, gh) for gh in guias_emitidas_hist):
                    continue

                # Resolver RUC
                empresa_upper = empresa.upper()
                ruc_resuelto = ""
                if empresa_upper in cat_clientes:
                    ruc_resuelto = cat_clientes[empresa_upper].get("ruc", "")
                else:
                    for k_cli, v_cli in cat_clientes.items():
                        if k_cli in empresa_upper or empresa_upper in k_cli:
                            ruc_resuelto = v_cli.get("ruc", "")
                            break

                # Resolver Dirección de Partida
                fundo_upper = fundo.upper()
                dir_resuelta = ""
                dirs_empresa = cat_direcciones.get(empresa_upper, {})
                if not dirs_empresa:
                    for k_dir, v_map in cat_direcciones.items():
                        if k_dir in empresa_upper or empresa_upper in k_dir:
                            dirs_empresa = v_map
                            break
                if dirs_empresa:
                    if fundo_upper in dirs_empresa:
                        dir_resuelta = dirs_empresa[fundo_upper]
                    else:
                        for f_k, d_val in dirs_empresa.items():
                            if f_k in fundo_upper or fundo_upper in f_k:
                                dir_resuelta = d_val
                                break

                if not dir_resuelta and fundo:
                    dir_resuelta = f"Fundo - {fundo}"

                tipo_sug = tipo_cert
                if not tipo_sug:
                    tipo_sug = "Disposición Final 1" if "PETRAMAS" in empresa_upper else "Comercialización"

                pendientes.append({
                    "fila": fila_num,
                    "fecha": fecha_str,
                    "empresa": empresa,
                    "fundo": fundo,
                    "guia": guia_num,
                    "tipo_guia": tipo_guia,
                    "ruc": ruc_resuelto,
                    "direccion_partida": dir_resuelta,
                    "archivo": archivo,
                    "tipo_sugerido": tipo_sug
                })

        return pendientes
    except Exception as e:
        logger.error(f"Error consultando guías pendientes: {e}")
        return []

def consolidar_guias_repositorio_ocr(guias: List[Dict[str, Any]], drive_service=None) -> Dict[str, Any]:
    """
    Descarga cada guía de Drive seleccionada, ejecuta Vertex OCR,
    extrae vehículos (placas), ítems con sus descripciones/cantidades/pesos,
    y unifica todo para el formulario de la Mini App.
    """
    if not drive_service:
        drive_service, _ = obtener_servicios_google()

    items_resultado = []
    placas = []
    punto_partida_detectado = ""
    punto_llegada_detectado = ""
    cliente_detectado = ""
    ruc_detectado = ""

    for g in guias:
        nombre_o_id = g.get('archivo') or g.get('link_guia') or ''
        guia_num_fallback = g.get('guia') or g.get('numero_guia') or ''
        fecha_fallback = normalizar_fecha(g.get('fecha', ''))

        pdf_bytes = None
        if nombre_o_id:
            pdf_bytes = descargar_archivo_drive_por_id_o_nombre(nombre_o_id, drive_service)

        datos_ocr = None
        if pdf_bytes:
            try:
                datos_ocr = procesar_guia_ia_vertex(pdf_bytes)
            except Exception as e:
                logger.warning(f"Error OCR para guía '{guia_num_fallback}': {e}")

        if datos_ocr:
            s = datos_ocr.get('serie') or guia_num_fallback
            f = normalizar_fecha(datos_ocr.get('fecha') or fecha_fallback)
            p = str(datos_ocr.get('vehiculo', '')).strip().upper()

            # Auto-corrección si Vertex invirtió serie y placa
            if es_formato_placa(s) and es_formato_guia(p):
                s, p = p, s

            if p and p not in placas:
                placas.append(p)

            if not punto_partida_detectado and datos_ocr.get('punto_partida'):
                punto_partida_detectado = datos_ocr.get('punto_partida')

            if not punto_llegada_detectado and datos_ocr.get('punto_llegada'):
                punto_llegada_detectado = datos_ocr.get('punto_llegada')

            if not cliente_detectado and datos_ocr.get('cliente'):
                cliente_detectado = datos_ocr.get('cliente')

            if not ruc_detectado and datos_ocr.get('ruc_cliente'):
                ruc_detectado = datos_ocr.get('ruc_cliente')

            ocr_items = datos_ocr.get('items', [])
            if ocr_items:
                for it in ocr_items:
                    desc_limpia = limpiar_descripcion(it.get('desc', ''))
                    cant_limpia = formato_inteligente(limpiar_monto(it.get('cant', 1)))
                    um_raw = str(it.get('um', 'KG')).upper()
                    um_limpia = 'KG' if 'KILO' in um_raw else ('GLN' if 'GALO' in um_raw else ('UNID' if 'UNIDA' in um_raw else um_raw))
                    peso_limpio = formato_inteligente(limpiar_monto(it.get('peso', 0)))

                    items_resultado.append({
                        'fecha_origen': f,
                        'placa_origen': p,
                        'guia_origen': s,
                        'desc': desc_limpia,
                        'cant': cant_limpia,
                        'um': um_limpia,
                        'peso': peso_limpio
                    })
            else:
                items_resultado.append({
                    'fecha_origen': f,
                    'placa_origen': p,
                    'guia_origen': s,
                    'desc': 'RESIDUOS RECICLABLES Y APROVECHABLES',
                    'cant': '1',
                    'um': 'KG',
                    'peso': '0.00'
                })
        else:
            # Fallback si no hay PDF o falló OCR
            items_resultado.append({
                'fecha_origen': fecha_fallback,
                'placa_origen': '',
                'guia_origen': guia_num_fallback,
                'desc': 'RESIDUOS RECICLABLES Y APROVECHABLES',
                'cant': '1',
                'um': 'KG',
                'peso': '0.00'
            })

    def fecha_a_entero(fecha_str):
        try:
            p = str(fecha_str).strip().split('/')
            if len(p) == 3:
                return int(f"{p[2]}{p[1]}{p[0]}")
        except Exception:
            pass
        return 99999999

    items_resultado.sort(key=lambda x: fecha_a_entero(x['fecha_origen']))
    placa_sugerida = ", ".join(placas) if placas else ""

    return {
        "success": True,
        "placa": placa_sugerida,
        "placas": placas,
        "items": items_resultado,
        "punto_partida": punto_partida_detectado,
        "punto_llegada": punto_llegada_detectado,
        "cliente": cliente_detectado,
        "ruc_cliente": ruc_detectado,
        "total_procesadas": len(guias)
    }

def obtener_url_webapp_certificados(correlativo: Optional[str] = None, datos_edicion: Optional[Dict[str, Any]] = None, user_id: Optional[Any] = None, modo: Optional[str] = None) -> str:
    """Construye la URL segura HTTPS para abrir la Telegram Mini App de Certificados."""
    import urllib.parse
    import base64
    from config.settings import WEBAPP_CERTIFICADOS_URL, RENDER_EXTERNAL_URL

    base_url = WEBAPP_CERTIFICADOS_URL
    if not base_url and RENDER_EXTERNAL_URL:
        base_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/certificados"
    if not base_url:
        base_url = "https://lia-w2yb.onrender.com/certificados"

    params = {'v': str(int(time.time()))}
    if correlativo:
        params['corr'] = str(correlativo)
    if user_id:
        params['uid'] = str(user_id)
    if modo:
        params['modo'] = str(modo)
    if datos_edicion:
        json_str = json.dumps(datos_edicion, ensure_ascii=False)
        params['data'] = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

    sep = '&' if '?' in base_url else '?'
    return f"{base_url}{sep}{urllib.parse.urlencode(params)}"
