# ====================================================================
# --- MÓDULO DE FACTURAS ELECTRÓNICAS (PDF / XML) - LÍA ---
# ====================================================================
import os
import re
import json
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
import gspread
try:
    import pypdf
except ImportError:
    pypdf = None
from google.genai import types

from config.settings import logger, SHEET_ID, DRIVE_FOLDER_FACTURAS
from core.sheets_client import obtener_credenciales, normalizar_valor_upper
from core.ai_client import generar_con_reintento
from utils.helpers import clean_json_response, normalize_search_text

PET = timezone(timedelta(hours=-5))

# ====================================================================
# --- PROMPT PARA LECTURA DE FACTURAS EN PDF CON IA (GEMINI) ---
# ====================================================================
PROMPT_ANALISIS_FACTURA = """Eres un asistente experto en facturación electrónica peruana (SUNAT) y análisis de documentos contables.
Tu tarea es analizar minuciosamente el documento PDF adjunto que corresponde a una FACTURA (o comprobante de pago electrónico similar) y extraer la información en formato JSON estricto.

Debes extraer los siguientes campos obligatorios a nivel de factura:
1. "fecha_emision": Fecha de emisión de la factura en formato DD/MM/YYYY (ej: "15/09/2026").
2. "numero_factura": Serie y correlativo completo de la factura (ej: "F001-0004567", "E001-123", "F002-12345").
3. "emisor_ruc": Número de RUC del emisor (11 dígitos, ej: "20601234567").
4. "emisor_nombre": Razón Social o nombre comercial de la empresa que emite la factura.
5. "moneda": Código de moneda "PEN" (Soles / S/.) o "USD" (Dólares / $).
6. "cantidad": Cantidad total de bienes o servicios facturados (suma de cantidades). Si no especifica o es un servicio global, colocar 1.0.
7. "valor_unitario": Valor unitario sin IGV. Si la factura tiene varios ítems o es global, calcula: valor_total / cantidad.
8. "precio_unitario": Precio unitario con IGV incluido. Si la factura tiene varios ítems o es global, calcula: importe_total / cantidad.
9. "valor_total": Valor de venta total de la factura sin IGV (Subtotal / Op. Gravada, ej: 1000.00).
10. "igv": Monto numérico total del IGV (ej: 180.00). Si está exonerado o no tiene IGV, colocar 0.00.
11. "importe_total": Importe total a pagar de la factura en formato numérico (ej: 1180.00).

REGLAS DE CÁLCULO:
- NO extraigas la descripción de los productos ni ítems individuales; solo se registra una fila por factura.
- "valor_total" es el valor de venta total sin IGV (Op. Gravada / Subtotal). Si no figura explícito, calcula: valor_total = importe_total - igv.
- Si no figura cantidad explícita, usa cantidad = 1.0.
- Si cantidad > 0: valor_unitario = valor_total / cantidad, y precio_unitario = importe_total / cantidad.
- Si solo figura Precio Unitario (con IGV) y no Valor Unitario, calcula: valor_unitario = precio_unitario / 1.18.
- Si solo figura Valor Unitario, calcula: precio_unitario = valor_unitario * 1.18.

RESPONDE ÚNICAMENTE CON EL OBJETO JSON VÁLIDO, SIN BLOQUES MARKDOWN EXTRA:
{
  "fecha_emision": "DD/MM/YYYY",
  "numero_factura": "F001-0000000",
  "emisor_ruc": "20XXXXXXXXX",
  "emisor_nombre": "RAZON SOCIAL S.A.C.",
  "moneda": "PEN",
  "cantidad": 1.0,
  "valor_unitario": 0.00,
  "precio_unitario": 0.00,
  "valor_total": 0.00,
  "igv": 0.00,
  "importe_total": 0.00
}
"""

# ====================================================================
# --- HELPER PARSER XML UBL 2.1 SUNAT ---
# ====================================================================
def _clean_tag(tag):
    if not isinstance(tag, str):
        return ""
    if '}' in tag:
        tag = tag.split('}')[-1]
    if ':' in tag:
        tag = tag.split(':')[-1]
    return tag

def _first_not_none(*elements):
    for el in elements:
        if el is not None:
            return el
    return None

def _find_child(el, name):
    if el is None: return None
    for c in el:
        if _clean_tag(c.tag).lower() == name.lower():
            return c
    return None

def _find_desc(el, name):
    if el is None: return None
    for d in el.iter():
        if _clean_tag(d.tag).lower() == name.lower():
            return d
    return None

def _find_all_children(el, name):
    if el is None: return []
    return [c for c in el if _clean_tag(c.tag).lower() == name.lower()]

def _format_date(date_str):
    if not date_str:
        return ""
    date_str = str(date_str).strip()
    # Si viene en YYYY-MM-DD
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', date_str)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return date_str

def _to_float(val, default=0.0):
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    val_str = str(val).strip().replace(',', '')
    try:
        return float(val_str)
    except Exception:
        return default

def parse_factura_xml(file_path):
    """
    Parsea un archivo XML de Factura Electrónica SUNAT (UBL 2.0 / 2.1).
    Extrae con precisión matemática del 100%:
    - Fecha Emisión
    - N° Factura
    - RUC Emisor y Empresa Emisora
    - Cantidad total
    - Valor Unitario
    - Precio Unitario
    - Valor Total
    - IGV
    - Importe Total
    - Moneda
    """
    try:
        with open(file_path, 'rb') as f:
            tree = ET.parse(f)
            root = tree.getroot()

        # 1. N° Factura
        id_el = _first_not_none(_find_child(root, 'id'), _find_desc(root, 'id'))
        numero_factura = id_el.text.strip().upper() if id_el is not None and id_el.text else "S/N"

        # 2. Fecha Emisión
        date_el = _first_not_none(_find_child(root, 'issuedate'), _find_desc(root, 'issuedate'))
        raw_date = date_el.text.strip() if date_el is not None and date_el.text else ""
        fecha_emision = _format_date(raw_date)

        # 3. Moneda
        curr_el = _first_not_none(_find_child(root, 'documentcurrencycode'), _find_desc(root, 'documentcurrencycode'))
        moneda = curr_el.text.strip().upper() if curr_el is not None and curr_el.text else "PEN"
        if moneda in ["SOLES", "SOL", "S/.", "S/"]:
            moneda = "PEN"
        elif moneda in ["DOLARES", "DOLAR", "USD", "$"]:
            moneda = "USD"

        # 4. Emisor (RUC y Razón Social)
        emisor_ruc = ""
        emisor_nombre = ""
        supplier = _find_desc(root, 'accountingsupplierparty')
        if supplier is not None:
            # Buscar RUC
            for desc in supplier.iter():
                t = _clean_tag(desc.tag).lower()
                if t in ['id', 'companyid'] and desc.text:
                    txt = desc.text.strip()
                    if len(txt) == 11 and txt.isdigit():
                        emisor_ruc = txt
                        break
                    elif not emisor_ruc and txt.isdigit():
                        emisor_ruc = txt

            # Buscar Razón Social
            name_el = _first_not_none(_find_desc(supplier, 'registrationname'), _find_desc(supplier, 'name'))
            if name_el is not None and name_el.text:
                emisor_nombre = name_el.text.strip().upper()

        if not emisor_nombre:
            emisor_nombre = "EMISOR DESCONOCIDO"

        # 5. IGV
        igv = 0.0
        tax_total = _find_desc(root, 'taxtotal')
        if tax_total is not None:
            # Buscar subtotal de IGV (Código 1000 o nombre IGV)
            subtotals = _find_all_children(tax_total, 'taxsubtotal')
            igv_found = False
            for sub in subtotals:
                scheme = _find_desc(sub, 'taxscheme')
                scheme_id = _find_desc(scheme, 'id') if scheme is not None else None
                scheme_name = _find_desc(scheme, 'name') if scheme is not None else None
                id_txt = scheme_id.text.strip() if scheme_id is not None and scheme_id.text else ""
                name_txt = scheme_name.text.strip().upper() if scheme_name is not None and scheme_name.text else ""
                if id_txt == '1000' or 'IGV' in name_txt:
                    sub_amount = _find_desc(sub, 'taxamount')
                    if sub_amount is not None and sub_amount.text:
                        igv = _to_float(sub_amount.text)
                        igv_found = True
                        break
            if not igv_found:
                tot_amount = _first_not_none(_find_child(tax_total, 'taxamount'), _find_desc(tax_total, 'taxamount'))
                if tot_amount is not None and tot_amount.text:
                    igv = _to_float(tot_amount.text)

        # 6. Importe Total y Valor Total a nivel global
        importe_total = 0.0
        valor_total = 0.0
        monetary = _find_desc(root, 'legalmonetarytotal')
        if monetary is not None:
            pay_el = _first_not_none(_find_desc(monetary, 'payableamount'), _find_desc(monetary, 'taxinclusiveamount'))
            if pay_el is not None and pay_el.text:
                importe_total = _to_float(pay_el.text)
            line_ext_el = _find_desc(monetary, 'lineextensionamount')
            if line_ext_el is not None and line_ext_el.text:
                valor_total = _to_float(line_ext_el.text)

        # 7. Líneas de la Factura (para calcular Cantidad, Valor Unitario y Precio Unitario)
        lines = [c for c in root if _clean_tag(c.tag).lower() in ['invoiceline', 'creditnoteline', 'debitnoteline']]
        total_cantidad = 0.0
        suma_valor_lineas = 0.0
        single_vu = 0.0
        single_pu = 0.0

        for line in lines:
            qty_el = _first_not_none(_find_child(line, 'invoicedquantity'), _find_desc(line, 'invoicedquantity'), _find_desc(line, 'quantity'))
            cant = _to_float(qty_el.text if qty_el is not None and qty_el.text else 1.0)
            total_cantidad += cant

            val_tot_el = _first_not_none(_find_child(line, 'lineextensionamount'), _find_desc(line, 'lineextensionamount'))
            val_tot_line = _to_float(val_tot_el.text if val_tot_el is not None and val_tot_el.text else 0.0)
            suma_valor_lineas += val_tot_line

            if len(lines) == 1:
                price_el = _find_desc(line, 'price')
                val_unit_el = _find_desc(price_el, 'priceamount') if price_el is not None else None
                if val_unit_el is not None and val_unit_el.text:
                    single_vu = _to_float(val_unit_el.text)

                pricing_ref = _find_desc(line, 'pricingreference')
                if pricing_ref is not None:
                    alt_price = _find_desc(pricing_ref, 'alternativeconditionprice')
                    if alt_price is not None:
                        p_amt = _find_desc(alt_price, 'priceamount')
                        if p_amt is not None and p_amt.text:
                            single_pu = _to_float(p_amt.text)

        if total_cantidad <= 0:
            total_cantidad = 1.0

        if valor_total == 0.0:
            if suma_valor_lineas > 0:
                valor_total = suma_valor_lineas
            elif importe_total > 0:
                valor_total = round(importe_total - igv, 2)

        if importe_total == 0.0 and valor_total > 0:
            importe_total = round(valor_total + igv, 2)

        # Determinar Valor Unitario y Precio Unitario
        if len(lines) == 1 and single_vu > 0:
            valor_unitario = single_vu
        elif total_cantidad > 0 and valor_total > 0:
            valor_unitario = round(valor_total / total_cantidad, 4)
        else:
            valor_unitario = round(valor_total, 2)

        if len(lines) == 1 and single_pu > 0:
            precio_unitario = single_pu
        elif total_cantidad > 0 and importe_total > 0:
            precio_unitario = round(importe_total / total_cantidad, 4)
        else:
            precio_unitario = round(valor_unitario * 1.18, 4) if valor_unitario > 0 else 0.0

        return {
            "fecha_emision": fecha_emision,
            "numero_factura": numero_factura,
            "emisor_ruc": emisor_ruc,
            "emisor_nombre": emisor_nombre,
            "moneda": moneda,
            "cantidad": round(total_cantidad, 2) if total_cantidad != int(total_cantidad) else int(total_cantidad),
            "valor_unitario": valor_unitario,
            "precio_unitario": precio_unitario,
            "valor_total": round(valor_total, 2),
            "igv": round(igv, 2),
            "importe_total": round(importe_total, 2),
            "origen": "XML"
        }
    except Exception as e:
        logger.error(f"❌ Error parseando XML de factura: {e}")
        raise ValueError(f"No se pudo parsear el archivo XML: {e}")

async def async_parse_factura_xml(file_path):
    return await asyncio.to_thread(parse_factura_xml, file_path)

# ====================================================================
# --- PARSER DE FACTURAS EN PDF CON IA (GEMINI 2.5 FLASH) ---
# ====================================================================
async def parse_factura_pdf(file_path, msg_status=None):
    """
    Lee una factura en PDF (digital o escaneada) utilizando Gemini 2.5 Flash
    y devuelve la estructura estandarizada a nivel de factura (sin detalle de ítems).
    """
    try:
        with open(file_path, "rb") as bf:
            content = bf.read()
        part = types.Part.from_bytes(data=content, mime_type="application/pdf")

        response = await generar_con_reintento([part], PROMPT_ANALISIS_FACTURA, msg_status, is_json=True)
        raw_text = clean_json_response(response.text)
        data = json.loads(raw_text)

        fecha_emision = _format_date(data.get("fecha_emision", ""))
        numero_factura = str(data.get("numero_factura", "")).strip().upper()
        emisor_ruc = str(data.get("emisor_ruc", "")).strip()
        emisor_nombre = str(data.get("emisor_nombre", "")).strip().upper()
        moneda = str(data.get("moneda", "PEN")).strip().upper()
        if moneda in ["SOLES", "SOL", "S/.", "S/"]:
            moneda = "PEN"
        elif moneda in ["DOLARES", "DOLAR", "USD", "$"]:
            moneda = "USD"

        cantidad = _to_float(data.get("cantidad", 1.0))
        if cantidad <= 0:
            cantidad = 1.0

        igv = _to_float(data.get("igv", 0.0))
        importe_total = _to_float(data.get("importe_total", 0.0))
        valor_total = _to_float(data.get("valor_total", 0.0))

        if valor_total == 0.0 and importe_total > 0:
            valor_total = round(importe_total - igv, 2)
        if importe_total == 0.0 and valor_total > 0:
            importe_total = round(valor_total + igv, 2)

        valor_unitario = _to_float(data.get("valor_unitario", 0.0))
        precio_unitario = _to_float(data.get("precio_unitario", 0.0))

        if valor_unitario == 0.0 and cantidad > 0 and valor_total > 0:
            valor_unitario = round(valor_total / cantidad, 4)
        if precio_unitario == 0.0 and cantidad > 0 and importe_total > 0:
            precio_unitario = round(importe_total / cantidad, 4)
        elif precio_unitario == 0.0 and valor_unitario > 0:
            precio_unitario = round(valor_unitario * 1.18, 4)

        return {
            "fecha_emision": fecha_emision,
            "numero_factura": numero_factura,
            "emisor_ruc": emisor_ruc,
            "emisor_nombre": emisor_nombre,
            "moneda": moneda,
            "cantidad": round(cantidad, 2) if cantidad != int(cantidad) else int(cantidad),
            "valor_unitario": valor_unitario,
            "precio_unitario": precio_unitario,
            "valor_total": round(valor_total, 2),
            "igv": round(igv, 2),
            "importe_total": round(importe_total, 2),
            "origen": "PDF_IA"
        }
    except Exception as e:
        logger.error(f"❌ Error parseando PDF de factura con IA: {e}")
        raise ValueError(f"No se pudo extraer la información del PDF: {e}")

# ====================================================================
# --- DETECCIÓN INTELIGENTE DE TIPO DE DOCUMENTO EN PDF ---
# ====================================================================
def detectar_tipo_documento_pdf(file_path):
    """
    Inspecciona el texto incrustado en el PDF para determinar rápidamente
    si corresponde a una 'FACTURA' o una 'GUIA'.
    Si es escaneado o ambiguo, retorna 'DESCONOCIDO'.
    """
    if pypdf is None:
        return "DESCONOCIDO"
    try:
        reader = pypdf.PdfReader(file_path)
        text = ""
        for i in range(min(2, len(reader.pages))):
            extracted = reader.pages[i].extract_text()
            if extracted:
                text += " " + extracted.upper()

        if not text.strip():
            return "DESCONOCIDO"

        score_factura = 0
        if "FACTURA ELECTR" in text or "FACTURA DE VENTA" in text or "FACTURA" in text:
            score_factura += 3
        if "BOLETA DE VENTA" in text:
            score_factura += 2
        if "PRECIO DE VENTA" in text or "VALOR DE VENTA" in text or "OP. GRAVADA" in text or "OPERACION GRAVADA" in text:
            score_factura += 2
        if "IMPORTE TOTAL" in text or "TOTAL A PAGAR" in text:
            score_factura += 1
        if "DETRACCION" in text or "DETRACCIÓN" in text:
            score_factura += 1

        score_guia = 0
        if "GUIA DE REMISION" in text or "GUÍA DE REMISIÓN" in text or "GUIA REMISION" in text:
            score_guia += 4
        if "DESTINATARIO" in text and "REMITENTE" in text:
            score_guia += 2
        if "MOTIVO DE TRASLADO" in text or "PUNTO DE PARTIDA" in text or "PUNTO DE LLEGADA" in text:
            score_guia += 3
        if "PESO BRUTO" in text or "MODALIDAD DE TRANSPORTE" in text:
            score_guia += 2

        if score_factura >= 3 and score_factura > score_guia:
            return "FACTURA"
        elif score_guia >= 3 and score_guia > score_factura:
            return "GUIA"
        else:
            return "DESCONOCIDO"
    except Exception as e:
        logger.warning(f"Error analizando texto de PDF: {e}")
        return "DESCONOCIDO"

async def async_detectar_tipo_documento_pdf(file_path):
    return await asyncio.to_thread(detectar_tipo_documento_pdf, file_path)

# ====================================================================
# --- GOOGLE SHEETS: PESTAÑA REGISTRO_FACTURAS ---
# ====================================================================
SHEET_FACTURAS_TITLE = "Registro_Facturas"
FACTURAS_HEADERS = [
    "Fecha Emisión",
    "N° Factura",
    "RUC Emisor",
    "Empresa Emisora",
    "Cantidad",
    "Valor Unitario",
    "Precio Unitario",
    "Valor Total",
    "IGV",
    "Importe Total",
    "Moneda",
    "Enlace Drive",
    "Fecha Registro"
]

def obtener_o_crear_sheet_facturas():
    """Conecta con Google Sheets y obtiene la pestaña Registro_Facturas, asegurando los 13 encabezados."""
    creds = obtener_credenciales()
    if not creds:
        raise ValueError("No se pudieron obtener credenciales para Google Sheets.")
    client = gspread.authorize(creds)
    book = client.open_by_key(SHEET_ID)
    try:
        sheet = book.worksheet(SHEET_FACTURAS_TITLE)
        # Sincronizar si la columna Descripción aún existe en la fila 1
        try:
            current_headers = sheet.row_values(1)
            for idx, h in enumerate(current_headers, start=1):
                if "descrip" in str(h).lower():
                    logger.info(f"Eliminando columna '{h}' (col {idx}) de {SHEET_FACTURAS_TITLE}...")
                    sheet.delete_columns(idx)
                    sheet.update('A1:M1', [FACTURAS_HEADERS])
                    break
        except Exception as ex_sync:
            logger.warning(f"Aviso al sincronizar cabeceras de facturas: {ex_sync}")
    except gspread.exceptions.WorksheetNotFound:
        sheet = book.add_worksheet(title=SHEET_FACTURAS_TITLE, rows="1000", cols="15")
        sheet.append_row(FACTURAS_HEADERS)
        logger.info(f"✅ Pestaña '{SHEET_FACTURAS_TITLE}' creada con éxito.")
    return sheet

def guardar_factura_en_sheet(datos_factura):
    """
    Registra una factura en la pestaña 'Registro_Facturas'.
    Inserta exactamente UNA fila por factura con los 13 campos requeridos (sin Descripción).
    Si el N° de factura ya existía, reemplaza las filas previas para evitar duplicados.
    Retorna tupla: (accion, 1) -> ('appended' o 'updated', 1)
    """
    try:
        sheet = obtener_o_crear_sheet_facturas()
        num_factura = str(datos_factura.get("numero_factura", "")).strip().upper()
        ruc = str(datos_factura.get("emisor_ruc", "")).strip()

        col_facturas = sheet.col_values(2)  # Columna B: N° Factura
        col_facturas_upper = [str(x).strip().upper() for x in col_facturas]

        # Verificar si ya existe en la hoja
        existe = num_factura in col_facturas_upper and num_factura not in ["", "S/N"]

        if existe:
            # Eliminar filas antiguas de esa misma factura de abajo hacia arriba
            matching_indices = [i + 1 for i, val in enumerate(col_facturas_upper) if val == num_factura]
            for row_idx in reversed(matching_indices):
                sheet.delete_rows(row_idx)
            accion = "updated"
        else:
            accion = "appended"

        timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M:%S")

        row = [
            datos_factura.get("fecha_emision", ""),
            num_factura,
            ruc,
            datos_factura.get("emisor_nombre", ""),
            datos_factura.get("cantidad", 1),
            datos_factura.get("valor_unitario", 0.0),
            datos_factura.get("precio_unitario", 0.0),
            datos_factura.get("valor_total", 0.0),
            datos_factura.get("igv", 0.0),
            datos_factura.get("importe_total", 0.0),
            datos_factura.get("moneda", "PEN"),
            datos_factura.get("enlace_drive", ""),
            timestamp
        ]
        row = [normalizar_valor_upper(x) if isinstance(x, str) else x for x in row]

        sheet.append_row(row, value_input_option='USER_ENTERED')
        logger.info(f"✅ Factura {num_factura} ({accion}): 1 fila registrada.")
        return accion, 1
    except Exception as e:
        logger.error(f"❌ Error al guardar factura en Google Sheets: {e}")
        raise e

async def async_guardar_factura_en_sheet(datos_factura):
    return await asyncio.to_thread(guardar_factura_en_sheet, datos_factura)

# ====================================================================
# --- BÚSQUEDA DE FACTURAS EN GOOGLE SHEETS ---
# ====================================================================
def buscar_facturas_en_sheet(query_str):
    """
    Busca facturas en 'Registro_Facturas' por N° Factura, RUC o Nombre de Empresa.
    Retorna lista de facturas encontradas.
    """
    try:
        sheet = obtener_o_crear_sheet_facturas()
        records = sheet.get_all_records()
        q_norm = normalize_search_text(query_str)
        if not q_norm:
            return []

        coincidencias = []
        for r in records:
            num = str(r.get("N° Factura", "")).strip().upper()
            ruc = str(r.get("RUC Emisor", "")).strip()
            empresa = str(r.get("Empresa Emisora", "")).strip().upper()
            fecha = str(r.get("Fecha Emisión", "")).strip()

            target_text = normalize_search_text(f"{num} {ruc} {empresa} {fecha}")

            if q_norm in target_text:
                coincidencias.append({
                    "numero_factura": num,
                    "fecha_emision": fecha,
                    "emisor_ruc": ruc,
                    "emisor_nombre": empresa,
                    "cantidad": r.get("Cantidad", 1),
                    "valor_unitario": r.get("Valor Unitario", 0.0),
                    "precio_unitario": r.get("Precio Unitario", 0.0),
                    "valor_total": r.get("Valor Total", 0.0),
                    "igv": r.get("IGV", 0.0),
                    "importe_total": r.get("Importe Total", 0.0),
                    "moneda": r.get("Moneda", "PEN"),
                    "enlace_drive": r.get("Enlace Drive", "")
                })

        return coincidencias
    except Exception as e:
        logger.error(f"Error buscando facturas: {e}")
        return []

async def async_buscar_facturas_en_sheet(query_str):
    return await asyncio.to_thread(buscar_facturas_en_sheet, query_str)
