# ====================================================================
# --- SERVICIO DE BITÁCORA INTELIGENTE (LÍA) ---
# ====================================================================
import os
import re
import json
import asyncio
from datetime import datetime, timezone, timedelta
import gspread
from google.genai import types

from config.settings import SHEET_ID, logger
from core.sheets_client import obtener_credenciales
from core.ai_client import generar_con_reintento, MODEL_NAME

PET = timezone(timedelta(hours=-5))

CATEGORIAS_DISPONIBLES = [
    "PROCEDIMIENTO",
    "CLIENTE",
    "TRANSPORTE",
    "INCIDENCIA",
    "FINANZAS",
    "RECORDATORIO",
    "GENERAL"
]

CATEGORIAS_INFO = {
    "PROCEDIMIENTO": ("📜", "Procedimiento / Regla"),
    "CLIENTE": ("👤", "Cliente / Proveedor"),
    "TRANSPORTE": ("🚚", "Transporte / Operación"),
    "INCIDENCIA": ("⚠️", "Incidencia / Problema"),
    "FINANZAS": ("💰", "Finanzas / Pagos"),
    "RECORDATORIO": ("📌", "Recordatorio / Tarea"),
    "GENERAL": ("📝", "General / Otro")
}

HEADERS_BITACORA = ['FECHA', 'USUARIO', 'CATEGORIA', 'ETIQUETAS', 'FORMATO', 'LINK', 'COMENTARIO']

def asegurar_estructura_sheet_bitacora(ws):
    """Garantiza que la hoja Bitácora tenga las 7 columnas estándar."""
    try:
        first_row = ws.row_values(1)
        if not first_row or len(first_row) < 7 or first_row[:2] != ['FECHA', 'USUARIO'] or 'CATEGORIA' not in first_row:
            rows = ws.get_all_values()
            new_rows = [HEADERS_BITACORA]
            for r in rows[1:]:
                if not r or not any(r): continue
                f = r[0] if len(r) > 0 else ""
                u = r[1] if len(r) > 1 else ""
                if len(r) >= 7 and 'CATEGORIA' in first_row:
                    cat = r[2] if len(r) > 2 else "GENERAL"
                    tag = r[3] if len(r) > 3 else ""
                    fmt = r[4] if len(r) > 4 else ""
                    lk = r[5] if len(r) > 5 else ""
                    com = r[6] if len(r) > 6 else ""
                else:
                    # Antiguo formato de 5 columnas
                    fmt = r[2] if len(r) > 2 else ""
                    lk = r[3] if len(r) > 3 else ""
                    com = r[4] if len(r) > 4 else ""
                    cat = "GENERAL"
                    tag = ""
                new_rows.append([f, u, cat, tag, fmt, lk, com])
            ws.clear()
            ws.update(values=new_rows, range_name=f"A1:G{len(new_rows)}")
    except Exception as e:
        logger.error(f"Error asegurando estructura de Bitácora: {e}")

def limpiar_json_ia(raw_text: str) -> str:
    """Limpia bloques de código markdown de una respuesta JSON."""
    text = raw_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()

# ====================================================================
# --- PROCESAMIENTO CON IA (TRANSCRIPCIÓN, OCR Y CLASIFICACIÓN) ---
# ====================================================================
async def procesar_entrada_ia(texto=None, file_path=None, mime_type=None, caption=None, msg_status=None):
    """
    Procesa audio (transcripción), foto/documento (OCR y análisis) o texto directo.
    Extrae contenido fiel, categoría sugerida y etiquetas (tags).
    """
    partes = []
    formato = "Texto/Anotación"

    if file_path and os.path.exists(file_path):
        with open(file_path, "rb") as f:
            content = f.read()
        
        is_audio = any(x in (mime_type or '') for x in ['audio', 'ogg', 'opus', 'mp3', 'wav', 'm4a'])
        is_image = any(x in (mime_type or '') for x in ['image', 'jpeg', 'jpg', 'png', 'webp'])
        is_pdf = 'pdf' in (mime_type or '')

        if is_audio:
            formato = "Nota de Voz" if 'ogg' in (mime_type or '') or 'opus' in (mime_type or '') else "Audio"
            part = types.Part.from_bytes(data=content, mime_type=mime_type or "audio/ogg")
            partes.append(part)
            prompt = """Eres un transcriptor y clasificador experto para una empresa de transporte y logística en Perú.
Se te entrega una nota de audio grabada por un usuario del equipo.

[[INSTRUCCIONES]]
1. TRANSCRIPCIÓN: Transcribe TODO lo que dice el audio de forma fiel, completa y en español. Si menciona nombres de empresas (ej: Beta, Prosembra, Los Olivos), personas, placas o números, captúralos con máxima precisión.
2. CATEGORÍA: Elige estrictamente UNA de las siguientes categorías según el tema principal:
   - PROCEDIMIENTO (reglas de emisión de guías, certificados, normativas)
   - CLIENTE (acuerdos, datos o instrucciones específicas de un cliente o proveedor)
   - TRANSPORTE (camiones, choferes, placas, RUCs de transporte, fletes)
   - INCIDENCIA (problemas, fallas de camión, demoras, errores en guías)
   - FINANZAS (gastos, cobros, peajes, compras, pagos)
   - RECORDATORIO (tareas pendientes, avisos de fin de mes)
   - GENERAL (otros apuntes operativos)
3. ETIQUETAS: Genera entre 2 y 4 tags o palabras clave en mayúsculas iniciando con # (ej: "#PROSEMBRA #CERTIFICADO #FECHA").
4. RESUMEN: Redacta un resumen corto de 4 a 8 palabras.

Responde ÚNICAMENTE en JSON con este formato exacto:
{
  "texto": "[Transcripción completa del audio]",
  "categoria": "[CATEGORIA]",
  "tags": "[#TAG1 #TAG2 ...]",
  "resumen": "[Resumen corto]"
}
"""
        elif is_image or is_pdf:
            formato = "Foto" if is_image else "Documento"
            part = types.Part.from_bytes(data=content, mime_type=mime_type or ("image/jpeg" if is_image else "application/pdf"))
            partes.append(part)
            caption_str = f"Comentario adicional del usuario: \"{caption}\"" if caption else "Sin comentario adicional."
            prompt = f"""Eres un asistente de análisis documental y visual para una empresa de transporte y logística en Perú.
Se te entrega una imagen o documento PDF. {caption_str}

[[INSTRUCCIONES]]
1. CONTENIDO Y OCR: Extrae el texto legible más importante (OCR), números clave (RUC, guía, placa, montos, fechas, nombres) y explica detalladamente qué documento o situación muestra. Integra el comentario del usuario si lo hay.
2. CATEGORÍA: Elige estrictamente UNA de las siguientes categorías:
   - PROCEDIMIENTO
   - CLIENTE
   - TRANSPORTE
   - INCIDENCIA
   - FINANZAS
   - RECORDATORIO
   - GENERAL
3. ETIQUETAS: Genera entre 2 y 4 tags en mayúsculas iniciando con # (ej: "#GUIA #REMISIÓN #BETA").
4. RESUMEN: Redacta un resumen de 4 a 8 palabras.

Responde ÚNICAMENTE en JSON con este formato exacto:
{
  "texto": "[Descripción detallada y texto extraído]",
  "categoria": "[CATEGORIA]",
  "tags": "[#TAG1 #TAG2 ...]",
  "resumen": "[Resumen corto]"
}
"""
        else:
            formato = "Archivo"
            prompt = f"""Analiza este archivo para una bitácora de transporte. Comentario: "{caption or ''}".
Determina la categoría entre [PROCEDIMIENTO, CLIENTE, TRANSPORTE, INCIDENCIA, FINANZAS, RECORDATORIO, GENERAL] y 2-4 tags.
Responde ÚNICAMENTE en JSON:
{{
  "texto": "{caption or 'Archivo adjunto'}",
  "categoria": "GENERAL",
  "tags": "#ARCHIVO #BITACORA",
  "resumen": "Archivo registrado"
}}
"""
    else:
        # Texto plano
        texto_limpio = str(texto or "").strip()
        prompt = f"""Eres un clasificador inteligente para una bitácora de gestión logística y transporte en Perú.
Se te entrega el siguiente texto anotado por un usuario:
"{texto_limpio}"

[[INSTRUCCIONES]]
1. CATEGORÍA: Elige estrictamente UNA de las siguientes categorías:
   - PROCEDIMIENTO (reglas de emisión de guías, certificados, normativas)
   - CLIENTE (acuerdos o notas de clientes o proveedores como Los Olivos, Prosembra, Beta, Green, Agrolatina, etc.)
   - TRANSPORTE (camiones, choferes, placas, RUCs de transportistas, fletes)
   - INCIDENCIA (problemas, demoras, fallas mecánicas, errores)
   - FINANZAS (gastos, cobros, peajes, compras, pagos)
   - RECORDATORIO (tareas pendientes, avisos de fin de mes)
   - GENERAL (otros apuntes operativos)
2. ETIQUETAS: Genera entre 2 y 4 tags en mayúsculas iniciando con # (ej: "#LOS_OLIVOS #FUNDOS #GUIA").
3. RESUMEN: Redacta un resumen conciso de 4 a 8 palabras.

Responde ÚNICAMENTE en JSON con este formato exacto:
{{
  "texto": "{texto_limpio}",
  "categoria": "[CATEGORIA]",
  "tags": "[#TAG1 #TAG2 ...]",
  "resumen": "[Resumen corto]"
}}
"""

    try:
        response = await generar_con_reintento(partes, prompt, msg_status, is_json=True)
        cleaned = limpiar_json_ia(response.text)
        data = json.loads(cleaned)
        
        texto_final = data.get("texto", texto or caption or "Sin contenido")
        cat_final = data.get("categoria", "GENERAL").strip().upper()
        if cat_final not in CATEGORIAS_DISPONIBLES:
            cat_final = "GENERAL"
            
        tags_final = data.get("tags", "#BITACORA").strip()
        resumen_final = data.get("resumen", "")
        
        return {
            "texto": texto_final,
            "categoria": cat_final,
            "tags": tags_final,
            "formato": formato,
            "resumen": resumen_final
        }
    except Exception as e:
        logger.error(f"Error procesando entrada de bitácora con IA: {e}")
        return {
            "texto": texto or caption or "Archivo adjunto registrado",
            "categoria": "GENERAL",
            "tags": "#GENERAL",
            "formato": formato,
            "resumen": "Anotación registrada"
        }

# ====================================================================
# --- LECTURA Y ESCRITURA EN GOOGLE SHEETS ---
# ====================================================================
def _guardar_en_sheet_sync(fecha, usuario, categoria, tags, formato, enlace_drive, comentario):
    creds = obtener_credenciales()
    client = gspread.authorize(creds)
    book = client.open_by_key(SHEET_ID)
    try:
        ws = book.worksheet("Bitacora")
    except gspread.exceptions.WorksheetNotFound:
        ws = book.add_worksheet(title="Bitacora", rows="1000", cols="8")
        ws.append_row(HEADERS_BITACORA)

    asegurar_estructura_sheet_bitacora(ws)
    fila = [fecha, usuario, categoria, tags, formato, enlace_drive, comentario]
    next_row = len(ws.get_all_values()) + 1
    ws.insert_row(fila, index=next_row, value_input_option='USER_ENTERED')
    return True

async def guardar_registro_bitacora(fecha, usuario, categoria, tags, formato, enlace_drive, comentario):
    return await asyncio.to_thread(
        _guardar_en_sheet_sync, fecha, usuario, categoria, tags, formato, enlace_drive, comentario
    )

def _obtener_registros_sync():
    creds = obtener_credenciales()
    client = gspread.authorize(creds)
    book = client.open_by_key(SHEET_ID)
    try:
        ws = book.worksheet("Bitacora")
    except gspread.exceptions.WorksheetNotFound:
        return []

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    header = values[0]
    registros = []
    is_standard = len(header) >= 7 and 'CATEGORIA' in header

    for r in values[1:]:
        if not r or not any(r): continue
        if is_standard:
            fecha = r[0] if len(r) > 0 else ""
            usuario = r[1] if len(r) > 1 else ""
            cat = r[2] if len(r) > 2 else "GENERAL"
            tags = r[3] if len(r) > 3 else ""
            formato = r[4] if len(r) > 4 else ""
            link = r[5] if len(r) > 5 else ""
            comentario = r[6] if len(r) > 6 else ""
        else:
            fecha = r[0] if len(r) > 0 else ""
            usuario = r[1] if len(r) > 1 else ""
            cat = "GENERAL"
            tags = ""
            formato = r[2] if len(r) > 2 else ""
            link = r[3] if len(r) > 3 else ""
            comentario = r[4] if len(r) > 4 else ""

        registros.append({
            "fecha": fecha,
            "usuario": usuario,
            "categoria": cat,
            "tags": tags,
            "formato": formato,
            "link": link,
            "comentario": comentario
        })
    return registros

async def obtener_registros_bitacora():
    return await asyncio.to_thread(_obtener_registros_sync)

# ====================================================================
# --- BÚSQUEDA CONVERSACIONAL Y FLEXIBLE CON IA ---
# ====================================================================
async def buscar_conversacional_bitacora(pregunta_usuario: str, msg_status=None):
    """
    Realiza una búsqueda semántica y conversacional en la bitácora con Gemini.
    Tolerante a errores ortográficos, abreviaturas y preguntas naturales.
    """
    registros = await obtener_registros_bitacora()
    if not registros:
        return "📓 La Bitácora está vacía actualmente. No hay registros guardados."

    # Formatear contexto de la bitácora para la IA
    # Mostramos los registros en orden cronológico inverso o todos si son razonables
    contexto = "REGISTROS REGISTRADOS EN LA BITÁCORA:\n\n"
    for idx, r in enumerate(reversed(registros), 1):
        contexto += f"[{idx}] FECHA: {r['fecha']} | USUARIO: {r['usuario']} | CATEGORÍA: {r['categoria']} | TAGS: {r['tags']} | TIPO: {r['formato']}\n"
        if r['link']:
            contexto += f"    ENLACE DRIVE: {r['link']}\n"
        contexto += f"    DETALLE/NOTA: {r['comentario']}\n\n"

    prompt = f"""Eres Lía, la asistente virtual de operaciones, logística y gestión documental en Perú.
El usuario te está haciendo una consulta en lenguaje natural sobre la información guardada en la Bitácora de la empresa.

[[INSTRUCCIONES CLAVE]]
1. MÁXIMA FLEXIBILIDAD Y TOLERANCIA A ERRORES:
   - El usuario puede escribir con errores ortográficos, faltas de tipeo, palabras juntas, abreviaturas ("q", "d", "cn", "xq", "tbn", "pk", "aser"), o nombres mal escritos (ej: "villacuri" en vez de "Villa Curi", "piter" por "Pedro", etc.).
   - Interpreta SIEMPRE la intención real detrás de la consulta. NO exijas coincidencia exacta de palabras.
2. RESPUESTA DIRECTA, AMABLE Y CONVERSACIONAL:
   - Responde de forma cálida, directa y estructurada en español (con viñetas si hay varios puntos).
   - Si la consulta pide un procedimiento o regla (ej: cómo hacer una guía o certificado para un cliente), explica exactamente el procedimiento registrado.
   - Si la consulta pide datos específicos (un RUC, una placa, un teléfono, un peso), resáltalo en negrita.
3. FUENTES, FECHAS Y ENLACES:
   - Menciona la fecha y el usuario que lo registró si aporta contexto.
   - Si el registro tiene un enlace a Google Drive de fotos, audios o documentos, INCLÚYELO obligatoriamente en formato Markdown: [📎 Ver Archivo en Drive](enlace).
4. RESÚMENES O CONSULTAS GENERALES:
   - Si el usuario pregunta qué se anotó hoy, qué es lo último, o pide un resumen general, lista de forma concisa y ordenada las anotaciones más recientes.
5. SI NO EXISTE INFORMACIÓN:
   - Si la información no se encuentra en la Bitácora, indícalo amablemente diciendo que no hay registros sobre ese tema, y menciona brevemente de qué temas o clientes sí hay notas registradas para orientarlo.

[[REGISTROS EN BITÁCORA]]
{contexto}

[[CONSULTA DEL USUARIO]]
"{pregunta_usuario}"
"""

    try:
        response = await generar_con_reintento([], prompt, msg_status, is_json=False)
        return response.text.strip()
    except Exception as e:
        logger.error(f"Error en búsqueda conversacional de bitácora: {e}")
        return f"❌ Ocurrió un error al consultar la Bitácora con la IA: {e}"

async def obtener_ultimas_anotaciones_formateadas(n=5):
    """Devuelve las últimas N anotaciones formateadas en tarjetas de texto."""
    registros = await obtener_registros_bitacora()
    if not registros:
        return "📓 La Bitácora está vacía actualmente."

    ultimos = list(reversed(registros))[:n]
    texto = f"📋 **Últimas {len(ultimos)} Anotaciones en Bitácora:**\n\n"
    for idx, r in enumerate(ultimos, 1):
        cat_info = CATEGORIAS_INFO.get(r['categoria'], ("📝", r['categoria']))
        texto += f"**{idx}. {cat_info[0]} {r['categoria']}** ({r['fecha']})\n"
        texto += f"👤 *Por:* `{r['usuario']}` | 📂 *Tipo:* `{r['formato']}`\n"
        if r['tags']:
            texto += f"🏷️ *Tags:* `{r['tags']}`\n"
        texto += f"📝 _{r['comentario']}_\n"
        if r['link']:
            texto += f"📎 [Ver Archivo en Drive]({r['link']})\n"
        texto += "────────────────────\n"
    return texto
