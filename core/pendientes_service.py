# ====================================================================
# --- SERVICIO DE PENDIENTES CON ALERTAS (LÍA) ---
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

HEADERS_PENDIENTES = [
    'ID', 'FECHA_REGISTRO', 'USUARIO', 'TITULO_TAREA', 'DETALLE',
    'CLIENTE_REF', 'FECHA_ALERTA', 'PRIORIDAD', 'ESTADO', 'ALERTA_ENVIADA', 'FECHA_CIERRE'
]

def obtener_sheet_pendientes():
    """Abre o crea la hoja 'Pendientes' en el Spreadsheet oficial."""
    creds = obtener_credenciales()
    if not creds:
        raise ValueError("No se pudieron obtener credenciales para Google Sheets")
    client = gspread.authorize(creds)
    book = client.open_by_key(SHEET_ID)
    try:
        ws = book.worksheet("Pendientes")
    except gspread.exceptions.WorksheetNotFound:
        ws = book.add_worksheet(title="Pendientes", rows="1000", cols="15")
        ws.append_row(HEADERS_PENDIENTES)
    asegurar_estructura_sheet_pendientes(ws)
    return ws

def asegurar_estructura_sheet_pendientes(ws):
    """Garantiza que la hoja tenga los encabezados oficiales."""
    try:
        first_row = ws.row_values(1)
        if not first_row or len(first_row) < len(HEADERS_PENDIENTES) or first_row[0] != 'ID':
            ws.update(values=[HEADERS_PENDIENTES], range_name=f"A1:K1")
    except Exception as e:
        logger.error(f"Error asegurando estructura de hoja Pendientes: {e}")

def parse_fecha_pet(fecha_str):
    """Parsea una cadena de fecha DD/MM/YYYY HH:MM o DD/MM/YYYY a datetime en PET."""
    if not fecha_str:
        return None
    s = str(fecha_str).strip()
    formatos = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]
    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=PET)
        except ValueError:
            continue
    return None

def obtener_siguiente_id_pendiente(records=None):
    """Genera el siguiente correlativo PND-001, PND-002..."""
    max_id = 0
    if records:
        for r in records:
            id_val = str(r.get("ID", "")).strip().upper()
            m = re.match(r"^PND-(\d+)$", id_val)
            if m:
                num = int(m.group(1))
                if num > max_id:
                    max_id = num
    return f"PND-{max_id + 1:03d}"

# ====================================================================
# --- FUNCIONES SÍNCRONAS Y ASÍNCRONAS DE LECTURA Y ESCRITURA ---
# ====================================================================

def obtener_pendientes_sync(solo_activos=False):
    """Lectura síncrona de pendientes para HTTP Server."""
    ws = obtener_sheet_pendientes()
    records = ws.get_all_records()
    if solo_activos:
        records = [r for r in records if str(r.get("ESTADO", "")).strip().upper() in ["PENDIENTE", "POSPUESTO", "EN PROCESO"]]
    return records

async def async_obtener_pendientes(solo_activos=False):
    """Retorna los registros de pendientes como lista de diccionarios."""
    try:
        return await asyncio.to_thread(obtener_pendientes_sync, solo_activos)
    except Exception as e:
        logger.error(f"Error en async_obtener_pendientes: {e}")
        return []

def crear_pendiente_sync(datos: dict):
    """Inserta un nuevo pendiente en Google Sheets de forma síncrona."""
    ws = obtener_sheet_pendientes()
    records = ws.get_all_records()
    pnd_id = datos.get("id") or obtener_siguiente_id_pendiente(records)
    now_str = datetime.now(PET).strftime("%d/%m/%Y %H:%M")
    
    row = [
        pnd_id,
        now_str,
        datos.get("usuario", "Usuario"),
        datos.get("titulo", "Sin título"),
        datos.get("detalle", ""),
        datos.get("cliente_ref", ""),
        datos.get("fecha_alerta", now_str),
        datos.get("prioridad", "MEDIA").upper(),
        "PENDIENTE",
        "NO",
        ""
    ]
    ws.append_row(row)
    return pnd_id

async def async_crear_pendiente(datos: dict):
    """Inserta un nuevo pendiente en Google Sheets."""
    try:
        return await asyncio.to_thread(crear_pendiente_sync, datos)
    except Exception as e:
        logger.error(f"Error en async_crear_pendiente: {e}")
        raise e

def actualizar_estado_pendiente_sync(pnd_id: str, nuevo_estado: str, fecha_cierre=None):
    """Actualiza el estado de un pendiente de forma síncrona."""
    ws = obtener_sheet_pendientes()
    records = ws.get_all_records()
    for idx, r in enumerate(records, start=2):
        if str(r.get("ID", "")).strip().upper() == str(pnd_id).strip().upper():
            ws.update_cell(idx, 9, nuevo_estado.upper()) # Col I: ESTADO
            if nuevo_estado.upper() == "COMPLETADO":
                cierre_str = fecha_cierre or datetime.now(PET).strftime("%d/%m/%Y %H:%M")
                ws.update_cell(idx, 11, cierre_str) # Col K: FECHA_CIERRE
            return True
    return False

async def async_actualizar_estado_pendiente(pnd_id: str, nuevo_estado: str, fecha_cierre=None):
    """Actualiza el estado de un pendiente (COMPLETADO, CANCELADO, PENDIENTE)."""
    try:
        return await asyncio.to_thread(actualizar_estado_pendiente_sync, pnd_id, nuevo_estado, fecha_cierre)
    except Exception as e:
        logger.error(f"Error en async_actualizar_estado_pendiente: {e}")
        return False

def posponer_pendiente_sync(pnd_id: str, minutos: int):
    """Pospone la alerta de forma síncrona."""
    ws = obtener_sheet_pendientes()
    records = ws.get_all_records()
    nueva_fecha_dt = datetime.now(PET) + timedelta(minutes=minutos)
    nueva_fecha_str = nueva_fecha_dt.strftime("%d/%m/%Y %H:%M")

    for idx, r in enumerate(records, start=2):
        if str(r.get("ID", "")).strip().upper() == str(pnd_id).strip().upper():
            ws.update_cell(idx, 7, nueva_fecha_str) # Col G: FECHA_ALERTA
            ws.update_cell(idx, 9, "PENDIENTE")     # Col I: ESTADO
            ws.update_cell(idx, 10, "NO")           # Col J: ALERTA_ENVIADA
            return nueva_fecha_str
    return None

async def async_posponer_pendiente(pnd_id: str, minutos: int):
    """Pospone la alerta sumando minutos a la fecha actual y reseteando ALERTA_ENVIADA a 'NO'."""
    try:
        return await asyncio.to_thread(posponer_pendiente_sync, pnd_id, minutos)
    except Exception as e:
        logger.error(f"Error en async_posponer_pendiente: {e}")
        return None

async def async_marcar_alerta_enviada(pnd_id: str):
    """Marca la columna ALERTA_ENVIADA como 'SI'."""
    def _marcar():
        ws = obtener_sheet_pendientes()
        records = ws.get_all_records()
        for idx, r in enumerate(records, start=2):
            if str(r.get("ID", "")).strip().upper() == str(pnd_id).strip().upper():
                ws.update_cell(idx, 10, "SI")
                return True
        return False

    try:
        return await asyncio.to_thread(_marcar)
    except Exception as e:
        logger.error(f"Error en async_marcar_alerta_enviada: {e}")
        return False

async def async_obtener_alertas_por_disparar():
    """
    Busca pendientes cuyo estado sea PENDIENTE, ALERTA_ENVIADA sea 'NO'
    y cuya FECHA_ALERTA ya se haya alcanzado o pasado según la hora de Perú.
    """
    def _check():
        ws = obtener_sheet_pendientes()
        records = ws.get_all_records()
        now_pet = datetime.now(PET)
        disparables = []

        for r in records:
            estado = str(r.get("ESTADO", "")).strip().upper()
            alerta_enviada = str(r.get("ALERTA_ENVIADA", "")).strip().upper()
            if estado == "PENDIENTE" and alerta_enviada != "SI":
                fecha_alerta_str = str(r.get("FECHA_ALERTA", "")).strip()
                dt_alerta = parse_fecha_pet(fecha_alerta_str)
                if dt_alerta and dt_alerta <= now_pet:
                    disparables.append(r)
        return disparables

    try:
        return await asyncio.to_thread(_check)
    except Exception as e:
        logger.error(f"Error en async_obtener_alertas_por_disparar: {e}")
        return []

# ====================================================================
# --- PROCESAMIENTO INTELIGENTE CON IA (GEMINI) ---
# ====================================================================

async def procesar_pendiente_ia(texto=None, file_path=None, mime_type=None):
    """
    Analiza una instrucción (texto, audio o documento) y extrae:
    - titulo: breve resumen de la tarea
    - detalle: contexto o información adicional
    - cliente_ref: empresa, fundo o referencia relacionada
    - fecha_alerta: fecha y hora programada en formato 'DD/MM/YYYY HH:MM'
    - prioridad: 'ALTA', 'MEDIA' o 'BAJA'
    """
    now_pet = datetime.now(PET)
    hoy_str = now_pet.strftime("%d/%m/%Y")
    hora_str = now_pet.strftime("%H:%M")
    dia_semana = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"][now_pet.weekday()]

    prompt = f"""Eres el asistente de gestión operativa Lía para la empresa EPMI S.A.C.
Tu tarea es estructurar un PENDIENTE / RECORDATORIO a partir de lo que el usuario indica.

CONTEXTO TEMPORAL ACTUAL (ZONA HORARIA PERÚ UTC-5):
- Fecha actual: {hoy_str} ({dia_semana})
- Hora actual: {hora_str}
- Año en curso: {now_pet.year}

REGLAS DE INTERPRETACIÓN:
1. Extrae:
   - "titulo": Título claro y conciso de la tarea (máx. 10 palabras).
   - "detalle": Datos específicos (números de guía, placas, observaciones, nombres).
   - "cliente_ref": Nombre del cliente, empresa, proveedor o fundo mencionado (ej. "PROSEMBRA", "LOS OLIVOS", "CERRO PRIETO"). Si no hay, dejar cadena vacía.
   - "fecha_alerta": Formato estricto "DD/MM/YYYY HH:MM".
     * Si dice "en 30 minutos", suma 30 minutos a la hora actual.
     * Si dice "en 2 horas", suma 2 horas a la hora actual.
     * Si dice "mañana a las 9am", usa la fecha de mañana con hora "09:00".
     * Si no especifica hora pero dice "mañana", usa las 09:00 AM.
     * Si no especifica fecha ni hora, programa por defecto 2 horas después de la hora actual.
   - "prioridad": "ALTA" si menciona urgencia, multas, pagos, o plazos inmediatos; "BAJA" si es una tarea secundaria; de lo contrario "MEDIA".

RESPONDE ÚNICAMENTE CON UN OBJETO JSON VÁLIDO CON ESTA ESTRUCTURA:
{{
    "titulo": "...",
    "detalle": "...",
    "cliente_ref": "...",
    "fecha_alerta": "DD/MM/YYYY HH:MM",
    "prioridad": "ALTA" | "MEDIA" | "BAJA"
}}
"""

    partes = []
    if file_path and os.path.exists(file_path):
        with open(file_path, "rb") as f:
            content = f.read()
        part = types.Part.from_bytes(data=content, mime_type=mime_type or "audio/ogg")
        partes.append(part)

    texto_final = prompt
    if texto:
        texto_final += f"\n\nMENSAJE DEL USUARIO:\n\"{texto}\""
    partes.append(texto_final)

    try:
        response = await generar_con_reintento(
            model=MODEL_NAME,
            contents=partes,
            config=types.GenerateContentConfig(temperature=0.1)
        )
        raw_text = response.text.strip()
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```[a-zA-Z]*\n?", "", raw_text)
            raw_text = re.sub(r"\n?```$", "", raw_text)
        data = json.loads(raw_text.strip())
        return data
    except Exception as e:
        logger.error(f"Error procesando pendiente con IA: {e}")
        # Fallback básico si falla la IA
        alerta_def = (now_pet + timedelta(hours=2)).strftime("%d/%m/%Y %H:%M")
        return {
            "titulo": (texto or "Nuevo pendiente")[:50],
            "detalle": texto or "",
            "cliente_ref": "",
            "fecha_alerta": alerta_def,
            "prioridad": "MEDIA"
        }

def obtener_url_panel(tab='pendientes', user_id=None):
    """Construye la URL segura HTTPS para abrir la Telegram Mini App del Panel de Gestión."""
    import urllib.parse
    import time
    from config.settings import WEBAPP_PANEL_URL, RENDER_EXTERNAL_URL

    base_url = WEBAPP_PANEL_URL
    if not base_url and RENDER_EXTERNAL_URL:
        base_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/panel"
    if not base_url:
        base_url = "https://rojasnunezj-collab.github.io/Lia/webapp/panel.html"

    params = {'v': str(int(time.time())), 'tab': tab}
    if user_id:
        params['uid'] = str(user_id)

    sep = '&' if '?' in base_url else '?'
    return f"{base_url}{sep}{urllib.parse.urlencode(params)}"
