# ====================================================================
# --- SERVICIO DE CREDENCIALES Y ACCESOS (LÍA) ---
# ====================================================================
import os
import re
import asyncio
from datetime import datetime, timezone, timedelta
import gspread

from config.settings import SHEET_ID, logger
from core.sheets_client import obtener_credenciales

PET = timezone(timedelta(hours=-5))

HEADERS_CREDENCIALES = [
    'ID', 'SERVICIO', 'CATEGORIA', 'URL_LOGIN', 'USUARIO_RUC',
    'CONTRASEÑA', 'PIN_EXTRA', 'TITULAR', 'OBSERVACIONES', 'ULTIMA_ACTUALIZACION'
]

CATEGORIAS_CREDENCIALES = [
    "Tributario / SUNAT",
    "Balanza / Planta",
    "Clientes / Portales",
    "Transporte / Logística",
    "Bancos / Finanzas",
    "Correo / TI",
    "General"
]

def obtener_sheet_credenciales():
    """Abre o crea la hoja 'Credenciales' en el Spreadsheet oficial."""
    creds = obtener_credenciales()
    if not creds:
        raise ValueError("No se pudieron obtener credenciales para Google Sheets")
    client = gspread.authorize(creds)
    book = client.open_by_key(SHEET_ID)
    try:
        ws = book.worksheet("Credenciales")
    except gspread.exceptions.WorksheetNotFound:
        ws = book.add_worksheet(title="Credenciales", rows="500", cols="12")
        ws.append_row(HEADERS_CREDENCIALES)
    asegurar_estructura_sheet_credenciales(ws)
    return ws

def asegurar_estructura_sheet_credenciales(ws):
    """Garantiza que la hoja tenga los encabezados oficiales."""
    try:
        first_row = ws.row_values(1)
        if not first_row or len(first_row) < len(HEADERS_CREDENCIALES) or first_row[0] != 'ID':
            ws.update(values=[HEADERS_CREDENCIALES], range_name=f"A1:J1")
    except Exception as e:
        logger.error(f"Error asegurando estructura de hoja Credenciales: {e}")

def obtener_siguiente_id_credencial(records=None):
    """Genera el siguiente correlativo CRD-001, CRD-002..."""
    max_id = 0
    if records:
        for r in records:
            id_val = str(r.get("ID", "")).strip().upper()
            m = re.match(r"^CRD-(\d+)$", id_val)
            if m:
                num = int(m.group(1))
                if num > max_id:
                    max_id = num
    return f"CRD-{max_id + 1:03d}"

# ====================================================================
# --- FUNCIONES SÍNCRONAS Y ASÍNCRONAS DE LECTURA Y ESCRITURA ---
# ====================================================================

def obtener_credenciales_sync(query=None, categoria=None):
    """Lectura síncrona de credenciales para HTTP Server."""
    ws = obtener_sheet_credenciales()
    records = ws.get_all_records()
    resultados = []
    q_norm = (query or "").strip().lower()

    for r in records:
        if not r or not r.get("SERVICIO"):
            continue
        
        # Filtro por categoría
        if categoria and str(r.get("CATEGORIA", "")).strip().lower() != categoria.strip().lower():
            continue

        # Filtro por búsqueda de texto
        if q_norm:
            texto_busqueda = f"{r.get('SERVICIO', '')} {r.get('CATEGORIA', '')} {r.get('USUARIO_RUC', '')} {r.get('TITULAR', '')} {r.get('OBSERVACIONES', '')}".lower()
            if q_norm not in texto_busqueda:
                continue

        resultados.append(r)
    return resultados

async def async_obtener_credenciales(query=None, categoria=None):
    """Retorna la lista de credenciales registradas."""
    try:
        return await asyncio.to_thread(obtener_credenciales_sync, query, categoria)
    except Exception as e:
        logger.error(f"Error en async_obtener_credenciales: {e}")
        return []

def guardar_credencial_sync(datos: dict):
    """Guarda o actualiza una credencial de forma síncrona."""
    ws = obtener_sheet_credenciales()
    records = ws.get_all_records()
    crd_id = datos.get("id")
    now_str = datetime.now(PET).strftime("%d/%m/%Y")

    if crd_id:
        for idx, r in enumerate(records, start=2):
            if str(r.get("ID", "")).strip().upper() == str(crd_id).strip().upper():
                row = [
                    crd_id,
                    datos.get("servicio", r.get("SERVICIO", "")),
                    datos.get("categoria", r.get("CATEGORIA", "General")),
                    datos.get("url_login", r.get("URL_LOGIN", "")),
                    datos.get("usuario_ruc", r.get("USUARIO_RUC", "")),
                    datos.get("contraseña", r.get("CONTRASEÑA", "")),
                    datos.get("pin_extra", r.get("PIN_EXTRA", "")),
                    datos.get("titular", r.get("TITULAR", "")),
                    datos.get("observaciones", r.get("OBSERVACIONES", "")),
                    now_str
                ]
                ws.update(values=[row], range_name=f"A{idx}:J{idx}")
                return crd_id

    nuevo_id = obtener_siguiente_id_credencial(records)
    row = [
        nuevo_id,
        datos.get("servicio", "Sin nombre"),
        datos.get("categoria", "General"),
        datos.get("url_login", ""),
        datos.get("usuario_ruc", ""),
        datos.get("contraseña", ""),
        datos.get("pin_extra", ""),
        datos.get("titular", ""),
        datos.get("observaciones", ""),
        now_str
    ]
    ws.append_row(row)
    return nuevo_id

async def async_guardar_credencial(datos: dict):
    """Guarda o actualiza una credencial."""
    try:
        return await asyncio.to_thread(guardar_credencial_sync, datos)
    except Exception as e:
        logger.error(f"Error en async_guardar_credencial: {e}")
        raise e

def eliminar_credencial_sync(crd_id: str):
    """Elimina una credencial por su ID de forma síncrona."""
    ws = obtener_sheet_credenciales()
    records = ws.get_all_records()
    for idx, r in enumerate(records, start=2):
        if str(r.get("ID", "")).strip().upper() == str(crd_id).strip().upper():
            ws.delete_rows(idx)
            return True
    return False

async def async_eliminar_credencial(crd_id: str):
    """Elimina una credencial por su ID."""
    try:
        return await asyncio.to_thread(eliminar_credencial_sync, crd_id)
    except Exception as e:
        logger.error(f"Error en async_eliminar_credencial: {e}")
        return False
