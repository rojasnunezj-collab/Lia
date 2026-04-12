# ====================================================================
# --- IMPORTS ---
# ====================================================================
import os
import time
import asyncio
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from config.settings import KEY_FILE, base_path, logger

# ====================================================================
# --- CONSTANTES Y VARIABLES SHEET/DRIVE ---
# ====================================================================
PET = timezone(timedelta(hours=-5))
SHEET_ID = os.getenv("SHEET_ID")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
DRIVE_FOLDER_LEER = os.getenv("DRIVE_FOLDER_LEER")
SHEET_URL_DIRECT = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"

sheet_notas = None
sheet_control = None
drive_service = None

# ====================================================================
# --- AUTENTICACIÓN GOOGLE ---
# ====================================================================
def obtener_credenciales():
    sa_path = '/app/credenciales_lia.json'
    
    # Fallback a KEY_FILE local si no estamos en el entorno de producción
    if not os.path.exists(sa_path) and KEY_FILE and os.path.exists(KEY_FILE):
        sa_path = KEY_FILE

    if os.path.exists(sa_path):
        try:
            SCOPES_COMBINED = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/cloud-platform"
            ]
            return service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES_COMBINED)
        except Exception as e:
            logger.error(f"❌ Error en Cuenta de Servicio: {e}")
            return None
    else:
        logger.error(f"❌ No se encontró el archivo de credenciales en {sa_path}")
    return None

# ====================================================================
# --- CONEXIÓN GSPREAD / DRIVE ---
# ====================================================================
def conectar_servicios():
    global sheet_notas, sheet_control, drive_service
    try:
        final_creds = obtener_credenciales()
        if not final_creds:
            logger.error("❌ No se encontró un método de autenticación válido.")
            return False

        client = gspread.authorize(final_creds)
        drive_service = build('drive', 'v3', credentials=final_creds, cache_discovery=False)
        
        from core.ai_client import init_ai
        init_ai(final_creds)
        
        book = client.open_by_key(SHEET_ID)
            
        try:
            sheet_control = book.worksheet("Registro_Guias")
        except gspread.exceptions.WorksheetNotFound:
            sheet_control = book.add_worksheet(title="Registro_Guias", rows="1000", cols="10")
            sheet_control.append_row(["Fecha", "N° Guía", "Tipo Guía", "Motivo", "Empresa Principal", "Destinatario/Remitente", "Destinario/Proveedor", "Link Drive", "Observacion", "Comentario Manual"])

        return True
    except Exception as e:
        logger.error(f"❌ Error al conectar servicios: {e}")
        return False

# ====================================================================
# --- GOOGLE DRIVE FUNCIONES ---
# ====================================================================
def subir_a_drive(file_path, mime_type, folder_id=None):
    for attempt in range(3):
        try:
            if not drive_service: conectar_servicios()
            if not drive_service: return "No subido"

            file_metadata = {'name': os.path.basename(file_path)}
            final_folder = folder_id if folder_id else DRIVE_FOLDER_ID
            if final_folder: file_metadata['parents'] = [final_folder]
                
            media = MediaFileUpload(file_path, mimetype=mime_type)
            file = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink', supportsAllDrives=True).execute()
            drive_service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
            return file.get('webViewLink')
        except Exception as e:
            if attempt < 2: time.sleep(5)
            else: return f"No subido: {e}"

async def async_subir_a_drive(file_path, mime_type, folder_id=None):
    return await asyncio.to_thread(subir_a_drive, file_path, mime_type, folder_id)

def buscar_link_en_drive(nombre_archivo):
    try:
        if not drive_service: conectar_servicios()
        if not drive_service: return None
        
        nombre_limpio = str(nombre_archivo).strip()
        query = f"name='{nombre_limpio}'"
        results = drive_service.files().list(q=query, fields="files(id, webViewLink)").execute()
        items = results.get('files', [])
        
        if items:
            return items[0]['webViewLink']
    except Exception as e:
        logger.error(f"Error buscando archivo en Drive: {e}")
    return None

async def async_buscar_link_en_drive(nombre_archivo):
    return await asyncio.to_thread(buscar_link_en_drive, nombre_archivo)

# ====================================================================
# --- GOOGLE SHEETS UPSERT ---
# ====================================================================
def sync_upsert_row(sheet, num_guia, row_data, col_guia_index=2, col_comentario_index=9):
    try:
        timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M")
        if not num_guia:
            sheet.append_row(row_data, value_input_option='USER_ENTERED')
            return "appended"
            
        col_values = sheet.col_values(col_guia_index)
        if num_guia in col_values:
            row_idx = col_values.index(num_guia) + 1  
            
            while len(row_data) < col_comentario_index:
                row_data.append("")
            
            row_data[col_comentario_index - 1] = f"🔄 Actualizado: {timestamp}"
            
            try:
                sheet.update(values=[row_data], range_name=f"A{row_idx}", value_input_option='USER_ENTERED')
            except TypeError:
                try:
                    sheet.update(f"A{row_idx}", [row_data], value_input_option='USER_ENTERED')
                except Exception:
                    sheet.update([row_data], f"A{row_idx}")
            return "updated"
        else:
            while len(row_data) < col_comentario_index:
                row_data.append("")
            row_data[col_comentario_index - 1] = f"✅ Nuevo: {timestamp}"
            
            sheet.append_row(row_data, value_input_option='USER_ENTERED')
            return "appended"
    except Exception as e:
        logger.error(f"Error en upsert: {e}")
        raise e

async def async_upsert_row(sheet, num_guia, row_data, col_guia_index=2, col_comentario_index=9):
    return await asyncio.to_thread(sync_upsert_row, sheet, num_guia, row_data, col_guia_index, col_comentario_index)

async def async_get_all_records(sheet):
    return await asyncio.to_thread(sheet.get_all_records)
