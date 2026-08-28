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
from google.oauth2.credentials import Credentials as OAuthCredentials

def obtener_credenciales():
    token_path = '/app/token.json'
    sa_path = '/app/credenciales_lia.json'
    render_secret_path = '/etc/secrets/credenciales_lia.json'
    
    # Fallback a archivos locales si no estamos en Docker
    local_token = 'token.json'
    if not os.path.exists(token_path) and os.path.exists(local_token):
        token_path = local_token
        
    if os.path.exists(render_secret_path):
        sa_path = render_secret_path
    elif not os.path.exists(sa_path) and KEY_FILE and os.path.exists(KEY_FILE):
        sa_path = KEY_FILE

    SCOPES_COMBINED = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform"
    ]

    import json
    import base64
    
    # Intentar con Base64 (A prueba de errores de copiar/pegar)
    env_b64 = os.environ.get("GOOGLE_CREDENTIALS_B64")
    if env_b64:
        try:
            info = json.loads(base64.b64decode(env_b64).decode('utf-8'))
            return service_account.Credentials.from_service_account_info(info, scopes=SCOPES_COMBINED)
        except Exception as e:
            logger.error(f"❌ Error parseando GOOGLE_CREDENTIALS_B64: {e}")

    env_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if env_json:
        try:
            info = json.loads(env_json)
            return service_account.Credentials.from_service_account_info(info, scopes=SCOPES_COMBINED)
        except Exception as e:
            logger.error(f"❌ Error parseando GOOGLE_CREDENTIALS_JSON: {e}")

    # Prioridad 1: Intentar usar Token Humano (token.json) para evitar cuotas de Service Account
    if os.path.exists(token_path):
        try:
            return OAuthCredentials.from_authorized_user_file(token_path, SCOPES_COMBINED)
        except Exception as e:
            logger.error(f"❌ Error leyendo token.json: {e}")

    # Prioridad 2: Fallback a Service Account
    if os.path.exists(sa_path):
        try:
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
                
            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
            file = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink', supportsAllDrives=True).execute(num_retries=3)
            drive_service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute(num_retries=3)
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

def normalizar_valor_upper(val):
    if isinstance(val, str):
        val_strip = val.strip()
        if val_strip.lower().startswith("http://") or val_strip.lower().startswith("https://"):
            return val_strip
        return val_strip.upper()
    return val

# ====================================================================
# --- GOOGLE SHEETS UPSERT ---
# ====================================================================
def sync_upsert_row(sheet, num_guia, row_data, col_guia_index=2, col_comentario_index=9, allow_singuia_update=False):
    try:
        timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M:%S")
        
        # Normalizar todo el row_data a mayúsculas
        row_data = [normalizar_valor_upper(x) for x in row_data]
        
        if not num_guia:
            next_row = len(sheet.get_all_values()) + 1
            sheet.insert_row(row_data, index=next_row, value_input_option='USER_ENTERED')
            return "appended"
            
        col_values = sheet.col_values(col_guia_index)
        num_upper = str(num_guia).strip().upper()
        col_values_upper = [str(x).strip().upper() for x in col_values]
        
        terminos_genericos = ["SIN GUIA", "SIN GUÍA", "S/D", "BALANZA", "TICKET"]
        is_singuia = any(term in num_upper for term in terminos_genericos) or num_upper in ["-", ""]
        
        if num_upper in col_values_upper and (allow_singuia_update or not is_singuia):
            row_idx = col_values_upper.index(num_upper) + 1  
            
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
            
            next_row = len(sheet.get_all_values()) + 1
            sheet.insert_row(row_data, index=next_row, value_input_option='USER_ENTERED')
            return "appended"
    except Exception as e:
        logger.error(f"Error en upsert: {e}")
        raise e

async def async_upsert_row(sheet, num_guia, row_data, col_guia_index=2, col_comentario_index=9, allow_singuia_update=False):
    return await asyncio.to_thread(sync_upsert_row, sheet, num_guia, row_data, col_guia_index, col_comentario_index, allow_singuia_update)

async def async_get_all_records(sheet):
    return await asyncio.to_thread(sheet.get_all_records)
