# ====================================================================
# --- IMPORTS ---
# ====================================================================
import re
import sqlite3
import asyncio
from datetime import datetime
from config.settings import logger

# Para mantener PET en utilitarios
from datetime import timedelta, timezone
PET = timezone(timedelta(hours=-5))

# ====================================================================
# --- FUNCIONES DE UTILIDAD (UTILS) ---
# ====================================================================
def clean_json_response(text):
    text = re.sub(r'```json\s*|\s*```', '', text, flags=re.IGNORECASE)
    match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
    return match.group(1) if match else text

# ====================================================================
# --- CONFIGURACIÓN DE BASE DE DATOS Y LOGS ---
# ====================================================================
def init_db():
    try:
        conn = sqlite3.connect('lia_logs.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,
                usuario_id INTEGER,
                numero_guia TEXT,
                accion TEXT
            )
        ''')
        conn.commit()
    except Exception as e:
        logger.error(f"Error inicializando BD: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def log_action(usuario_id, numero_guia, accion):
    try:
        conn = sqlite3.connect('lia_logs.db')
        cursor = conn.cursor()
        fecha_str = datetime.now(PET).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("INSERT INTO logs (fecha, usuario_id, numero_guia, accion) VALUES (?, ?, ?, ?)",
                       (fecha_str, usuario_id, numero_guia, accion))
        conn.commit()
    except Exception as e:
        logger.error(f"Error escribiendo log: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

async def async_log_action(usuario_id, numero_guia, accion):
    await asyncio.to_thread(log_action, usuario_id, numero_guia, accion)
