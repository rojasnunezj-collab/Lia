# ====================================================================
# --- IMPORTS ---
# ====================================================================
import re
import sqlite3
import asyncio
import json
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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS estado_bot (
                clave TEXT PRIMARY KEY,
                valor TEXT
            )
        ''')
        conn.commit()
    except Exception as e:
        logger.error(f"Error inicializando BD: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def load_memoria_vinculacion():
    try:
        conn = sqlite3.connect('lia_logs.db')
        cursor = conn.cursor()
        cursor.execute("SELECT valor FROM estado_bot WHERE clave = 'MEMORIA_VINCULACION'")
        row = cursor.fetchone()
        if row:
            memoria = json.loads(row[0])
            return {int(k) if k.isdigit() else k: v for k, v in memoria.items()}
    except Exception as e:
        logger.error(f"Error cargando memoria_vinculacion: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
    return {}

def save_memoria_vinculacion(memoria_dict):
    try:
        conn = sqlite3.connect('lia_logs.db')
        cursor = conn.cursor()
        valor_json = json.dumps(memoria_dict)
        cursor.execute('''
            INSERT INTO estado_bot (clave, valor)
            VALUES (?, ?)
            ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor
        ''', ('MEMORIA_VINCULACION', valor_json))
        conn.commit()
    except Exception as e:
        logger.error(f"Error guardando memoria_vinculacion: {e}")
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
