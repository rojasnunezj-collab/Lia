# ====================================================================
# --- IMPORTS ---
# ====================================================================
import os
import re
import sqlite3
import asyncio
import json
import unicodedata
import difflib
from datetime import datetime
from config.settings import logger

# Para mantener PET en utilitarios
from datetime import timedelta, timezone
PET = timezone(timedelta(hours=-5))

# ====================================================================
# --- SUPABASE CONFIG ---
# ====================================================================
USE_SUPABASE = False
try:
    from supabase import create_client, Client
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            supabase: Client = create_client(SUPABASE_URL.strip(), SUPABASE_KEY.strip())
            USE_SUPABASE = True
            logger.info("Supabase configurado correctamente.")
        except Exception as init_err:
            logger.error(f"Error al inicializar Supabase (revisa tus credenciales): {init_err}")
except ImportError:
    pass

# ====================================================================
# --- FUNCIONES DE UTILIDAD (UTILS) ---
# ====================================================================
def clean_json_response(text):
    text = re.sub(r'```json\s*|\s*```', '', text, flags=re.IGNORECASE)
    match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
    return match.group(1) if match else text

def normalize_search_text(text: str) -> str:
    """Normaliza texto para búsqueda: mayúsculas, sin tildes/diacríticos y sin puntuación."""
    if not text:
        return ""
    text = str(text).strip().upper()
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    text = re.sub(r'[^A-Z0-9\s]', ' ', text)
    return ' '.join(text.split())

def match_company_flexible(query: str, target: str) -> tuple[bool, float]:
    """
    Búsqueda flexible por nombre de empresa (Solución 3):
    - Insensible a mayúsculas/minúsculas.
    - Insensible a tildes y acentos.
    - Búsqueda por subcadena parcial o palabras contenidas.
    - Tolerancia a errores ortográficos / leves de digitación (fuzzy matching con difflib).
    Retorna (is_match, score).
    """
    q_norm = normalize_search_text(query)
    t_norm = normalize_search_text(target)
    if not q_norm or not t_norm:
        return False, 0.0

    # 1. Coincidencia directa de subcadena
    if q_norm in t_norm:
        return True, 1.0

    q_words = q_norm.split()
    t_words = t_norm.split()

    def word_matches_target(qw, target_words, target_full):
        if qw in target_full:
            return True
        for tw in target_words:
            if len(qw) >= 4 and len(tw) >= 4:
                if difflib.SequenceMatcher(None, qw, tw).ratio() >= 0.78:
                    return True
        return False

    # 2. Múltiples palabras: todas deben coincidir (exacta o fuzzy)
    if len(q_words) > 1:
        if all(word_matches_target(qw, t_words, t_norm) for qw in q_words):
            return True, 0.95
    else:
        # 3. Palabra única: fuzzy match contra palabras del nombre
        qw = q_words[0]
        best_ratio = 0.0
        for tw in t_words:
            if len(qw) >= 4 and len(tw) >= 4:
                ratio = difflib.SequenceMatcher(None, qw, tw).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
        if best_ratio >= 0.78:
            return True, best_ratio

    # 4. Similitud global de toda la cadena
    ratio = difflib.SequenceMatcher(None, q_norm, t_norm).ratio()
    if ratio >= 0.75:
        return True, ratio

    return False, 0.0

# ====================================================================
# --- CONFIGURACIÓN DE BASE DE DATOS Y LOGS ---
# ====================================================================
def init_db():
    if USE_SUPABASE:
        # Supabase no requiere inicializar las tablas desde código de esta forma,
        # asumimos que las tablas 'logs' y 'estado_bot' ya fueron creadas en el panel de Supabase.
        return

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
    if USE_SUPABASE:
        try:
            response = supabase.table('estado_bot').select("valor").eq("clave", "MEMORIA_VINCULACION").execute()
            if response.data and len(response.data) > 0:
                memoria = json.loads(response.data[0]['valor'])
                return {int(k) if k.isdigit() else k: v for k, v in memoria.items()}
            return {}
        except Exception as e:
            logger.error(f"Error cargando memoria de Supabase: {e}")
            return {}

    # Fallback a SQLite
    try:
        conn = sqlite3.connect('lia_logs.db')
        cursor = conn.cursor()
        cursor.execute("SELECT valor FROM estado_bot WHERE clave = 'MEMORIA_VINCULACION'")
        row = cursor.fetchone()
        if row:
            memoria = json.loads(row[0])
            return {int(k) if k.isdigit() else k: v for k, v in memoria.items()}
    except Exception as e:
        logger.error(f"Error cargando memoria_vinculacion local: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
    return {}

def save_memoria_vinculacion(memoria_dict):
    valor_json = json.dumps(memoria_dict)
    
    if USE_SUPABASE:
        try:
            # Upsert en Supabase
            supabase.table('estado_bot').upsert({"clave": "MEMORIA_VINCULACION", "valor": valor_json}).execute()
        except Exception as e:
            logger.error(f"Error guardando memoria en Supabase: {e}")
        return

    # Fallback a SQLite
    try:
        conn = sqlite3.connect('lia_logs.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO estado_bot (clave, valor)
            VALUES (?, ?)
            ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor
        ''', ('MEMORIA_VINCULACION', valor_json))
        conn.commit()
    except Exception as e:
        logger.error(f"Error guardando memoria_vinculacion local: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def log_action(usuario_id, numero_guia, accion):
    fecha_str = datetime.now(PET).strftime("%Y-%m-%d %H:%M:%S")

    if USE_SUPABASE:
        try:
            supabase.table('logs').insert({
                "fecha": fecha_str,
                "usuario_id": usuario_id,
                "numero_guia": numero_guia,
                "accion": accion
            }).execute()
        except Exception as e:
            logger.error(f"Error escribiendo log en Supabase: {e}")
        return

    # Fallback a SQLite
    try:
        conn = sqlite3.connect('lia_logs.db')
        cursor = conn.cursor()
        cursor.execute("INSERT INTO logs (fecha, usuario_id, numero_guia, accion) VALUES (?, ?, ?, ?)",
                       (fecha_str, usuario_id, numero_guia, accion))
        conn.commit()
    except Exception as e:
        logger.error(f"Error escribiendo log local: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

async def async_log_action(usuario_id, numero_guia, accion):
    await asyncio.to_thread(log_action, usuario_id, numero_guia, accion)
