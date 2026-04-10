# ====================================================================
# --- IMPORTS Y CONFIGURACIÓN ---
# ====================================================================
import os
import logging
from dotenv import load_dotenv

# ====================================================================
# --- LOGGING ---
# ====================================================================
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================================================
# --- CARGA DE .ENV ---
# ====================================================================
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(base_path, '.env')
load_dotenv(dotenv_path=env_path)

# ====================================================================
# --- VARIABLES GLOBALES ---
# ====================================================================
KEY_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("DOCUMENT_AI_PROJECT_ID")
REGION_ESTABLE = os.getenv("VERTEX_AI_LOCATION", "us-central1") 
MODEL_NAME = "gemini-2.0-flash" 

if KEY_FILE and not os.path.isabs(KEY_FILE):
    KEY_FILE = os.path.join(base_path, KEY_FILE)

os.environ["GOOGLE_API_USE_MTLS_ENDPOINT"] = "never"

# ====================================================================
# --- CONSTANTES DE MODOS ---
# ====================================================================
MODO_GUIAS_LEER = "GUIAS_LEER" 
MODO_GUIAS_REGISTRAR = "GUIAS_REGISTRAR"
MODO_GUIAS_MANUAL = "GUIAS_MANUAL"
MODO_GUIAS_MANUAL_FECHA = "GUIAS_MANUAL_FECHA"
MODO_GUIAS_MANUAL_NUMGUIA = "GUIAS_MANUAL_NUMGUIA"
MODO_GUIAS_MANUAL_TIPO = "GUIAS_MANUAL_TIPO"
MODO_GUIAS_MANUAL_EMPRESA = "GUIAS_MANUAL_EMPRESA"
MODO_GUIAS_MANUAL_FUNDO = "GUIAS_MANUAL_FUNDO"
MODO_REPORTE_REGISTRO = "REPORTE_REGISTRO"
MODO_REPORTE_RECIBIDAS = "REPORTE_RECIBIDAS"
MODO_COMENTAR_GUIA = "COMENTAR_GUIA"
MODO_COMENTAR_TEXTO = "COMENTAR_TEXTO"
MODO_BUSCAR_CERT_FECHA = "BUSCAR_CERT_FECHA"
MODO_BUSCAR_CERT_FUNDO = "BUSCAR_CERT_FUNDO"
MODO_BUSCAR_CERT_CORRE = "BUSCAR_CERT_CORRE"
MODO_BUSCAR_CERT_EMPRESA = "BUSCAR_CERT_EMPRESA"
MODO_DIR_EMPRESA = "DIR_EMPRESA"
MODO_DIR_FUNDO = "DIR_FUNDO"
MODO_BITACORA_ADD = "BITACORA_ADD"
MODO_BITACORA_SEARCH = "BITACORA_SEARCH"
