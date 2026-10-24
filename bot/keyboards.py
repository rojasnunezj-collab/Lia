# ====================================================================
# --- bot/keyboards.py: Teclados Persistentes y Atajos Rápidos ---
# ====================================================================
import time
from typing import Optional, Any
from telegram import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo

# Funciones auxiliares para obtener URLs de WebApps de forma segura y diferida (lazy)
def _get_url_cotizaciones(user_id=None):
    try:
        from core.cotizaciones_service import obtener_url_webapp
        return obtener_url_webapp(user_id=user_id)
    except Exception:
        return "https://lia-w2yb.onrender.com/cotizaciones"

def _get_url_certificados(user_id=None, modo=None):
    try:
        from core.certificados_service import obtener_url_webapp_certificados
        return obtener_url_webapp_certificados(user_id=user_id, modo=modo)
    except Exception:
        return "https://lia-w2yb.onrender.com/certificados"

def _get_url_flota(user_id=None):
    try:
        from core.flota_service import obtener_url_webapp_flota
        return obtener_url_webapp_flota('camiones', user_id=user_id)
    except Exception:
        return "https://lia-w2yb.onrender.com/flota"

def _get_url_pendientes(user_id=None):
    try:
        from core.pendientes_service import obtener_url_webapp_pendientes
        return obtener_url_webapp_pendientes(user_id=user_id)
    except Exception:
        return "https://lia-w2yb.onrender.com/pendientes"

def _get_url_credenciales(user_id=None):
    try:
        from core.credenciales_service import obtener_url_webapp_credenciales
        return obtener_url_webapp_credenciales(user_id=user_id)
    except Exception:
        return "https://lia-w2yb.onrender.com/credenciales"

def _get_url_valorizaciones(user_id=None):
    try:
        from core.valorizaciones_service import obtener_url_webapp_valorizaciones
        return obtener_url_webapp_valorizaciones(user_id=user_id)
    except Exception:
        return "https://lia-w2yb.onrender.com/valorizaciones"

# --------------------------------------------------------------------
# CONSTANTES DE BOTONES (Etiquetas abreviadas y compactas)
# --------------------------------------------------------------------
# Navegación Global
BTN_DOCS = "📑 Docs"
BTN_CERTS = "📜 Certs"
BTN_FLOTA = "🚚 Flota"
BTN_PENDIENTES = "📌 Pend."
BTN_BITACORA = "📓 Bitác."
BTN_BUSCAR = "🔍 Buscar"
BTN_CREDS = "🔐 Creds"
BTN_SALIR = "❌ Salir"
BTN_VOLVER_MENU = "🔙 Menú Ppal"
BTN_VOLVER_DOCS = "🔙 Docs"
BTN_CANCELAR = "❌ Cancelar"

# Documentos
BTN_GUIAS = "📘 Guías"
BTN_FACTURAS = "🧾 Fact."
BTN_COTIZ = "📋 Cotiz."
BTN_VALORIZ = "📊 Valoriz."

# Guías
BTN_GUIA_OCR = "📷 Leer OCR"
BTN_GUIA_REGISTRAR = "📁 Registrar"
BTN_GUIA_MANUAL = "✍️ Manual"
BTN_GUIA_COMENTAR = "💬 Comentar"

# Facturas
BTN_FACT_SUBIR = "📥 Subir Fact."
BTN_FACT_BUSCAR = "🔍 Buscar Fact."

# Cotizaciones
BTN_COTIZ_NUEVA = "➕ Nueva Cotiz."
BTN_COTIZ_ULTIMAS = "📋 5 Cotiz."
BTN_COTIZ_EMPRESA = "🏢 Por Empresa"
BTN_COTIZ_CORREL = "🔍 Por Correl."
BTN_COTIZ_IA = "🎙️ Dictar IA"

# Certificados
BTN_CERT_APP = "🚀 App Certs."
BTN_CERT_MODIF = "🔄 Modif. Exp."
BTN_CERT_BUSCAR = "🔍 Buscar Cert"

# Flota
BTN_FLOTA_APP = "📱 App Flota"
BTN_FLOTA_CAMIONES = "🚛 Camiones"
BTN_FLOTA_CHOFERES = "👨‍✈️ Choferes"
BTN_FLOTA_ADD_CAMION = "➕ Camión"
BTN_FLOTA_ADD_CHOFER = "➕ Chofer"

# Pendientes
BTN_PEND_NUEVO = "➕ Nuevo Pend."
BTN_PEND_ACTIVOS = "📋 Ver Activos"
BTN_PEND_APP = "📱 App Pend."

# Bitácora
BTN_BITA_ANOTAR = "📝 Anotar"
BTN_BITA_PREGUNTAR = "🔍 Preguntar IA"
BTN_BITA_ULTIMAS = "📋 5 Anotac."

# Búsqueda
BTN_BUSQ_GUIAS = "📊 Rep. Guías"
BTN_BUSQ_CLIENTES = "👥 Clientes"
BTN_BUSQ_DIR = "📍 Direcciones"

# Credenciales
BTN_CREDS_BUSCAR = "🔍 Buscar Cred."
BTN_CREDS_LISTAR = "📋 Ver Cuentas"
BTN_CREDS_APP = "🔐 Bóveda App"

# Valorizaciones
BTN_VALORIZ_APP = "🚀 App Valoriz."

# Conjunto de todos los botones de comando para interrupción limpia de estados
TODOS_LOS_BOTONES_MENU = {
    BTN_DOCS, BTN_CERTS, BTN_FLOTA, BTN_PENDIENTES, BTN_BITACORA, BTN_BUSCAR, BTN_CREDS, BTN_SALIR,
    BTN_VOLVER_MENU, BTN_VOLVER_DOCS, BTN_CANCELAR,
    BTN_GUIAS, BTN_FACTURAS, BTN_COTIZ, BTN_VALORIZ,
    BTN_GUIA_OCR, BTN_GUIA_REGISTRAR, BTN_GUIA_MANUAL, BTN_GUIA_COMENTAR,
    BTN_FACT_SUBIR, BTN_FACT_BUSCAR,
    BTN_COTIZ_NUEVA, BTN_COTIZ_ULTIMAS, BTN_COTIZ_EMPRESA, BTN_COTIZ_CORREL, BTN_COTIZ_IA,
    BTN_CERT_APP, BTN_CERT_MODIF, BTN_CERT_BUSCAR,
    BTN_FLOTA_APP, BTN_FLOTA_CAMIONES, BTN_FLOTA_CHOFERES, BTN_FLOTA_ADD_CAMION, BTN_FLOTA_ADD_CHOFER,
    BTN_PEND_NUEVO, BTN_PEND_ACTIVOS, BTN_PEND_APP,
    BTN_BITA_ANOTAR, BTN_BITA_PREGUNTAR, BTN_BITA_ULTIMAS,
    BTN_BUSQ_GUIAS, BTN_BUSQ_CLIENTES, BTN_BUSQ_DIR,
    BTN_CREDS_BUSCAR, BTN_CREDS_LISTAR, BTN_CREDS_APP,
    BTN_VALORIZ_APP
}


def _make_reply_kb(rows):
    """Construye un ReplyKeyboardMarkup compacto, responsivo y persistente."""
    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True
    )


# --------------------------------------------------------------------
# GENERADORES DE TECLADOS INFERIORES PERSISTENTES
# --------------------------------------------------------------------
def get_persistent_main_keyboard(user_id: Optional[Any] = None) -> ReplyKeyboardMarkup:
    """Menú Principal en la base del chat."""
    rows = [
        [KeyboardButton(BTN_DOCS), KeyboardButton(BTN_CERTS)],
        [KeyboardButton(BTN_FLOTA), KeyboardButton(BTN_PENDIENTES)],
        [KeyboardButton(BTN_BITACORA), KeyboardButton(BTN_BUSCAR)],
        [KeyboardButton(BTN_CREDS), KeyboardButton(BTN_SALIR)]
    ]
    return _make_reply_kb(rows)


def get_persistent_docs_keyboard() -> ReplyKeyboardMarkup:
    """Submenú de Gestión de Documentos."""
    rows = [
        [KeyboardButton(BTN_GUIAS), KeyboardButton(BTN_FACTURAS)],
        [KeyboardButton(BTN_COTIZ), KeyboardButton(BTN_VALORIZ)],
        [KeyboardButton(BTN_CERTS), KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_guias_keyboard() -> ReplyKeyboardMarkup:
    """Submenú de Guías de Remisión."""
    rows = [
        [KeyboardButton(BTN_GUIA_OCR), KeyboardButton(BTN_GUIA_REGISTRAR)],
        [KeyboardButton(BTN_GUIA_MANUAL), KeyboardButton(BTN_GUIA_COMENTAR)],
        [KeyboardButton(BTN_VOLVER_DOCS), KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_facturas_keyboard() -> ReplyKeyboardMarkup:
    """Submenú de Facturas SUNAT."""
    rows = [
        [KeyboardButton(BTN_FACT_SUBIR), KeyboardButton(BTN_FACT_BUSCAR)],
        [KeyboardButton(BTN_VOLVER_DOCS), KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_cotizaciones_keyboard(user_id: Optional[Any] = None) -> ReplyKeyboardMarkup:
    """Submenú de Cotizaciones."""
    url_app = _get_url_cotizaciones(user_id=user_id)
    btn_nueva = KeyboardButton(BTN_COTIZ_NUEVA, web_app=WebAppInfo(url=url_app)) if url_app else KeyboardButton(BTN_COTIZ_NUEVA)

    rows = [
        [btn_nueva, KeyboardButton(BTN_COTIZ_ULTIMAS)],
        [KeyboardButton(BTN_COTIZ_EMPRESA), KeyboardButton(BTN_COTIZ_CORREL)],
        [KeyboardButton(BTN_COTIZ_IA), KeyboardButton(BTN_VOLVER_DOCS)],
        [KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_certificados_keyboard(user_id: Optional[Any] = None) -> ReplyKeyboardMarkup:
    """Submenú de Certificados."""
    url_app = _get_url_certificados(user_id=user_id)
    btn_app = KeyboardButton(BTN_CERT_APP, web_app=WebAppInfo(url=url_app)) if url_app else KeyboardButton(BTN_CERT_APP)

    url_mod = _get_url_certificados(user_id=user_id, modo="modificar")
    btn_mod = KeyboardButton(BTN_CERT_MODIF, web_app=WebAppInfo(url=url_mod)) if url_mod else KeyboardButton(BTN_CERT_MODIF)

    rows = [
        [btn_app, btn_mod],
        [KeyboardButton(BTN_CERT_BUSCAR), KeyboardButton(BTN_VOLVER_DOCS)],
        [KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_flota_keyboard(user_id: Optional[Any] = None) -> ReplyKeyboardMarkup:
    """Submenú de Flota & Choferes."""
    url_app = _get_url_flota(user_id=user_id)
    btn_app = KeyboardButton(BTN_FLOTA_APP, web_app=WebAppInfo(url=url_app)) if url_app else KeyboardButton(BTN_FLOTA_APP)

    rows = [
        [btn_app, KeyboardButton(BTN_FLOTA_CAMIONES)],
        [KeyboardButton(BTN_FLOTA_CHOFERES), KeyboardButton(BTN_FLOTA_ADD_CAMION)],
        [KeyboardButton(BTN_FLOTA_ADD_CHOFER), KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_pendientes_keyboard(user_id: Optional[Any] = None) -> ReplyKeyboardMarkup:
    """Submenú de Pendientes & Alertas."""
    url_app = _get_url_pendientes(user_id=user_id)
    btn_app = KeyboardButton(BTN_PEND_APP, web_app=WebAppInfo(url=url_app)) if url_app else KeyboardButton(BTN_PEND_APP)

    rows = [
        [KeyboardButton(BTN_PEND_NUEVO), KeyboardButton(BTN_PEND_ACTIVOS)],
        [btn_app, KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_bitacora_keyboard() -> ReplyKeyboardMarkup:
    """Submenú de Bitácora Libre."""
    rows = [
        [KeyboardButton(BTN_BITA_ANOTAR), KeyboardButton(BTN_BITA_PREGUNTAR)],
        [KeyboardButton(BTN_BITA_ULTIMAS), KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_busqueda_keyboard() -> ReplyKeyboardMarkup:
    """Submenú del Centro de Búsqueda."""
    rows = [
        [KeyboardButton(BTN_BUSQ_GUIAS), KeyboardButton(BTN_BUSQ_CLIENTES)],
        [KeyboardButton(BTN_BUSQ_DIR), KeyboardButton(BTN_GUIA_COMENTAR)],
        [KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_credenciales_keyboard(user_id: Optional[Any] = None) -> ReplyKeyboardMarkup:
    """Submenú de Credenciales."""
    url_app = _get_url_credenciales(user_id=user_id)
    btn_app = KeyboardButton(BTN_CREDS_APP, web_app=WebAppInfo(url=url_app)) if url_app else KeyboardButton(BTN_CREDS_APP)

    rows = [
        [KeyboardButton(BTN_CREDS_BUSCAR), KeyboardButton(BTN_CREDS_LISTAR)],
        [btn_app, KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_valorizaciones_keyboard(user_id: Optional[Any] = None) -> ReplyKeyboardMarkup:
    """Submenú de Valorizaciones."""
    url_app = _get_url_valorizaciones(user_id=user_id)
    btn_app = KeyboardButton(BTN_VALORIZ_APP, web_app=WebAppInfo(url=url_app)) if url_app else KeyboardButton(BTN_VALORIZ_APP)

    rows = [
        [btn_app],
        [KeyboardButton(BTN_VOLVER_DOCS), KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)


def get_persistent_cancel_keyboard() -> ReplyKeyboardMarkup:
    """Teclado simple con botón para cancelar la operación actual."""
    rows = [
        [KeyboardButton(BTN_CANCELAR), KeyboardButton(BTN_VOLVER_MENU)]
    ]
    return _make_reply_kb(rows)
