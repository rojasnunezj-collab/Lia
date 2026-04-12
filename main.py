# ====================================================================
# --- IMPORTS Y CONFIGURACIÓN INICIAL ---
# ====================================================================
import os
import sys
import signal
import requests
from telegram.ext import Application, ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackQueryHandler

from config.settings import logger, KEY_FILE
from utils.helpers import init_db
from core.sheets_client import conectar_servicios
from bot.handlers import start, ping, button_handler, handle_text, handle_files, handle_callback_vinculacion

# ====================================================================
# --- INICIALIZACIÓN DE ENTORNO ---
# ====================================================================
if 'PYTHONANYWHERE_DOMAIN' in os.environ:
    proxy = "http://proxy.server:3128"
    os.environ['http_proxy'] = proxy
    os.environ['https_proxy'] = proxy
    os.environ['HTTP_PROXY'] = proxy
    os.environ['HTTPS_PROXY'] = proxy
    print("🌐 PythonAnywhere detectado: Proxy global configurado.")

try:
    if KEY_FILE and os.path.exists(KEY_FILE):
        conectar_servicios()
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass
        print(f"✅ SISTEMA v41 (MODULAR): Lía Operativa (BLINDAJE Placa y Productos).")
except Exception as e:
    logger.error(f"❌ Error crítico de infraestructura: {e}")

# ====================================================================
# --- HANDLERS DE SISTEMA (APAGADO / INICIO) ---
# ====================================================================
def hard_shutdown_handler(signum, frame):
    import time
    print("\n🛑 Señal de apagado detectada. Avisando a Telegram...")
    try:
        token = os.getenv("TELEGRAM_TOKEN")
        admin_id = os.getenv("ADMIN_CHAT_ID")
        if token and admin_id:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {"chat_id": admin_id, "text": "🛑 ALERTA: Servidor de Lía apagándose o consola cerrada. (PythonAnywhere / Local Offline)."}
            requests.post(url, json=payload, timeout=3)
            time.sleep(2) # Pausa para asegurar que el mensaje salga
    except Exception as e:
        print(f"No se pudo enviar aviso de apagado: {e}")
    sys.exit(0)

async def post_init(application: Application):
    """Mensaje de Arranque."""
    admin_id = os.getenv("ADMIN_CHAT_ID")
    if admin_id:
        try:
            await application.bot.send_message(chat_id=admin_id, text="🚀 Sistema Lía reiniciado y en línea.")
        except Exception as e:
            logger.error(f"No se pudo enviar msj de inicio: {e}")

# ====================================================================
# --- PUNTO DE ENTRADA PRINCIPAL ---
# ====================================================================
def main():
    try:
        signal.signal(signal.SIGINT, hard_shutdown_handler)
        signal.signal(signal.SIGTERM, hard_shutdown_handler)
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, hard_shutdown_handler)
    except Exception:
        pass

    init_db()
    token = os.getenv("TELEGRAM_TOKEN")
    
    app = ApplicationBuilder().token(token).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CallbackQueryHandler(handle_callback_vinculacion, pattern=r"^vinc\|"))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VOICE | filters.AUDIO | filters.VIDEO, handle_files))
    
    app.run_polling(stop_signals=())

if __name__ == '__main__':
    main()
