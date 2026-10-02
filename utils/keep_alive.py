import os
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from config.settings import logger

_clientes_cache = None
_clientes_cache_time = 0

class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ['/cotizaciones', '/cotizaciones.html']:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                html_path = os.path.join(base_dir, 'webapp', 'cotizaciones.html')
                if os.path.exists(html_path):
                    with open(html_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
                    self.send_header('Pragma', 'no-cache')
                    self.send_header('Expires', '0')
                    self.end_headers()
                    self.wfile.write(content.encode('utf-8'))
                    return
            except Exception as e:
                logger.error(f"Error sirviendo /cotizaciones: {e}")

        elif path == '/api/clientes':
            try:
                from core.cotizaciones_service import obtener_catalogo_clientes
                import time
                global _clientes_cache, _clientes_cache_time
                if not _clientes_cache or (time.time() - _clientes_cache_time > 300):
                    _clientes_cache = obtener_catalogo_clientes()
                    _clientes_cache_time = time.time()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(_clientes_cache).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en /api/clientes: {e}")

        # Default Health Check para UptimeRobot y Render
        self.send_response(200)
        self.send_header('Content-type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(b"Bot Lia is alive and running!")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == '/api/cotizacion':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.cotizaciones_service import procesar_generacion_cotizacion, obtener_url_webapp
                import requests
                from config.settings import TELEGRAM_TOKEN, ADMIN_CHAT_ID

                res = procesar_generacion_cotizacion(payload)

                target_chat_id = payload.get('user_id') or ADMIN_CHAT_ID
                if target_chat_id and TELEGRAM_TOKEN:
                    url_edit = obtener_url_webapp(correlativo=res['correlativo'], datos_edicion=res['datos_json'], user_id=target_chat_id)
                    import html
                    c_cliente = html.escape(str(res['cliente']))
                    c_codigo = html.escape(str(res['codigo']))
                    c_fecha = html.escape(str(res['fecha']))
                    caption = (
                        f"✅ <b>Cotización Generada Exitosamente</b>\n\n"
                        f"📌 <b>Código:</b> <code>COTIZACION N°{c_codigo}</code>\n"
                        f"🏢 <b>Cliente:</b> <code>{c_cliente}</code>\n"
                        f"📅 <b>Fecha:</b> <code>{c_fecha}</code>\n"
                        f"💰 <b>Ítems cotizados:</b> {len(payload.get('items', []))} residuos\n\n"
                        f"💾 Guardada en Google Drive y registrada en Sheets."
                    )
                    files = {'document': (res['nombre_archivo'], res['pdf_bytes'], 'application/pdf')}
                    data = {
                        'chat_id': str(target_chat_id),
                        'caption': caption,
                        'parse_mode': 'HTML',
                        'reply_markup': json.dumps({
                            'inline_keyboard': [
                                [{'text': '✏️ Modificar Cotización', 'web_app': {'url': url_edit}}],
                                [{'text': '📄 Doc Editable', 'url': res['doc_link']}, {'text': '📂 Ver en Drive', 'url': res['pdf_link']}],
                                [{'text': '📋 Menú Cotizaciones', 'callback_data': 'menu_cotizaciones'}]
                            ]
                        })
                    }
                    try:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument", data=data, files=files, timeout=30)
                    except Exception as e:
                        logger.error(f"Error enviando documento por Telegram Bot API: {e}")

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'codigo': res['codigo']}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/cotizacion: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

    def log_message(self, format, *args):
        # Evitar saturar la consola de logs
        pass

def run_keep_alive():
    port = int(os.environ.get('PORT', 10000))
    try:
        server = HTTPServer(('0.0.0.0', port), KeepAliveHandler)
        logger.info(f"Iniciando Keep-Alive server en puerto {port}...")
        server.serve_forever()
    except Exception as e:
        logger.warning(f"No se pudo iniciar Keep-Alive server: {e}")

def start_keep_alive():
    """Inicia el servidor HTTP en un hilo en segundo plano para UptimeRobot y Mini App."""
    keep_alive_thread = threading.Thread(target=run_keep_alive, daemon=True)
    keep_alive_thread.start()

