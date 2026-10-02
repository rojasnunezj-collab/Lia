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
                    self.send_header('Cache-Control', 'no-cache')
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

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

    def log_message(self, format, *args):
        # Evitar saturar la consola de logs
        pass

def run_keep_alive():
    port = int(os.environ.get('PORT', 8080))
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

