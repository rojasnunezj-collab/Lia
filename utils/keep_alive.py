import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from config.settings import logger

class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot Lia is alive and running!")
        
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        
    def log_message(self, format, *args):
        # Evitar llenar la consola de logs cada 5 minutos
        pass

def run_keep_alive():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), KeepAliveHandler)
    logger.info(f"Iniciando Keep-Alive server en puerto {port}...")
    server.serve_forever()

def start_keep_alive():
    """Inicia el servidor HTTP en un hilo en segundo plano para UptimeRobot."""
    keep_alive_thread = threading.Thread(target=run_keep_alive, daemon=True)
    keep_alive_thread.start()
