import os
import json
import time
import hmac
import hashlib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, parse_qsl
from config.settings import logger, TELEGRAM_TOKEN, es_usuario_autorizado, ALLOWED_USER_IDS
from utils.helpers import liberar_memoria_proceso

MAX_CONTENT_LENGTH = 35 * 1024 * 1024  # 35 MB máximo para solicitudes HTTP

import collections

class BruteForceProtector:
    """
    Protección activa contra ataques de fuerza bruta, escaneo automatizado y saturación (DDoS/Flooding).
    - Bloquea direcciones IP tras acumular 5 intentos fallidos de autenticación en 5 minutos.
    - Aplica un ban temporal de 15 minutos (900 segundos).
    - Limita la tasa general a un máximo de 60 peticiones por minuto por IP (Rate Limiter).
    """
    def __init__(self, max_fallos=5, ventana_bloqueo=900, max_rpm=60):
        self.lock = threading.Lock()
        self.max_fallos = max_fallos
        self.ventana_bloqueo = ventana_bloqueo
        self.max_rpm = max_rpm
        self.fallos_ip = collections.defaultdict(list)
        self.peticiones_ip = collections.defaultdict(list)
        self.bloqueados = {}

    def esta_bloqueado(self, ip: str) -> tuple[bool, int]:
        ahora = time.time()
        with self.lock:
            # 1. Verificar si la IP ya se encuentra en lista negra temporal
            if ip in self.bloqueados:
                tiempo_restante = int(self.bloqueados[ip] - ahora)
                if tiempo_restante > 0:
                    return True, tiempo_restante
                else:
                    del self.bloqueados[ip]
                    self.fallos_ip.pop(ip, None)

            # 2. Rate Limiting general por IP
            pts = [t for t in self.peticiones_ip.get(ip, []) if ahora - t < 60]
            if len(pts) >= self.max_rpm:
                self.bloqueados[ip] = ahora + 120  # Cooldown de 2 minutos
                logger.warning(f"🚨 Rate Limit excedido para IP {ip}: Bloqueada temporalmente 120s.")
                return True, 120
            pts.append(ahora)
            self.peticiones_ip[ip] = pts
            return False, 0

    def registrar_fallo(self, ip: str):
        if not ip:
            return
        ahora = time.time()
        with self.lock:
            tiempos = [t for t in self.fallos_ip[ip] if ahora - t < 300]
            tiempos.append(ahora)
            self.fallos_ip[ip] = tiempos
            if len(tiempos) >= self.max_fallos:
                self.bloqueados[ip] = ahora + self.ventana_bloqueo
                logger.warning(f"🚨 ALERTA BRUTE FORCE: IP {ip} BLOQUEADA por {self.ventana_bloqueo}s ({len(tiempos)} intentos fallidos).")

    def registrar_exito(self, ip: str):
        if not ip:
            return
        with self.lock:
            self.fallos_ip.pop(ip, None)

protector_fuerza_bruta = BruteForceProtector()

def obtener_ip_cliente(handler) -> str:
    """Extrae la IP real del cliente considerando proxies inversos (Render / Cloudflare)."""
    xff = handler.headers.get('X-Forwarded-For')
    if xff:
        return xff.split(',')[0].strip()
    return handler.client_address[0]


def validar_telegram_init_data(init_data: str, bot_token: str) -> dict | None:
    """
    Valida criptográficamente el hash de autenticación emitido por Telegram WebApp
    usando HMAC-SHA256 y la clave secreta derivada del TELEGRAM_TOKEN.
    """
    if not init_data or not bot_token:
        return None
    try:
        parsed_data = dict(parse_qsl(init_data, keep_blank_values=True))
        hash_recibido = parsed_data.pop('hash', None)
        if not hash_recibido:
            return None
        
        # Ordenar parámetros alfabéticamente
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode('utf-8'), hashlib.sha256).digest()
        calculado = hmac.new(secret_key, data_check_string.encode('utf-8'), hashlib.sha256).hexdigest()
        
        if hmac.compare_digest(calculado, hash_recibido):
            raw_user = parsed_data.get('user')
            if raw_user:
                return json.loads(raw_user)
            return {'id': parsed_data.get('user_id')}
    except Exception as e:
        logger.warning(f"Error validando Telegram initData: {e}")
    return None

def autenticar_peticion_webapp(headers, parsed_query, accion_critica: bool = False, client_ip: str = ""):
    """
    Verifica si una solicitud HTTP proviene de una Telegram Mini App legítima y autorizada.
    Retorna (autorizado: bool, user_id: int | None, error_msg: str | None)
    """
    init_data = (
        headers.get('X-Telegram-Init-Data') or
        headers.get('Authorization', '').replace('Bearer ', '') or
        parsed_query.get('init_data', [None])[0] or
        parsed_query.get('tgWebAppData', [None])[0]
    )

    if init_data and TELEGRAM_TOKEN:
        user_info = validar_telegram_init_data(init_data, TELEGRAM_TOKEN)
        if user_info and 'id' in user_info:
            uid = user_info['id']
            if es_usuario_autorizado(uid, accion_critica=accion_critica):
                return True, uid, None
            return False, uid, "Usuario autenticado pero no autorizado para esta acción."

    # Clave de API de respaldo si se define en el entorno para llamadas automatizadas/debug
    api_key_env = os.getenv("API_SECRET_KEY")
    req_api_key = headers.get('X-API-Key')
    if api_key_env and req_api_key and hmac.compare_digest(api_key_env, req_api_key):
        return True, 0, None

    # Fallback si no hay init_data
    if accion_critica:
        return False, None, "Acceso no autorizado: Se requiere autenticación criptográfica desde Telegram Mini App."

    # Para acciones no críticas, verificar si el uid proporcionado está en ALLOWED_USER_IDS si está configurado
    uid_param = parsed_query.get('uid', [None])[0]
    if ALLOWED_USER_IDS:
        if uid_param and es_usuario_autorizado(uid_param, accion_critica=False):
            return True, int(uid_param), None
        return False, None, "Acceso no autorizado: Usuario no listado en ALLOWED_USERS."

    return True, None, None

_clientes_cache = None
_clientes_cache_time = 0

_certificados_lock = threading.Lock()
_operaciones_activas = {}
TIEMPO_EXPIRACION_LOCK_SEG = 60

def adquirir_candado_certificado(clave: str) -> bool:
    """
    Intenta adquirir un candado para una operación (ej. 'emitir:218', 'juntar:218').
    Retorna True si se adquirió con éxito, False si ya hay una operación en curso.
    Limpia automáticamente candados antiguos que excedan TIEMPO_EXPIRACION_LOCK_SEG.
    """
    ahora = time.time()
    with _certificados_lock:
        expirados = [k for k, t in _operaciones_activas.items() if ahora - t > TIEMPO_EXPIRACION_LOCK_SEG]
        for k in expirados:
            _operaciones_activas.pop(k, None)

        if clave in _operaciones_activas:
            return False
        _operaciones_activas[clave] = ahora
        return True

def liberar_candado_certificado(clave: str):
    """Libera el candado de la operación especificada."""
    with _certificados_lock:
        _operaciones_activas.pop(clave, None)

class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        client_ip = obtener_ip_cliente(self)
        bloqueado, tiempo_espera = protector_fuerza_bruta.esta_bloqueado(client_ip)
        if bloqueado:
            self.send_response(429)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Retry-After', str(tiempo_espera))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': f'🚨 Solicitudes bloqueadas por seguridad. Demasiadas peticiones o intentos fallidos desde esta IP. Intenta en {tiempo_espera}s.'
            }).encode('utf-8'))
            return

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

        elif path in ['/pendientes', '/pendientes.html']:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                html_path = os.path.join(base_dir, 'webapp', 'pendientes.html')
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
                logger.error(f"Error sirviendo /pendientes: {e}")

        elif path in ['/credenciales', '/credenciales.html']:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                html_path = os.path.join(base_dir, 'webapp', 'credenciales.html')
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
                logger.error(f"Error sirviendo /credenciales: {e}")

        elif path in ['/panel', '/panel.html']:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                html_path = os.path.join(base_dir, 'webapp', 'panel.html')
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
                logger.error(f"Error sirviendo /panel: {e}")

        elif path in ['/certificados', '/certificados.html']:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                html_path = os.path.join(base_dir, 'webapp', 'certificados.html')
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
                else:
                    self.send_response(404)
                    self.send_header('Content-Type', 'text/plain; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(b"Vista /certificados aun no creada.")
                    return
            except Exception as e:
                logger.error(f"Error sirviendo /certificados: {e}")

        elif path in ['/flota', '/flota.html']:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                html_path = os.path.join(base_dir, 'webapp', 'flota.html')
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
                else:
                    self.send_response(404)
                    self.send_header('Content-Type', 'text/plain; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(b"Vista /flota aun no creada.")
                    return
            except Exception as e:
                logger.error(f"Error sirviendo /flota: {e}")

        elif path == '/api/flota/camiones':
            try:
                from core.flota_service import listar_camiones
                camiones = listar_camiones()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'camiones': camiones}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/flota/camiones: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/flota/conductores':
            try:
                from core.flota_service import listar_conductores
                conductores = listar_conductores()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'conductores': conductores}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/flota/conductores: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/certificados/catalogos':
            try:
                from core.certificados_service import (
                    obtener_catalogo_empresas, 
                    obtener_catalogo_servicios, 
                    obtener_siguiente_correlativo_cert,
                    obtener_catalogo_clientes_certificados,
                    obtener_catalogo_direcciones_certificados
                )
                empresas = obtener_catalogo_empresas()
                servicios = obtener_catalogo_servicios()
                clientes = obtener_catalogo_clientes_certificados()
                direcciones = obtener_catalogo_direcciones_certificados()
                corr_com = obtener_siguiente_correlativo_cert("Comercialización")
                corr_ser = obtener_siguiente_correlativo_cert("Disposición Final 1")
                data = {
                    "empresas": empresas,
                    "servicios": servicios,
                    "clientes": clientes,
                    "direcciones": direcciones,
                    "siguiente_correlativo_com": corr_com,
                    "siguiente_correlativo_ser": corr_ser
                }
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/certificados/catalogos: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/certificados/correlativo':
            try:
                from core.certificados_service import obtener_siguiente_correlativo_cert
                q_params = parse_qs(parsed.query)
                tipo = q_params.get('tipo', ['Comercialización'])[0]
                corr = obtener_siguiente_correlativo_cert(tipo)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'correlativo': corr}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/certificados/correlativo: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/certificados/buscar':
            try:
                from core.certificados_service import buscar_datos_certificado_en_historial
                q_params = parse_qs(parsed.query)
                corr = q_params.get('corr', [''])[0]
                res = buscar_datos_certificado_en_historial(corr)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'resultados': res}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/certificados/buscar: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/certificados/repositorio':
            try:
                from core.certificados_service import obtener_guias_pendientes_repositorio
                guias = obtener_guias_pendientes_repositorio()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'guias': guias}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/certificados/repositorio: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/pendientes':
            try:
                from core.pendientes_service import obtener_pendientes_sync
                data = obtener_pendientes_sync()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/pendientes: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/credenciales':
            try:
                qs = parse_qs(parsed.query)
                ok, uid, err_msg = autenticar_peticion_webapp(self.headers, qs, accion_critica=True, client_ip=client_ip)
                if not ok: protector_fuerza_bruta.registrar_fallo(client_ip)
                else: protector_fuerza_bruta.registrar_exito(client_ip)
                if not ok:
                    self.send_response(403)
                    self.send_header('Content-Type', 'application/json; charset=utf-8')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': err_msg or 'Acceso no autorizado a la bóveda de credenciales.'}).encode('utf-8'))
                    return

                from core.credenciales_service import obtener_credenciales_sync
                data = obtener_credenciales_sync()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/credenciales: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

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

        elif path == '/api/cotizaciones':
            try:
                from core.cotizaciones_service import obtener_todas_cotizaciones
                q_params = parse_qs(parsed.query)
                force = q_params.get('force', ['0'])[0] == '1'
                q_empresa = q_params.get('q', [''])[0]
                data = obtener_todas_cotizaciones(filtro_empresa=q_empresa or None, force_refresh=force)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Cache-Control', 'no-cache')
                self.end_headers()
                self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/cotizaciones: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path in ['/valorizaciones', '/valorizaciones.html']:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                html_path = os.path.join(base_dir, 'webapp', 'valorizaciones.html')
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
                logger.error(f"Error sirviendo /valorizaciones: {e}")

        elif path == '/api/valorizaciones/pestanas':
            try:
                from core.valorizaciones_service import listar_pestanas_valorizacion
                pestanas = listar_pestanas_valorizacion()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'pestanas': pestanas}, ensure_ascii=False).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/valorizaciones/pestanas: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/valorizaciones/detalle':
            try:
                from core.valorizaciones_service import obtener_detalle_pestana
                q_params = parse_qs(parsed.query)
                tab = q_params.get('tab', [''])[0]
                detalle = obtener_detalle_pestana(tab)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'detalle': detalle}, ensure_ascii=False).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en GET /api/valorizaciones/detalle: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                return

        # Default Health Check para UptimeRobot y Render
        self.send_response(200)
        self.send_header('Content-type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(b"Bot Lia is alive and running!")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Telegram-Init-Data, Authorization, X-API-Key')
        self.end_headers()

    def do_POST(self):
        client_ip = obtener_ip_cliente(self)
        bloqueado, tiempo_espera = protector_fuerza_bruta.esta_bloqueado(client_ip)
        if bloqueado:
            self.send_response(429)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Retry-After', str(tiempo_espera))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': f'🚨 Solicitudes bloqueadas por seguridad. Demasiadas peticiones o intentos fallidos desde esta IP. Intenta en {tiempo_espera}s.'
            }).encode('utf-8'))
            return

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
                    c_tipo = html.escape(str(res.get('tipo_nombre', 'Comercialización')))
                    total_items = res.get('total_items', len(payload.get('items', [])))
                    caption = (
                        f"✅ <b>Cotización Generada Exitosamente</b>\n\n"
                        f"📌 <b>Código:</b> <code>COTIZACION N°{c_codigo}</code>\n"
                        f"📋 <b>Plantilla:</b> <code>{c_tipo}</code>\n"
                        f"🏢 <b>Cliente:</b> <code>{c_cliente}</code>\n"
                        f"📅 <b>Fecha:</b> <code>{c_fecha}</code>\n"
                        f"💰 <b>Ítems cotizados:</b> {total_items} ítems\n\n"
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
                                [{'text': '🔄 Sincronizar PDF desde Doc', 'callback_data': f"coti_sync|{res['correlativo']}"}],
                                [{'text': '📋 Menú Cotizaciones', 'callback_data': 'menu_cotizaciones'}, {'text': '❌ Cancelar', 'callback_data': 'cancelar_start'}]
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

        elif path == '/api/pendientes':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.pendientes_service import crear_pendiente_sync
                pnd_id = crear_pendiente_sync(payload)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'id': pnd_id}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/pendientes: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/pendientes/accion':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                pnd_id = payload.get('id')
                accion = payload.get('accion')
                from core.pendientes_service import actualizar_estado_pendiente_sync, posponer_pendiente_sync

                if accion == 'completar':
                    ok = actualizar_estado_pendiente_sync(pnd_id, 'COMPLETADO')
                elif accion == 'posponer':
                    minutos = int(payload.get('minutos', 60))
                    res_posponer = posponer_pendiente_sync(pnd_id, minutos)
                    ok = res_posponer is not None
                else:
                    ok = False

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': ok}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/pendientes/accion: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/credenciales':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                if content_len > MAX_CONTENT_LENGTH:
                    self.send_response(413)
                    self.end_headers()
                    return
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                qs = parse_qs(parsed.query)
                ok, uid, err_msg = autenticar_peticion_webapp(self.headers, qs, accion_critica=True, client_ip=client_ip)
                if not ok: protector_fuerza_bruta.registrar_fallo(client_ip)
                else: protector_fuerza_bruta.registrar_exito(client_ip)
                if not ok:
                    self.send_response(403)
                    self.send_header('Content-Type', 'application/json; charset=utf-8')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({'success': False, 'error': err_msg or 'No autorizado para modificar credenciales.'}).encode('utf-8'))
                    return

                from core.credenciales_service import guardar_credencial_sync
                crd_id = guardar_credencial_sync(payload)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'id': crd_id}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/credenciales: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/certificados/ocr':
            post_body = None
            payload = None
            pdf_bytes = None
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                pdf_base64 = payload.get('pdf_base64')
                if not pdf_base64:
                    raise ValueError("No se recibió 'pdf_base64' en la petición.")

                import base64
                if ',' in pdf_base64:
                    pdf_base64 = pdf_base64.split(',', 1)[1]
                pdf_bytes = base64.b64decode(pdf_base64)

                from core.certificados_service import procesar_guia_ia_vertex
                datos_extraidos = procesar_guia_ia_vertex(pdf_bytes)

                if not datos_extraidos:
                    self.send_response(422)
                    self.send_header('Content-Type', 'application/json; charset=utf-8')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({'success': False, 'error': 'No se pudo extraer información del documento con IA.'}).encode('utf-8'))
                    return

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'datos': datos_extraidos}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/certificados/ocr: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return
            finally:
                del post_body, payload, pdf_bytes
                liberar_memoria_proceso()

        elif path == '/api/certificados/consolidar_repo':
            post_body = None
            payload = None
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))
                guias = payload.get('guias', [])
                if not guias:
                    raise ValueError("No se enviaron guías para consolidar.")

                from core.certificados_service import consolidar_guias_repositorio_ocr
                res = consolidar_guias_repositorio_ocr(guias)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(res).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/certificados/consolidar_repo: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return
            finally:
                del post_body, payload
                liberar_memoria_proceso()

        elif path == '/api/certificados/emitir':
            content_len = int(self.headers.get('Content-Length', 0))
            post_body = self.rfile.read(content_len)
            payload = json.loads(post_body.decode('utf-8'))

            corr = str(payload.get('correlativo', '')).strip()
            flujo = str(payload.get('tipo_flujo', '')).strip()
            candado_key = f"emitir:{flujo}:{corr}" if corr else f"emitir:{time.time()}"
            if not adquirir_candado_certificado(candado_key):
                logger.warning(f"Intento de emisión concurrente duplicada bloqueado: {candado_key}")
                self.send_response(409)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'success': False,
                    'error': f"⏳ Ya hay una emisión en curso para el certificado #{corr}. Por favor espera unos momentos."
                }).encode('utf-8'))
                return

            try:
                import base64
                remision_bytes = None
                transporte_bytes = None
                if payload.get('remision_base64'):
                    raw_rem = payload['remision_base64']
                    if ',' in raw_rem: raw_rem = raw_rem.split(',', 1)[1]
                    remision_bytes = base64.b64decode(raw_rem)

                if payload.get('transporte_base64'):
                    raw_trans = payload['transporte_base64']
                    if ',' in raw_trans: raw_trans = raw_trans.split(',', 1)[1]
                    transporte_bytes = base64.b64decode(raw_trans)

                from core.certificados_service import procesar_generacion_certificado, obtener_url_webapp_certificados
                res = procesar_generacion_certificado(payload, remision_bytes=remision_bytes, transporte_bytes=transporte_bytes)

                # Envío automático del PDF al chat de Telegram
                target_chat_id = payload.get('user_id') or os.getenv("ADMIN_CHAT_ID")
                token = os.getenv("TELEGRAM_TOKEN")
                if target_chat_id and token:
                    import requests
                    import html
                    url_edit = obtener_url_webapp_certificados(correlativo=res['correlativo'], user_id=target_chat_id, modo="modificar")
                    c_cli = html.escape(str(res['cliente']))
                    c_corr = html.escape(str(res['correlativo']))
                    c_fec = html.escape(str(res['fecha']))
                    c_nom = html.escape(str(res['nombre_archivo']))
                    caption = (
                        f"✅ <b>Certificado Emitido Exitosamente</b>\n\n"
                        f"📌 <b>Código:</b> <code>{c_nom}</code>\n"
                        f"🔢 <b>Correlativo:</b> <code>{c_corr}</code>\n"
                        f"🏢 <b>Cliente:</b> <code>{c_cli}</code>\n"
                        f"📅 <b>Fecha:</b> <code>{c_fec}</code>\n\n"
                        f"💾 Guardado en Google Drive y registrado en la hoja Historial."
                    )
                    kb_buttons = []
                    row1 = []
                    if res.get('doc_link'):
                        row1.append({'text': '📄 Word Editable', 'url': res['doc_link']})
                    if res.get('pdf_link'):
                        row1.append({'text': '📂 Ver en Drive', 'url': res['pdf_link']})
                    if row1:
                        kb_buttons.append(row1)
                    kb_buttons.append([{'text': '✏️ Modificar en Mini App', 'web_app': {'url': url_edit}}])
                    kb_buttons.append([{'text': '📋 Menú Certificados', 'callback_data': 'menu_certificados'}, {'text': '❌ Salir', 'callback_data': 'cancelar_start'}])

                    files = {'document': (res['nombre_archivo'], res['pdf_bytes'], 'application/pdf')}
                    data_tg = {
                        'chat_id': str(target_chat_id),
                        'caption': caption,
                        'parse_mode': 'HTML',
                        'reply_markup': json.dumps({'inline_keyboard': kb_buttons})
                    }
                    try:
                        requests.post(f"https://api.telegram.org/bot{token}/sendDocument", data=data_tg, files=files, timeout=30)
                    except Exception as e_tg:
                        logger.error(f"Error enviando certificado por Telegram: {e_tg}")
                    finally:
                        del files, data_tg

                # Respuesta segura JSON para la Mini App
                res_safe = {k: v for k, v in res.items() if k != 'pdf_bytes'}
                if isinstance(res, dict):
                    res.pop('pdf_bytes', None)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'res': res_safe}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/certificados/emitir: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return
            finally:
                liberar_candado_certificado(candado_key)
                del post_body, payload
                liberar_memoria_proceso()

        elif path == '/api/certificados/regenerar':
            content_len = int(self.headers.get('Content-Length', 0))
            post_body = self.rfile.read(content_len)
            payload = json.loads(post_body.decode('utf-8'))

            corr = payload.get('correlativo')
            if not corr:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': "Falta 'correlativo' en la solicitud."}).encode('utf-8'))
                return

            candado_key = f"regenerar:{corr}"
            if not adquirir_candado_certificado(candado_key):
                logger.warning(f"Intento de regeneración concurrente duplicada bloqueado: {candado_key}")
                self.send_response(409)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'success': False,
                    'error': f"⏳ Ya hay una regeneración en curso para el certificado #{corr}. Por favor espera unos momentos."
                }).encode('utf-8'))
                return

            try:
                import base64
                nuevo_doc_bytes = None
                nuevo_pdf_bytes = None
                if payload.get('nuevo_doc_base64'):
                    raw_doc = payload['nuevo_doc_base64']
                    if ',' in raw_doc: raw_doc = raw_doc.split(',', 1)[1]
                    nuevo_doc_bytes = base64.b64decode(raw_doc)
                if payload.get('nuevo_pdf_base64'):
                    raw_pdf = payload['nuevo_pdf_base64']
                    if ',' in raw_pdf: raw_pdf = raw_pdf.split(',', 1)[1]
                    nuevo_pdf_bytes = base64.b64decode(raw_pdf)

                from core.certificados_service import procesar_regeneracion_expediente
                res = procesar_regeneracion_expediente(
                    correlativo=corr,
                    nuevo_doc_bytes=nuevo_doc_bytes,
                    nuevo_pdf_bytes=nuevo_pdf_bytes,
                    nombre_pdf_usuario=payload.get('nombre_pdf', ''),
                    obs_extra=payload.get('obs', ''),
                    usuario_editor=payload.get('usuario_email') or payload.get('usuario') or 'Usuario MiniApp'
                )

                # Si hay usuario o admin, notificar por Telegram
                target_chat_id = payload.get('user_id') or os.getenv("ADMIN_CHAT_ID")
                token = os.getenv("TELEGRAM_TOKEN")
                if target_chat_id and token:
                    import requests
                    import html
                    c_corr = html.escape(str(corr))
                    c_nom = html.escape(str(res['nombre_archivo']))
                    caption = (
                        f"🔄 <b>Expediente Regenerado Quirúrgicamente</b>\n\n"
                        f"📌 <b>Archivo:</b> <code>{c_nom}</code>\n"
                        f"🔢 <b>Correlativo:</b> <code>{c_corr}</code>\n"
                        f"📑 <i>Las guías originales se preservaron intactas.</i>\n\n"
                        f"💾 Enlace de Google Drive actualizado in-place."
                    )
                    kb_buttons = [
                        [{'text': '📂 Ver PDF en Drive', 'url': res['pdf_link']}],
                        [{'text': '📋 Menú Certificados', 'callback_data': 'menu_certificados'}, {'text': '❌ Salir', 'callback_data': 'cancelar_start'}]
                    ]
                    files = {'document': (res['nombre_archivo'], res['pdf_bytes'], 'application/pdf')}
                    data_tg = {
                        'chat_id': str(target_chat_id),
                        'caption': caption,
                        'parse_mode': 'HTML',
                        'reply_markup': json.dumps({'inline_keyboard': kb_buttons})
                    }
                    try:
                        requests.post(f"https://api.telegram.org/bot{token}/sendDocument", data=data_tg, files=files, timeout=30)
                    except Exception as e_tg:
                        logger.error(f"Error enviando actualización por Telegram: {e_tg}")
                    finally:
                        del files, data_tg

                res_safe = {k: v for k, v in res.items() if k != 'pdf_bytes'}
                if isinstance(res, dict):
                    res.pop('pdf_bytes', None)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'res': res_safe}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/certificados/regenerar: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return
            finally:
                liberar_candado_certificado(candado_key)
                del post_body, payload
                liberar_memoria_proceso()

        elif path == '/api/certificados/juntar_expediente':
            content_len = int(self.headers.get('Content-Length', 0))
            post_body = self.rfile.read(content_len)
            payload = json.loads(post_body.decode('utf-8'))

            corr = payload.get('correlativo')
            if not corr:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': "Falta 'correlativo' en la solicitud."}).encode('utf-8'))
                return

            candado_key = f"juntar:{corr}"
            if not adquirir_candado_certificado(candado_key):
                logger.warning(f"Intento de unificación concurrente duplicada bloqueado: {candado_key}")
                self.send_response(409)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'success': False,
                    'error': f"⏳ Ya hay una unificación de expediente en curso para el certificado #{corr}. Por favor espera unos momentos."
                }).encode('utf-8'))
                return

            try:
                guias = payload.get('guias') or []
                usuario_email = payload.get('usuario_email') or payload.get('usuario') or 'Usuario MiniApp'

                from core.certificados_service import juntar_expediente_completo
                res = juntar_expediente_completo(
                    correlativo=corr,
                    guias_lista=guias,
                    usuario_editor=usuario_email
                )

                # Notificar y enviar documento consolidado por Telegram
                target_chat_id = payload.get('user_id') or os.getenv("ADMIN_CHAT_ID")
                token = os.getenv("TELEGRAM_TOKEN")
                if target_chat_id and token:
                    import requests
                    import html
                    c_corr = html.escape(str(corr))
                    c_nom = html.escape(str(res['nombre_archivo']))
                    caption = (
                        f"📑 <b>Expediente Unificado con Guías</b>\n\n"
                        f"📌 <b>Archivo:</b> <code>{c_nom}</code>\n"
                        f"🔢 <b>Correlativo:</b> <code>{c_corr}</code>\n"
                        f"📄 <b>Guías Remisión unidas:</b> {res['remisiones_unidas']}\n"
                        f"🚛 <b>Guías Transporte unidas:</b> {res['transportes_unidos']}\n"
                        f"📑 <b>Total Páginas:</b> {res['total_paginas']}\n\n"
                        f"💾 PDF consolidado actualizado en Google Drive."
                    )
                    kb_buttons = [
                        [{'text': '📂 Ver PDF en Drive', 'url': res['pdf_link']}],
                        [{'text': '📋 Menú Certificados', 'callback_data': 'menu_certificados'}, {'text': '❌ Salir', 'callback_data': 'cancelar_start'}]
                    ]
                    files = {'document': (res['nombre_archivo'], res['pdf_bytes'], 'application/pdf')}
                    data_tg = {
                        'chat_id': str(target_chat_id),
                        'caption': caption,
                        'parse_mode': 'HTML',
                        'reply_markup': json.dumps({'inline_keyboard': kb_buttons})
                    }
                    try:
                        requests.post(f"https://api.telegram.org/bot{token}/sendDocument", data=data_tg, files=files, timeout=30)
                    except Exception as e_tg:
                        logger.error(f"Error enviando expediente unificado por Telegram: {e_tg}")
                    finally:
                        del files, data_tg

                res_safe = {k: v for k, v in res.items() if k != 'pdf_bytes'}
                if isinstance(res, dict):
                    res.pop('pdf_bytes', None)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'res': res_safe}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/certificados/juntar_expediente: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return
            finally:
                liberar_candado_certificado(candado_key)
                del post_body, payload
                liberar_memoria_proceso()

        elif path == '/api/flota/camiones/guardar':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.flota_service import guardar_camion
                camion = guardar_camion(payload)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'camion': camion}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/flota/camiones/guardar: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/flota/camiones/eliminar':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.flota_service import eliminar_camion
                criterio = payload.get('id') or payload.get('placa')
                ok = eliminar_camion(criterio)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': ok}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/flota/camiones/eliminar: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/flota/conductores/guardar':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.flota_service import guardar_conductor
                conductor = guardar_conductor(payload)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True, 'conductor': conductor}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/flota/conductores/guardar: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/flota/conductores/eliminar':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.flota_service import eliminar_conductor
                criterio = payload.get('id') or payload.get('dni') or payload.get('licencia')
                ok = eliminar_conductor(criterio)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': ok}).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/flota/conductores/eliminar: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/valorizaciones/guardar':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.valorizaciones_service import guardar_valorizacion
                res = guardar_valorizacion(payload)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(res, ensure_ascii=False).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/valorizaciones/guardar: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                return

        elif path == '/api/valorizaciones/pdf':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                from core.valorizaciones_service import exportar_valorizacion_pdf, formatear_nombre_pdf
                from config.settings import TELEGRAM_TOKEN, ADMIN_CHAT_ID
                import html

                gid = payload.get('gid')
                pestana = payload.get('nombre_pestana') or gid
                cliente = payload.get('cliente', '')
                liq = payload.get('liquidacion', '')
                target_user = payload.get('user_id') or ADMIN_CHAT_ID

                nombre_archivo = formatear_nombre_pdf(pestana)
                res = exportar_valorizacion_pdf(pestana, nombre_archivo=nombre_archivo, datos_pantalla=payload)

                if res.get('success') and target_user and TELEGRAM_TOKEN:
                    import requests
                    c_cliente = html.escape(str(cliente))
                    c_liq = html.escape(str(liq))
                    c_pestana = html.escape(str(pestana))
                    caption = (
                        f"📊 <b>Valorización de Servicios Generada</b>\n\n"
                        f"📑 <b>Formato:</b> <code>LIQ-F-001 (Versión 1.0)</code>\n"
                        f"🏢 <b>Cliente:</b> <code>{c_cliente}</code>\n"
                        f"🔢 <b>Liquidación:</b> <code>N° {c_liq}</code>\n"
                        f"📋 <b>Pestaña:</b> <code>{c_pestana}</code>\n\n"
                        f"📁 Guardado en carpeta Drive: <code>Valorizaciones / Pdf</code>"
                    )
                    files = {'document': (res['filename'], res['pdf_bytes'], 'application/pdf')}
                    data = {
                        'chat_id': str(target_user),
                        'caption': caption,
                        'parse_mode': 'HTML',
                        'reply_markup': json.dumps({
                            'inline_keyboard': [
                                [{'text': '📂 Ver PDF en Drive', 'url': res['drive_link']}],
                                [{'text': '📊 Menú Documentos', 'callback_data': 'menu_documentos'}]
                            ]
                        })
                    }
                    try:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument", data=data, files=files, timeout=30)
                    except Exception as e_tg:
                        logger.error(f"Error enviando PDF de valorización por Telegram: {e_tg}")

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(res, ensure_ascii=False).encode('utf-8'))
                return
            except Exception as e:
                logger.error(f"Error en POST /api/valorizaciones/pdf: {e}")
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

