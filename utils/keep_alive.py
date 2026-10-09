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
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

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

        elif path == '/api/certificados/emitir':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

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

                # Respuesta segura JSON para la Mini App
                res_safe = {k: v for k, v in res.items() if k != 'pdf_bytes'}
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

        elif path == '/api/certificados/regenerar':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                post_body = self.rfile.read(content_len)
                payload = json.loads(post_body.decode('utf-8'))

                corr = payload.get('correlativo')
                if not corr:
                    raise ValueError("Falta 'correlativo' en la solicitud.")

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
                    usuario_editor=payload.get('usuario_email', 'Usuario MiniApp')
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

                res_safe = {k: v for k, v in res.items() if k != 'pdf_bytes'}
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

