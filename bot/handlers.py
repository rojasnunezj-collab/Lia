# ====================================================================
# --- IMPORTS ---
# ====================================================================
import os
import json
import asyncio
import re
import calendar
import html
from datetime import datetime, timezone, timedelta

from google.genai import types
from google.oauth2 import service_account  # <--- Agregado para las llaves
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes
import gspread

from config.settings import (
    logger, MODO_GUIAS_LEER, MODO_GUIAS_REGISTRAR,
    MODO_GUIAS_MANUAL, MODO_GUIAS_MANUAL_FECHA, MODO_GUIAS_MANUAL_NUMGUIA,
    MODO_GUIAS_MANUAL_TIPO, MODO_GUIAS_MANUAL_EMPRESA, MODO_GUIAS_MANUAL_FUNDO,
    MODO_REPORTE_REGISTRO, MODO_REPORTE_RECIBIDAS, MODO_COMENTAR_GUIA, MODO_COMENTAR_TEXTO,
    MODO_BUSCAR_CERT_FECHA, MODO_BUSCAR_CERT_FUNDO, 
    MODO_BUSCAR_CERT_CORRE, MODO_BUSCAR_CERT_EMPRESA,
    MODO_DIR_EMPRESA, MODO_DIR_FUNDO, MODO_BUSCAR_CLIENTE,
    MODO_BITACORA_ADD, MODO_BITACORA_SEARCH, MODO_BITACORA_EDIT_TEXT, MODO_OBS_ESCRIBIR, MODO_LIGAR_ESCRIBIR,
    MODO_COTIZACION_BUSCAR, MODO_COTIZACION_IA,
    MODO_FACTURAS_REGISTRAR, MODO_FACTURAS_BUSCAR,
    MODO_PENDIENTE_ADD, MODO_CREDENCIAL_BUSCAR,
    DRIVE_FOLDER_LEER, DRIVE_FOLDER_FACTURAS
)
from utils.helpers import clean_json_response, async_log_action, load_memoria_vinculacion, save_memoria_vinculacion, match_company_flexible
from core.ai_client import generar_con_reintento
from core.sheets_client import (
    conectar_servicios, async_get_all_records, async_buscar_link_en_drive, 
    async_subir_a_drive, sync_upsert_row, async_upsert_row, obtener_credenciales, SHEET_ID,
    SHEET_URL_DIRECT, normalizar_valor_upper
)
from core.cotizaciones_service import (
    async_generar_cotizacion, async_buscar_cotizacion_por_correlativo,
    obtener_siguiente_correlativo, obtener_url_webapp,
    async_sincronizar_pdf_desde_doc, interpretar_cotizacion_ia,
    buscar_ruc_por_empresa, async_buscar_ruc_por_empresa
)
from core.invoices_service import (
    async_parse_factura_xml, parse_factura_pdf, async_detectar_tipo_documento_pdf,
    async_guardar_factura_en_sheet, async_buscar_facturas_en_sheet,
    async_obtener_o_crear_carpeta_empresa
)
from core.bitacora_service import (
    CATEGORIAS_DISPONIBLES, CATEGORIAS_INFO, procesar_entrada_ia,
    guardar_registro_bitacora, obtener_registros_bitacora,
    buscar_conversacional_bitacora, obtener_ultimas_anotaciones_formateadas
)
from core.pendientes_service import (
    async_obtener_pendientes, async_crear_pendiente,
    async_actualizar_estado_pendiente, async_posponer_pendiente,
    async_obtener_alertas_por_disparar, async_marcar_alerta_enviada,
    procesar_pendiente_ia, obtener_url_panel, obtener_url_webapp_pendientes
)
from core.credenciales_service import (
    async_obtener_credenciales, async_guardar_credencial,
    obtener_url_webapp_credenciales
)

def normalize_guide_number(val):
    if not val:
        return ""
    val_str = str(val).strip().upper()
    val_clean = re.sub(r'[^A-Z0-9\-]', '', val_str)
    if "-" in val_clean:
        parts = val_clean.split("-")
        normalized_parts = []
        for p in parts:
            p_strip = p.lstrip('0')
            normalized_parts.append(p_strip if p_strip else '0')
        return "-".join(normalized_parts)
    else:
        p_strip = val_clean.lstrip('0')
        return p_strip if p_strip else '0'

def check_is_petramas(val):
    if not val:
        return False
    return bool(re.search(r'PETRAM[AÁ]S', str(val), re.IGNORECASE))

def match_guia_en_texto(guia_target, texto_referencia):
    """
    Determina si un número de guía (guia_target) coincide o está presente
    en el campo de texto de referencia (texto_referencia), tolerando diferencias
    de ceros a la izquierda, guiones, prefijos 'N°', 'Nº', etc.
    """
    if not guia_target or not texto_referencia:
        return False
    g_raw = str(guia_target).strip().upper()
    t_raw = str(texto_referencia).strip().upper()
    if not g_raw or not t_raw:
        return False

    # 1. Coincidencia directa por subcadena
    if g_raw in t_raw:
        return True

    # 2. Normalización estándar (remover ceros no significativos)
    norm_target = normalize_guide_number(g_raw)
    norm_ref = normalize_guide_number(t_raw)
    if norm_target and (norm_target == norm_ref or norm_target in norm_ref):
        return True

    # 3. Limpieza flexible (elimina prefijos comunes como 'N°', 'Nº', espacios y ceros)
    def _limpiar_flexible(s):
        s_clean = re.sub(r'N[°ºO\s]*', '', s)
        s_clean = re.sub(r'\s+', '', s_clean)
        if '-' in s_clean:
            partes = s_clean.split('-')
            return f"{partes[0]}-{partes[1].lstrip('0')}"
        return s_clean.lstrip('0')

    g_flex = _limpiar_flexible(g_raw)
    t_flex = _limpiar_flexible(t_raw)
    if g_flex and (g_flex in t_flex or t_flex in g_flex):
        return True

    # 4. Tokenización (para cuando en la columna Guía de Historial hay varias guías separadas por comas, etc.)
    tokens = re.split(r'[,;/|\s]+', t_raw)
    for tok in tokens:
        tok_clean = tok.strip()
        if not tok_clean:
            continue
        if norm_target and normalize_guide_number(tok_clean) == norm_target:
            return True
        if g_flex and _limpiar_flexible(tok_clean) == g_flex:
            return True

    return False

async def async_obtener_historial_certificados():
    """Descarga de forma segura y en hilo secundario los registros de la hoja Historial."""
    def _fetch():
        try:
            creds = obtener_credenciales()
            if not creds:
                return []
            client = gspread.authorize(creds)
            book2 = client.open_by_key(SHEET_ID)
            ws_hist = book2.worksheet("Historial")
            return ws_hist.get_all_records()
        except Exception as e:
            logger.warning(f"No se pudo cargar la hoja Historial para certificados: {e}")
            return []
    return await asyncio.to_thread(_fetch)

import core.sheets_client as rc 

# ====================================================================
# --- INICIALIZACIÓN DE IA Y MEMORIA (BLINDAJE) ---
# ====================================================================
# --- ESTADOS (CACHÉ) ---
# ====================================================================
user_states = {}
user_data_cache = {}
MEMORIA_VINCULACION = load_memoria_vinculacion() or {}

# ====================================================================
# --- TAREAS PROGRAMADAS (JOBS) ---
# ====================================================================
PET_TZ = timezone(timedelta(hours=-5))

async def prosembra_notification_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    guia = job.data.get("guia", "desconocida")
    empresa = "PROSEMBRA"
    
    kb = [
        [
            InlineKeyboardButton("✅ Sí", callback_data=f"rem|si|{guia}|{empresa}"),
            InlineKeyboardButton("❌ No", callback_data=f"rem|no|{guia}|{empresa}")
        ]
    ]
    
    await context.bot.send_message(
        chat_id=job.chat_id,
        text=f"🔔 *Recordatorio Prosembra:*\nHan pasado 30 minutos desde el registro de la guía `{guia}`.\n¿Ya tienes el peso?",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def olivos_notification_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    guia = job.data.get("guia", "desconocida")
    empresa = "LOS OLIVOS"
    
    kb = [
        [
            InlineKeyboardButton("✅ Sí", callback_data=f"rem|si|{guia}|{empresa}"),
            InlineKeyboardButton("❌ No", callback_data=f"rem|no|{guia}|{empresa}")
        ]
    ]
    
    await context.bot.send_message(
        chat_id=job.chat_id,
        text=f"🔔 *Recordatorio Los Olivos:*\nHan pasado 30 minutos desde el registro de la guía `{guia}`.\n¿Ya tienes el peso?",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def handle_callback_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query.data.startswith('rem|'):
        return
        
    await query.answer()
    
    partes = query.data.split("|")
    accion = partes[1]
    
    if accion == "si":
        guia = partes[2]
        empresa = partes[3]
        await query.edit_message_text(
            f"🔔 *Recordatorio {empresa.title()}:*\n"
            f"📄 Guía: `{guia}`\n\n"
            f"✅ *El peso ya está listo.* ¡Recordatorio finalizado!",
            parse_mode='Markdown'
        )
        
    elif accion == "no":
        guia = partes[2]
        empresa = partes[3]
        
        kb = [
            [InlineKeyboardButton("⏰ 15 min", callback_data=f"rem|set|15|{guia}|{empresa}")],
            [InlineKeyboardButton("⏰ 30 min", callback_data=f"rem|set|30|{guia}|{empresa}")],
            [InlineKeyboardButton("⏰ 1 hora", callback_data=f"rem|set|60|{guia}|{empresa}")],
            [InlineKeyboardButton("⏰ 2 horas", callback_data=f"rem|set|120|{guia}|{empresa}")],
            [InlineKeyboardButton("❌ Cancelar Recordatorios", callback_data=f"rem|cancel|{guia}|{empresa}")]
        ]
        
        await query.edit_message_text(
            f"🔔 *Recordatorio {empresa.title()}:*\n"
            f"📄 Guía: `{guia}`\n\n"
            f"⏳ El peso aún no está listo. ¿En cuánto tiempo deseas que te vuelva a recordar?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(kb)
        )
        
    elif accion == "set":
        minutos = int(partes[2])
        guia = partes[3]
        empresa = partes[4]
        chat_id = query.message.chat_id
        
        # Programar nuevo recordatorio
        job_func = prosembra_notification_job if empresa.upper() == "PROSEMBRA" else olivos_notification_job
        context.job_queue.run_once(job_func, minutos * 60, chat_id=chat_id, data={"guia": guia})
        
        tiempo_str = f"{minutos} minutos" if minutos < 60 else f"{minutos // 60} hora(s)"
        
        await query.edit_message_text(
            f"⏰ *Listo.* He programado un nuevo recordatorio en *{tiempo_str}* para la guía `{guia}`.",
            parse_mode='Markdown'
        )
        
    elif accion == "cancel":
        guia = partes[2]
        empresa = partes[3]
        await query.edit_message_text(
            f"❌ *Recordatorios cancelados* para la guía `{guia}`.",
            parse_mode='Markdown'
        )

async def handle_callback_pendientes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query.data.startswith('pnd_'):
        return
    try:
        await query.answer()
    except Exception:
        pass
    user_id = query.from_user.id
    data = query.data

    if data == 'pnd_add':
        user_states[user_id] = MODO_PENDIENTE_ADD
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='menu_pendientes'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]]
        texto = (
            "✍️ *Crear Nuevo Pendiente*\n\n"
            "Escribe o envía una nota de voz con lo que necesitas recordar. Por ejemplo:\n"
            "• _\"Recordar mañana a las 9am pedir certificado a Cerro Prieto\"_\n"
            "• _\"Pedir peso de guía T001-45 en 30 minutos\"_\n"
            "• _\"Pagar flete de transporte el viernes a las 3pm\"_\n\n"
            "💡 _Lía calculará automáticamente la fecha/hora y te pedirá confirmar el borrador antes de guardarlo en Sheets._"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
        return

    elif data == 'pnd_list':
        msg_wait = await query.message.reply_text("⏳ Consultando pendientes activos en Google Sheets...")
        try:
            pendientes = await async_obtener_pendientes(solo_activos=True)
            if not pendientes:
                kb = [
                    [InlineKeyboardButton("➕ Crear Pendiente", callback_data='pnd_add')],
                    [InlineKeyboardButton("🔙 Volver", callback_data='menu_pendientes')]
                ]
                await msg_wait.edit_text("🎉 <b>¡Excelente! No tienes pendientes activos por el momento.</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode='HTML')
            else:
                texto_lista = f"📌 <b>Pendientes Activos ({len(pendientes)}):</b>\n\n"
                kb_items = []
                for p in pendientes[:8]:
                    p_id = html.escape(str(p.get('ID', '')))
                    tit = html.escape(str(p.get('TITULO_TAREA', 'Sin título')))
                    fec = html.escape(str(p.get('FECHA_ALERTA', '')))
                    prio = str(p.get('PRIORIDAD', 'MEDIA')).upper()
                    p_badge = "🔴" if prio == "ALTA" else ("🟢" if prio == "BAJA" else "🟡")
                    texto_lista += f"{p_badge} <code>[{p_id}]</code> <b>{tit}</b>\n⏰ <i>{fec}</i>\n\n"
                    kb_items.append([
                        InlineKeyboardButton(f"✅ Listo {p_id}", callback_data=f"pnd_done|{p_id}"),
                        InlineKeyboardButton(f"⏰ +1h", callback_data=f"pnd_snooze|{p_id}|60")
                    ])

                url_app_pnd = obtener_url_webapp_pendientes(user_id=user_id)
                kb_items.append([InlineKeyboardButton("📱 Ver Todos en WebApp", web_app=WebAppInfo(url=url_app_pnd))])
                kb_items.append([InlineKeyboardButton("➕ Nuevo", callback_data='pnd_add'), InlineKeyboardButton("🔙 Volver", callback_data='menu_pendientes')])

                await msg_wait.edit_text(texto_lista, reply_markup=InlineKeyboardMarkup(kb_items), parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error listando pendientes: {e}")
            await msg_wait.edit_text(f"❌ Error al consultar pendientes: {html.escape(str(e))}")
        return

    elif data == 'pnd_confirm':
        cache = user_data_cache.get(user_id, {})
        draft = cache.get("pendiente_draft")
        if not draft:
            await query.answer("⚠️ No hay borrador de pendiente para guardar.", show_alert=True)
            return

        msg_save = await query.message.reply_text("⏳ Registrando pendiente en Google Sheets...")
        try:
            draft["usuario"] = query.from_user.first_name or f"User_{user_id}"
            pnd_id = await async_crear_pendiente(draft)
            user_data_cache[user_id] = {}
            user_states[user_id] = None

            url_panel = obtener_url_webapp_pendientes(user_id=user_id)
            kb = [
                [InlineKeyboardButton("📱 Ver en WebApp", web_app=WebAppInfo(url=url_panel))],
                [InlineKeyboardButton("➕ Nuevo Pendiente", callback_data='pnd_add')],
                [InlineKeyboardButton("🔙 Menú Pendientes", callback_data='menu_pendientes')]
            ]
            t_pnd = html.escape(str(pnd_id))
            t_tit = html.escape(str(draft.get('titulo', '')))
            t_fec = html.escape(str(draft.get('fecha_alerta', '')))
            t_prio = html.escape(str(draft.get('prioridad', '')))
            await msg_save.edit_text(
                f"✅ <b>¡Pendiente <code>{t_pnd}</code> guardado exitosamente!</b>\n\n"
                f"📌 <b>Tarea:</b> {t_tit}\n"
                f"⏰ <b>Alerta:</b> <code>{t_fec}</code>\n"
                f"🎯 <b>Prioridad:</b> <code>{t_prio}</code>\n\n"
                f"🔔 Te notificaré automáticamente por este chat cuando venza el plazo.",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Error guardando pendiente: {e}")
            await msg_save.edit_text(f"❌ Error al guardar pendiente en Google Sheets: {html.escape(str(e))}")

    elif data == 'pnd_cancel':
        user_data_cache[user_id] = {}
        user_states[user_id] = None
        await safe_edit_or_reply(query, "❌ Pendiente cancelado / descartado.", reply_markup=get_main_menu_keyboard(user_id))

    elif data.startswith('pnd_done|'):
        pnd_id = data.split('|')[1]
        ok = await async_actualizar_estado_pendiente(pnd_id, 'COMPLETADO')
        if ok:
            await safe_edit_or_reply(
                query,
                f"✅ <b>¡Pendiente <code>[{html.escape(pnd_id)}]</code> completado y marcado en Google Sheets!</b> 🎉",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Menú Pendientes", callback_data='menu_pendientes')]]),
                parse_mode='HTML'
            )
        else:
            await query.answer("❌ No se pudo actualizar el estado del pendiente.", show_alert=True)

    elif data.startswith('pnd_snooze|'):
        parts = data.split('|')
        pnd_id = parts[1]
        minutos = int(parts[2]) if len(parts) > 2 else 60
        nueva_fecha = await async_posponer_pendiente(pnd_id, minutos)
        if nueva_fecha:
            tiempo_str = f"{minutos} min" if minutos < 60 else f"{minutos // 60} h"
            if minutos >= 1440:
                tiempo_str = "mañana"
            await safe_edit_or_reply(
                query,
                f"⏰ <b>Pendiente <code>[{html.escape(pnd_id)}]</code> pospuesto ({tiempo_str}).</b>\nNueva alerta reprogramada para: <code>{html.escape(str(nueva_fecha))}</code>",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Menú Pendientes", callback_data='menu_pendientes')]]),
                parse_mode='HTML'
            )
        else:
            await query.answer("❌ Error posponiendo alerta.", show_alert=True)

async def daily_certificate_reminder(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(PET_TZ)
    if now.year < 2026 or (now.year == 2026 and now.month < 4):
        return
        
    last_day = calendar.monthrange(now.year, now.month)[1]
    is_day_before = (now.day == last_day - 1)
    is_last_day = (now.day == last_day)
    is_day_after = (now.day == 1)
    
    if not (is_day_before or is_last_day or is_day_after):
        return
        
    admin_chat_id = os.getenv("ADMIN_CHAT_ID")
    if not admin_chat_id: return

    try:
        def fetch_guias():
            creds = obtener_credenciales()
            client = gspread.authorize(creds)
            book2 = client.open_by_key(SHEET_ID)
            return book2.worksheet("Guias_recibidas").get_all_records()
            
        registros = await asyncio.to_thread(fetch_guias)
        pendientes = []
        
        for r in registros:
            fecha = str(r.get("Fecha", ""))
            if "/04/2026" in fecha or "/05/2026" in fecha or "/06/2026" in fecha or "/07/2026" in fecha or "/08/2026" in fecha or "/09/2026" in fecha or "/10/2026" in fecha or "/11/2026" in fecha or "/12/2026" in fecha or "2027" in fecha:
                certificado = str(r.get("Certificados", "")).strip()
                empresa = str(r.get("Empresa Principal", "")).upper()
                fundo = str(r.get("Fundo/Planta", "")).upper()
                texto_cliente = f"{empresa} {fundo}"
                
                # Omitir Prosembra y Los Olivos (se avisan a los 30 mins)
                if not certificado and "PROSEMBRA" not in texto_cliente and "LOS OLIVOS" not in texto_cliente:
                    pendientes.append(str(r.get("N° Guía", "S/D")))
        
        if pendientes:
            msg = f"📅 🔔 *RECORDATORIO DE FIN DE MES*\nHay {len(pendientes)} certificados pendientes desde abril en la pestaña Guías Recibidas:\n\n"
            msg += ", ".join(pendientes[:20])
            if len(pendientes) > 20:
                msg += f" ... y {len(pendientes)-20} más."
            await context.bot.send_message(chat_id=admin_chat_id, text=msg, parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Error en daily_certificate_reminder: {e}")

# ====================================================================
# --- TAREA PROGRAMADA: ALERTAS DE PENDIENTES ---
# ====================================================================
async def job_verificar_alertas_pendientes(context: ContextTypes.DEFAULT_TYPE):
    """Revisa en segundo plano si hay pendientes cuya hora de alerta ya llegó."""
    try:
        disparables = await async_obtener_alertas_por_disparar()
        if not disparables:
            return

        admin_chat_id = os.getenv("ADMIN_CHAT_ID")
        if not admin_chat_id:
            return

        for pnd in disparables:
            pnd_id = str(pnd.get("ID", "")).strip()
            titulo = str(pnd.get("TITULO_TAREA", "Sin título")).strip()
            detalle = str(pnd.get("DETALLE", "")).strip()
            cliente = str(pnd.get("CLIENTE_REF", "")).strip()
            prioridad = str(pnd.get("PRIORIDAD", "MEDIA")).strip().upper()
            fecha_alerta = str(pnd.get("FECHA_ALERTA", "")).strip()

            p_badge = "🔴 ALTA" if prioridad == "ALTA" else ("🟢 BAJA" if prioridad == "BAJA" else "🟡 MEDIA")

            t_id = html.escape(pnd_id)
            t_titulo = html.escape(titulo)
            t_cliente = html.escape(cliente)
            t_detalle = html.escape(detalle)
            t_fecha = html.escape(fecha_alerta)

            msg = (
                f"🔔 <b>ALERTA DE PENDIENTE</b> <code>[{t_id}]</code>\n\n"
                f"📌 <b>Tarea:</b> <b>{t_titulo}</b>\n"
            )
            if cliente:
                msg += f"🏢 <b>Referencia:</b> <code>{t_cliente}</code>\n"
            if detalle:
                msg += f"📝 <b>Detalle:</b> <i>{t_detalle}</i>\n"
            msg += (
                f"⏰ <b>Programado:</b> <code>{t_fecha}</code>\n"
                f"🎯 <b>Prioridad:</b> <code>{p_badge}</code>\n\n"
                f"¿Qué deseas hacer con este pendiente?"
            )

            kb = [
                [InlineKeyboardButton("✅ Marcar Listo", callback_data=f"pnd_done|{pnd_id}")],
                [InlineKeyboardButton("⏰ +30 min", callback_data=f"pnd_snooze|{pnd_id}|30"),
                 InlineKeyboardButton("⏰ +2 horas", callback_data=f"pnd_snooze|{pnd_id}|120")],
                [InlineKeyboardButton("📅 Para Mañana", callback_data=f"pnd_snooze|{pnd_id}|1440")]
            ]

            try:
                await context.bot.send_message(
                    chat_id=admin_chat_id,
                    text=msg,
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup(kb)
                )
                await async_marcar_alerta_enviada(pnd_id)
            except Exception as e_send:
                logger.error(f"Error enviando alerta pendiente {pnd_id}: {e_send}")
    except Exception as e:
        logger.error(f"Error en job_verificar_alertas_pendientes: {e}")

# ====================================================================
# --- HANDLERS BÁSICOS Y TECLADO PRINCIPAL ---
# ====================================================================
def get_main_menu_keyboard(user_id=None):
    """Construye el teclado del menú principal organizado jerárquicamente (Opción 1)."""
    keyboard = [
        [InlineKeyboardButton("📑 Gestión de Documentos", callback_data='menu_documentos')],
        [InlineKeyboardButton("🔍 Centro de Búsqueda", callback_data='menu_busqueda')],
        [
            InlineKeyboardButton("📌 Pendientes & Alertas", callback_data='menu_pendientes'),
            InlineKeyboardButton("📓 Bitácora Libre", callback_data='modo_bitacora')
        ],
        [
            InlineKeyboardButton("🔐 Credenciales", callback_data='menu_credenciales'),
            InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_documentos_menu_keyboard():
    """Construye el teclado del submenú de Documentos."""
    keyboard = [
        [InlineKeyboardButton("📘 Guías de Remisión", callback_data='menu_guias')],
        [InlineKeyboardButton("🧾 Facturas SUNAT", callback_data='menu_facturas')],
        [InlineKeyboardButton("📋 Cotizaciones", callback_data='menu_cotizaciones')],
        [InlineKeyboardButton("📜 Certificados", callback_data='menu_certificados')],
        [InlineKeyboardButton("🔙 Volver al Menú Principal", callback_data='volver_inicio')]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    await update.message.reply_text("👋 ¡Hola! Soy Lía.\nSelecciona el módulo al que deseas acceder:", reply_markup=get_main_menu_keyboard(user_id))

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏓 Pong!")

async def safe_edit_or_reply(query, text, reply_markup=None, parse_mode=None):
    """Edita el mensaje si es texto plano, o envía una nueva respuesta si el mensaje original es un documento o foto."""
    try:
        if query.message and query.message.text is not None:
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            return
    except Exception as e_edit:
        logger.warning(f"No se pudo editar mensaje directamente ({e_edit}), enviando como nueva respuesta.")
    try:
        await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception as e:
        logger.error(f"Error en safe_edit_or_reply: {e}")

def build_bitacora_draft_card(draft):
    """Construye la tarjeta de visualización de borrador de Bitácora con botones de confirmación y edición."""
    cat = draft.get("categoria", "GENERAL")
    cat_emoji, cat_label = CATEGORIAS_INFO.get(cat, ("📝", cat))
    tags = draft.get("tags", "")
    formato = draft.get("formato", "Texto/Anotación")
    texto = draft.get("texto", "")
    resumen = draft.get("resumen", "")
    
    msg_card = (
        "📓 **Borrador de Bitácora Generado**\n\n"
        f"📂 **Categoría:** `{cat}` ({cat_emoji} {cat_label})\n"
    )
    if tags:
        msg_card += f"🏷️ **Etiquetas:** `{tags}`\n"
    msg_card += f"📎 **Formato:** `{formato}`\n"
    if resumen:
        msg_card += f"💡 **Resumen:** _{resumen}_\n"
    msg_card += (
        f"\n📝 **Contenido detectado / transcrito:**\n"
        f"_{texto}_\n\n"
        f"❓ **¿La información está correcta o deseas editar algo antes de guardarla?**"
    )
    
    kb = [
        [InlineKeyboardButton("✅ Confirmar y Guardar", callback_data='bita_confirm')],
        [InlineKeyboardButton("✏️ Editar Texto / Nota", callback_data='bita_edit_text'),
         InlineKeyboardButton("🏷️ Cambiar Categoría", callback_data='bita_pick_cat')],
        [InlineKeyboardButton("❌ Cancelar / Descartar", callback_data='bita_cancel')]
    ]
    return msg_card, InlineKeyboardMarkup(kb)

def build_pendiente_draft_card(draft):
    """Construye la tarjeta de visualización de borrador de Pendiente con botones de confirmación."""
    titulo = html.escape(str(draft.get("titulo", "Sin título")).strip())
    detalle = html.escape(str(draft.get("detalle", "")).strip())
    cliente = html.escape(str(draft.get("cliente_ref", "")).strip())
    fecha_alerta = html.escape(str(draft.get("fecha_alerta", "")).strip())
    prioridad = str(draft.get("prioridad", "MEDIA")).strip().upper()

    p_badge = "🔴 ALTA" if prioridad == "ALTA" else ("🟢 BAJA" if prioridad == "BAJA" else "🟡 MEDIA")

    msg = (
        "📌 <b>Borrador de Pendiente Detectado</b>\n\n"
        f"📝 <b>Tarea:</b> <b>{titulo}</b>\n"
    )
    if cliente:
        msg += f"🏢 <b>Referencia / Cliente:</b> <code>{cliente}</code>\n"
    if detalle:
        msg += f"📋 <b>Detalle:</b> <i>{detalle}</i>\n"
    msg += (
        f"⏰ <b>Alerta Programada:</b> <code>{fecha_alerta}</code>\n"
        f"🎯 <b>Prioridad:</b> <code>{p_badge}</code>\n\n"
        f"❓ ¿Deseas confirmar y guardar este pendiente en Google Sheets?"
    )

    kb = [
        [InlineKeyboardButton("✅ Confirmar y Guardar", callback_data='pnd_confirm')],
        [InlineKeyboardButton("❌ Cancelar / Descartar", callback_data='pnd_cancel')]
    ]
    return msg, InlineKeyboardMarkup(kb)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"Ignorando error al responder query (posiblemente muy antiguo): {e}")
    user_id = query.from_user.id
    
    if query.data == 'cancelar_start':
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        await safe_edit_or_reply(query, "👍 Entendido. Me quedo atenta cuando me necesites. ¡Que tengas un excelente día! 👋", reply_markup=None)
        return
        
    if query.data == 'cancelar_operacion':
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        await safe_edit_or_reply(query, "👋 ¡Hola! Soy Lía.\nSelecciona el módulo al que deseas acceder:", reply_markup=get_main_menu_keyboard(user_id))

    elif query.data == 'volver_inicio':
        user_states[user_id] = None
        await safe_edit_or_reply(query, "👋 ¡Hola! Soy Lía.\nSelecciona el módulo al que deseas acceder:", reply_markup=get_main_menu_keyboard(user_id))

    elif query.data == 'menu_documentos':
        user_states[user_id] = None
        texto = (
            "📑 *Gestión de Documentos*\n\n"
            "Selecciona el tipo de documento que deseas gestionar:\n\n"
            "• 📘 *Guías:* Lectura inteligente, registro en Sheets y carga manual.\n"
            "• 🧾 *Facturas:* Carga XML/PDF y búsqueda rápida.\n"
            "• 📋 *Cotizaciones:* Emisión y búsqueda de cotizaciones.\n"
            "• 📜 *Certificados:* Búsqueda por fecha, fundo, empresa o correlativo."
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=get_documentos_menu_keyboard())

    elif query.data == 'menu_facturas':
        user_states[user_id] = None
        keyboard = [
            [InlineKeyboardButton("📥 Registrar Factura (PDF / XML)", callback_data='modo_facturas_registrar')],
            [InlineKeyboardButton("🔍 Buscar Factura", callback_data='modo_facturas_buscar')],
            [InlineKeyboardButton("🔙 Volver a Documentos", callback_data='menu_documentos')]
        ]
        texto = (
            "🧾 *Módulo de Facturas*\n\n"
            "• *Archivos XML:* Procesamiento instantáneo (100% exacto UBL 2.1 SUNAT).\n"
            "• *Archivos PDF:* Lectura inteligente y extracción con IA.\n"
            "• *Google Drive:* Respaldo automático en carpeta Facturas.\n\n"
            "Selecciona una opción:"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == 'modo_facturas_registrar':
        user_states[user_id] = MODO_FACTURAS_REGISTRAR
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='menu_facturas'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]]
        texto = (
            "📥 *Modo Registro de Facturas*\n\n"
            "Envía el archivo de la factura (**XML** o **PDF**).\n\n"
            "💡 _Si tienes el archivo XML, envíalo directamente: se procesa al instante con precisión exacta._"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif query.data == 'modo_facturas_buscar':
        user_states[user_id] = MODO_FACTURAS_BUSCAR
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='menu_facturas'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]]
        texto = (
            "🔍 *Buscar Factura en el Sheet*\n\n"
            "Escribe el *N° de Factura* (ej: `F001-4567`), el *RUC* o el *nombre del emisor*:"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
        
    elif query.data == 'menu_cotizaciones':
        user_states[user_id] = None
        corr_sig = obtener_siguiente_correlativo()
        url_app = obtener_url_webapp(correlativo=corr_sig, user_id=user_id)
        keyboard = [
            [InlineKeyboardButton("➕ Nueva Cotización", web_app=WebAppInfo(url=url_app))],
            [InlineKeyboardButton("🎙️ Cotizar por Voz o Texto", callback_data='coti_ia')],
            [InlineKeyboardButton("🔍 Buscar / Modificar Cotización", callback_data='coti_buscar')],
            [InlineKeyboardButton("🔙 Volver a Documentos", callback_data='menu_documentos'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
        ]
        texto = (
            f"📋 *Módulo de Cotizaciones - EPMI SAC*\n\n"
            f"• *Siguiente Correlativo sugerido:* `{corr_sig}`\n"
            f"• Pulsa *➕ Nueva Cotización* para abrir el formulario en tu celular.\n"
            f"• Pulsa *🎙️ Cotizar por Voz o Texto* para dictar la cotización a Lía.\n"
            f"• Para revisar o editar una cotización previa, pulsa *🔍 Buscar / Modificar*."
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == 'coti_ia':
        user_states[user_id] = MODO_COTIZACION_IA
        keyboard = [
            [InlineKeyboardButton("🔙 Volver a Cotizaciones", callback_data='menu_cotizaciones'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
        ]
        texto = (
            "🎙️ *Cotización Asistida por IA - EPMI SAC*\n\n"
            "Puedes dictar por *nota de voz* o escribir un *mensaje de texto* con los datos de la cotización.\n\n"
            "📌 *Indica:*\n"
            "1. La empresa o cliente (ej: _Agrícola Andrea, Exalmar, etc._)\n"
            "2. Los residuos y precios a cotizar\n\n"
            "💡 *Ejemplo de voz o texto:*\n"
            "_\"Cotización para Agrícola Andrea: Cartón a 0.10, Plástico film a 0.80 y Parihuelas a 15 la unidad\"_\n\n"
            "Lía estructurará los ítems automáticamente."
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data.startswith('coti_ia_gen|'):
        corr = query.data.split('|')[1]
        payload = context.user_data.get(f"coti_ia_{user_id}")
        if not payload:
            await query.answer("⚠️ No hay datos pendientes para generar. Intenta de nuevo.", show_alert=True)
            return
        await query.answer("⏳ Generando cotización...")
        msg_wait = await query.message.reply_text(f"⏳ Generando Google Doc y PDF para Cotización N°{corr}...")
        try:
            from io import BytesIO
            res = await async_generar_cotizacion(payload)
            pdf_bytes = res["pdf_bytes"]
            nombre_pdf = res["nombre_archivo"]
            doc_link = res["doc_link"]
            pdf_link = res["pdf_link"]
            codigo = res["codigo"]
            cliente = res["cliente"]
            fecha = res["fecha"]

            url_edit = obtener_url_webapp(correlativo=corr, datos_edicion=res["datos_json"], user_id=user_id)
            kb = [
                [InlineKeyboardButton("✏️ Modificar Cotización", web_app=WebAppInfo(url=url_edit))],
                [InlineKeyboardButton("📄 Doc Editable", url=doc_link), InlineKeyboardButton("📂 Ver en Drive", url=pdf_link)],
                [InlineKeyboardButton("🔄 Sincronizar PDF desde Doc", callback_data=f"coti_sync|{corr}")],
                [InlineKeyboardButton("📋 Menú Cotizaciones", callback_data='menu_cotizaciones'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
            ]
            caption = (
                f"✅ Cotización Generada Exitosamente\n\n"
                f"📌 Código: COTIZACION N°{codigo}\n"
                f"🏢 Cliente: {cliente}\n"
                f"📅 Fecha: {fecha}\n"
                f"💰 Ítems cotizados: {len(payload.get('items', []))} residuos\n\n"
                f"💾 Guardada en Google Drive y registrada en Sheets."
            )
            bio = BytesIO(pdf_bytes)
            bio.name = nombre_pdf
            await context.bot.send_document(
                chat_id=user_id,
                document=bio,
                caption=caption,
                reply_markup=InlineKeyboardMarkup(kb)
            )
            try:
                await msg_wait.delete()
            except Exception:
                pass
            context.user_data.pop(f"coti_ia_{user_id}", None)
        except Exception as e_gen:
            logger.error(f"Error generando cotización por IA: {e_gen}")
            await msg_wait.edit_text(f"❌ Error al generar cotización: {e_gen}")

    elif query.data.startswith('coti_sync|'):
        corr = query.data.split('|')[1]
        await query.answer("🔄 Sincronizando...")
        msg_wait = await query.message.reply_text(f"⏳ Leyendo cambios en Google Docs y actualizando PDF en Google Drive para N°{corr}...")
        try:
            res_sync = await async_sincronizar_pdf_desde_doc(corr)
            pdf_bytes = res_sync["pdf_bytes"]
            nombre_pdf = res_sync["nombre_archivo"]
            pdf_link = res_sync["pdf_link"]
            doc_id = res_sync["doc_id"]
            doc_link = f"https://docs.google.com/document/d/{doc_id}/edit"

            coti_data = await async_buscar_cotizacion_por_correlativo(corr)
            url_edit = obtener_url_webapp(correlativo=corr, datos_edicion=coti_data, user_id=user_id)

            from io import BytesIO
            bio = BytesIO(pdf_bytes)
            bio.name = nombre_pdf

            caption = (
                f"✅ <b>PDF Actualizado con Éxito</b>\n\n"
                f"📌 <b>Código:</b> <code>COTIZACION N°{res_sync['codigo']}</code>\n"
                f"🏢 <b>Cliente:</b> <code>{res_sync['cliente']}</code>\n\n"
                f"💾 El archivo PDF existente en Drive fue reemplazado con el contenido actual de Google Docs manteniendo el mismo enlace."
            )
            kb = [
                [InlineKeyboardButton("✏️ Modificar en Mini App", web_app=WebAppInfo(url=url_edit))],
                [InlineKeyboardButton("📄 Doc Editable", url=doc_link), InlineKeyboardButton("📂 Ver en Drive", url=pdf_link)],
                [InlineKeyboardButton("🔄 Sincronizar PDF de nuevo", callback_data=f"coti_sync|{corr}")],
                [InlineKeyboardButton("📋 Menú Cotizaciones", callback_data='menu_cotizaciones')]
            ]
            await query.message.reply_document(document=bio, caption=caption, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
            try:
                await msg_wait.delete()
            except Exception:
                pass
        except Exception as e_sync:
            logger.error(f"Error sincronizando PDF: {e_sync}")
            await msg_wait.edit_text(f"❌ Error al sincronizar PDF: {e_sync}")

    elif query.data == 'coti_buscar':
        user_states[user_id] = MODO_COTIZACION_BUSCAR
        keyboard = [
            [InlineKeyboardButton("🔙 Volver a Cotizaciones", callback_data='menu_cotizaciones'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
        ]
        texto = (
            "🔍 *Buscar o Modificar Cotización:*\n\n"
            "Escribe el número correlativo de la cotización (por ejemplo: `080` o `081`):"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data.startswith('coti_edit|'):
        corr = query.data.split('|')[1]
        coti_data = await async_buscar_cotizacion_por_correlativo(corr)
        if not coti_data:
            await query.answer("❌ No se encontró la cotización.", show_alert=True)
            return
        url_edit = obtener_url_webapp(correlativo=corr, datos_edicion=coti_data, user_id=user_id)
        keyboard = [
            [InlineKeyboardButton("✏️ Abrir Formulario de Edición", web_app=WebAppInfo(url=url_edit))],
            [InlineKeyboardButton("🔙 Volver a Cotizaciones", callback_data='menu_cotizaciones'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
        ]
        texto = (
            f"✏️ *Editar Cotización N°{corr}*\n\n"
            f"• *Cliente:* `{coti_data.get('cliente', '')}`\n"
            f"• *Fecha:* `{coti_data.get('fecha', '')}`\n\n"
            f"Pulsa el botón abajo para abrir la app con los datos cargados:"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif query.data == 'menu_pendientes':
        user_states[user_id] = None
        url_panel_pnd = obtener_url_webapp_pendientes(user_id=user_id)
        keyboard = [
            [InlineKeyboardButton("➕ Nuevo Pendiente (Texto o Voz)", callback_data='pnd_add')],
            [InlineKeyboardButton("📋 Ver Pendientes Activos", callback_data='pnd_list')],
            [InlineKeyboardButton("📱 Abrir en WebApp", web_app=WebAppInfo(url=url_panel_pnd))],
            [InlineKeyboardButton("🔙 Volver al Inicio", callback_data='volver_inicio')]
        ]
        texto = (
            "📌 *Módulo de Pendientes & Alertas*\n\n"
            "• Registra tareas con fecha y hora programada.\n"
            "• Lía te notificará automáticamente cuando se cumpla el plazo.\n"
            "• Puedes dictar notas de voz o escribir en lenguaje natural.\n\n"
            "Selecciona una opción:"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == 'pnd_add':
        user_states[user_id] = MODO_PENDIENTE_ADD
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='menu_pendientes'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]]
        texto = (
            "✍️ *Crear Nuevo Pendiente*\n\n"
            "Escribe o envía una nota de voz con lo que necesitas recordar. Por ejemplo:\n"
            "• _\"Recordar mañana a las 9am pedir certificado a Cerro Prieto\"_\n"
            "• _\"Pedir peso de guía T001-45 en 30 minutos\"_\n"
            "• _\"Pagar flete de transporte el viernes a las 3pm\"_\n\n"
            "💡 _Lía calculará automáticamente la fecha/hora y te pedirá confirmar el borrador antes de guardarlo en Sheets._"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif query.data == 'pnd_list':
        msg_wait = await query.message.reply_text("⏳ Consultando pendientes activos en Google Sheets...")
        try:
            pendientes = await async_obtener_pendientes(solo_activos=True)
            if not pendientes:
                kb = [
                    [InlineKeyboardButton("➕ Crear Pendiente", callback_data='pnd_add')],
                    [InlineKeyboardButton("🔙 Volver", callback_data='menu_pendientes')]
                ]
                await msg_wait.edit_text("🎉 <b>¡Excelente! No tienes pendientes activos por el momento.</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode='HTML')
            else:
                texto_lista = f"📌 <b>Pendientes Activos ({len(pendientes)}):</b>\n\n"
                kb_items = []
                for p in pendientes[:8]:
                    p_id = html.escape(str(p.get('ID', '')))
                    tit = html.escape(str(p.get('TITULO_TAREA', 'Sin título')))
                    fec = html.escape(str(p.get('FECHA_ALERTA', '')))
                    prio = str(p.get('PRIORIDAD', 'MEDIA')).upper()
                    p_badge = "🔴" if prio == "ALTA" else ("🟢" if prio == "BAJA" else "🟡")
                    texto_lista += f"{p_badge} <code>[{p_id}]</code> <b>{tit}</b>\n⏰ <i>{fec}</i>\n\n"
                    kb_items.append([
                        InlineKeyboardButton(f"✅ Listo {p_id}", callback_data=f"pnd_done|{p_id}"),
                        InlineKeyboardButton(f"⏰ +1h", callback_data=f"pnd_snooze|{p_id}|60")
                    ])

                url_panel_pnd = obtener_url_webapp_pendientes(user_id=user_id)
                kb_items.append([InlineKeyboardButton("📱 Ver Todos en WebApp", web_app=WebAppInfo(url=url_panel_pnd))])
                kb_items.append([InlineKeyboardButton("➕ Nuevo", callback_data='pnd_add'), InlineKeyboardButton("🔙 Volver", callback_data='menu_pendientes')])

                await msg_wait.edit_text(texto_lista, reply_markup=InlineKeyboardMarkup(kb_items), parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error listando pendientes: {e}")
            await msg_wait.edit_text(f"❌ Error al consultar pendientes: {html.escape(str(e))}")

    elif query.data == 'menu_credenciales':
        user_states[user_id] = None
        url_panel_crd = obtener_url_webapp_credenciales(user_id=user_id)
        keyboard = [
            [InlineKeyboardButton("🔍 Buscar Credencial", callback_data='crd_buscar')],
            [InlineKeyboardButton("📋 Ver Todas las Cuentas", callback_data='crd_list_all')],
            [InlineKeyboardButton("📱 Abrir Bóveda WebApp", web_app=WebAppInfo(url=url_panel_crd))],
            [InlineKeyboardButton("🔙 Volver al Inicio", callback_data='volver_inicio')]
        ]
        texto = (
            "🔐 *Bóveda de Credenciales & Accesos*\n\n"
            "• Consulta usuarios, contraseñas y accesos a portales.\n"
            "• SUNAT, Balanzas de Plantas, Portales de Clientes, etc.\n"
            "• Copia rápida de usuario y clave en un solo toque.\n\n"
            "Selecciona una opción:"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == 'crd_buscar':
        user_states[user_id] = MODO_CREDENCIAL_BUSCAR
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='menu_credenciales'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]]
        texto = (
            "🔍 *Buscar Credencial o Acceso*\n\n"
            "Escribe el nombre del servicio o plataforma (ej: `SUNAT`, `Petramás`, `Balanza`, `Ventanilla`):"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif query.data == 'crd_list_all':
        msg_wait = await query.message.reply_text("⏳ Consultando credenciales registradas...")
        try:
            creds = await async_obtener_credenciales()
            if not creds:
                url_panel_crd = obtener_url_webapp_credenciales(user_id=user_id)
                kb = [
                    [InlineKeyboardButton("➕ Registrar en WebApp", web_app=WebAppInfo(url=url_panel_crd))],
                    [InlineKeyboardButton("🔙 Volver", callback_data='menu_credenciales')]
                ]
                await msg_wait.edit_text("ℹ️ <b>No hay credenciales registradas aún en la hoja.</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode='HTML')
            else:
                texto_creds = f"🔐 <b>Bóveda de Credenciales ({len(creds)} registradas):</b>\n\n"
                for c in creds[:10]:
                    srv = html.escape(str(c.get('SERVICIO', '')).strip())
                    cat = html.escape(str(c.get('CATEGORIA', 'General')).strip())
                    usr = html.escape(str(c.get('USUARIO_RUC', '')).strip())
                    pwd = html.escape(str(c.get('CONTRASEÑA', '')).strip())
                    url_log = str(c.get('URL_LOGIN', '')).strip()
                    pin = html.escape(str(c.get('PIN_EXTRA', '')).strip())
                    obs = html.escape(str(c.get('OBSERVACIONES', '')).strip())

                    texto_creds += f"🏛️ <b>{srv}</b> <code>[{cat}]</code>\n"
                    if url_log:
                        url_esc = html.escape(url_log)
                        texto_creds += f"🌐 Enlace: {url_esc}\n"
                    if usr:
                        texto_creds += f"👤 Usuario: <code>{usr}</code>\n"
                    if pwd:
                        texto_creds += f"🔑 Clave: <code>{pwd}</code>\n"
                    if pin:
                        texto_creds += f"📌 PIN/Token: <code>{pin}</code>\n"
                    if obs:
                        texto_creds += f"💡 <i>{obs}</i>\n"
                    texto_creds += "──────────────────\n"

                url_panel_crd = obtener_url_webapp_credenciales(user_id=user_id)
                kb = [
                    [InlineKeyboardButton("📱 Gestionar en WebApp", web_app=WebAppInfo(url=url_panel_crd))],
                    [InlineKeyboardButton("🔍 Buscar", callback_data='crd_buscar'), InlineKeyboardButton("🔙 Volver", callback_data='menu_credenciales')]
                ]
                await msg_wait.edit_text(texto_creds, reply_markup=InlineKeyboardMarkup(kb), parse_mode='HTML', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error listando credenciales: {e}")
            await msg_wait.edit_text(f"❌ Error al consultar credenciales: {html.escape(str(e))}")

    elif query.data == 'menu_guias':
        user_states[user_id] = None
        keyboard = [
            [InlineKeyboardButton("📝 Leer Guía (OCR / IA)", callback_data='modo_guias_leer')],
            [InlineKeyboardButton("📁 Registrar Guía", callback_data='modo_guias_registrar')],
            [InlineKeyboardButton("📸 Subida Manual (Sin IA)", callback_data='modo_guias_manual')],
            [InlineKeyboardButton("🔙 Volver a Documentos", callback_data='menu_documentos')]
        ]
        texto = (
            "📘 *Módulo de Guías de Remisión*\n\n"
            "• *Leer Guía:* Extrae texto y datos clave sin guardar.\n"
            "• *Registrar Guía:* Procesa con IA y registra en Sheets + Drive.\n"
            "• *Subida Manual:* Carga paso a paso en caso de fotos difíciles.\n\n"
            "Selecciona una opción:"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == 'menu_busqueda':
        user_states[user_id] = None
        keyboard = [
            [InlineKeyboardButton("📊 Buscar Reporte (Guías)", callback_data='modo_buscar')],
            [InlineKeyboardButton("👥 Buscar Clientes", callback_data='modo_buscar_cliente')],
            [InlineKeyboardButton("📍 Buscar Direcciones", callback_data='modo_direcciones')],
            [InlineKeyboardButton("💬 Añadir Observación a Guía", callback_data='modo_comentar')],
            [InlineKeyboardButton("🔙 Volver al Menú Principal", callback_data='volver_inicio')]
        ]
        texto = (
            "🔍 *Centro de Búsqueda y Consultas*\n\n"
            "Selecciona la consulta que deseas realizar:\n\n"
            "• 📊 *Reportes:* Consulta en 'Registro Guías' o 'Guías Recibidas'.\n"
            "• 👥 *Clientes:* Búsqueda flexible de empresas registradas.\n"
            "• 📍 *Direcciones:* Ubicaciones por empresa o fundo/planta.\n"
            "• 💬 *Observaciones:* Añade comentarios a guías existentes."
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data in ('menu_certificados', 'modo_buscar_cert'):
        user_states[user_id] = None
        keyboard = [
            [InlineKeyboardButton("📅 Por Fecha", callback_data='cert_search_fecha'),
             InlineKeyboardButton("🏡 Por Fundo", callback_data='cert_search_fundo')],
            [InlineKeyboardButton("🏢 Por Empresa", callback_data='cert_search_empresa'),
             InlineKeyboardButton("🔢 Por Correlativo", callback_data='cert_search_corre')],
            [InlineKeyboardButton("🔙 Volver a Documentos", callback_data='menu_documentos')]
        ]
        texto = (
            "📜 *Módulo de Certificados*\n\n"
            "¿Por qué criterio deseas buscar el certificado?"
        )
        await safe_edit_or_reply(query, texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == 'modo_guias_leer':
        user_states[user_id] = MODO_GUIAS_LEER
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("✅ Modo Lectura. Sube la foto o PDF.", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_guias_registrar':
        user_states[user_id] = MODO_GUIAS_REGISTRAR
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("✅ Modo Registro. Sube la foto o PDF.", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_guias_manual':
        user_states[user_id] = MODO_GUIAS_MANUAL
        user_data_cache[user_id] = {}
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("📸 Subida Manual de Guía\nSube la foto o PDF de la guía. Después te pediré los datos manualmente.", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'manual_volver_fecha':
        user_states[user_id] = MODO_GUIAS_MANUAL_FECHA
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='menu_guias'), InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("📅 Paso 1/5 — Escribe la Fecha de la guía (Ej: 04/04/2026):", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'manual_volver_numguia':
        user_states[user_id] = MODO_GUIAS_MANUAL_NUMGUIA
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_fecha'), InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("📝 Paso 2/5 — Escribe el N° de Guía (Ej: T001-44 o EG03-293):", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'manual_volver_tipo':
        user_states[user_id] = MODO_GUIAS_MANUAL_TIPO
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_numguia'), InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("🏷️ Paso 3/5 — Escribe el Tipo Guía (Ej: REMITENTE o TRANSPORTISTA):", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'manual_volver_empresa':
        user_states[user_id] = MODO_GUIAS_MANUAL_EMPRESA
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_tipo'), InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("🏢 Paso 4/5 — Escribe la Empresa Principal:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'manual_volver_fundo':
        user_states[user_id] = MODO_GUIAS_MANUAL_FUNDO
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_empresa'), InlineKeyboardButton("❌ Cancelar", callback_data='menu_guias')]]
        await query.edit_message_text("🏡 Paso 5/5 — Escribe el Fundo/Planta:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_buscar':
        keyboard = [
            [InlineKeyboardButton("📋 Registro Guías", callback_data='reporte_registro')],
            [InlineKeyboardButton("📥 Guías Recibidas", callback_data='reporte_recibidas')],
            [InlineKeyboardButton("🔙 Volver a Búsquedas", callback_data='menu_busqueda')]
        ]
        await safe_edit_or_reply(query, "📊 *Buscar Reporte*\n¿En qué base de datos deseas buscar?", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'reporte_registro':
        user_states[user_id] = MODO_REPORTE_REGISTRO
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_buscar')]]
        await query.edit_message_text("🔍 Buscar en Registro Guias. Escribe la fecha (DD/MM/YYYY o DD/MM) o el número de guía", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'reporte_recibidas':
        user_states[user_id] = MODO_REPORTE_RECIBIDAS
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_buscar')]]
        await query.edit_message_text("🔍 Buscar en Guias Recibidas. Escribe la fecha (DD/MM/YYYY o DD/MM) o el número de guía", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_comentar':
        user_states[user_id] = MODO_COMENTAR_GUIA
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_busqueda')]]
        await query.edit_message_text("💬 Modo Observación\nIngresa el N° de Guía (Ej: EG03-293 o TR13-0002302) al que deseas añadirle un comentario:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'cert_search_fecha':
        user_states[user_id] = MODO_BUSCAR_CERT_FECHA
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_certificados')]]
        await query.edit_message_text("📅 Modo Certificados: Fecha\nEscribe la fecha (Ej: 30/03/2026 o 30/03):", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'cert_search_fundo':
        user_states[user_id] = MODO_BUSCAR_CERT_FUNDO
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_certificados')]]
        await query.edit_message_text("🏡 Modo Certificados: Fundo\nEscribe el nombre del Fundo:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'cert_search_empresa':
        user_states[user_id] = MODO_BUSCAR_CERT_EMPRESA
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_certificados')]]
        await query.edit_message_text("🏢 Modo Certificados: Empresa\nEscribe el nombre de la Empresa:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'cert_search_corre':
        user_states[user_id] = MODO_BUSCAR_CERT_CORRE
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_certificados')]]
        await query.edit_message_text("🔢 Modo Certificados: Correlativo\nEscribe el correlativo a buscar:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_direcciones':
        keyboard = [
            [InlineKeyboardButton("🏢 Por Empresa", callback_data='dir_buscar_empresa'),
             InlineKeyboardButton("🏡 Por Fundo/Planta", callback_data='dir_buscar_fundo')],
            [InlineKeyboardButton("🔙 Volver a Búsquedas", callback_data='menu_busqueda')]
        ]
        await safe_edit_or_reply(query, "📍 *Buscador de Direcciones*\n¿Por qué criterio deseas buscar?", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'dir_buscar_empresa':
        user_states[user_id] = MODO_DIR_EMPRESA
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_direcciones')]]
        await query.edit_message_text("🏢 Direcciones: Por Empresa\nIngresa un nombre parcial o clave de la empresa (Ej: 'Villa'):", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'dir_buscar_fundo':
        user_states[user_id] = MODO_DIR_FUNDO
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_direcciones')]]
        await query.edit_message_text("🏡 Direcciones: Por Fundo/Planta\nIngresa un nombre parcial o clave del Fundo/Planta:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_buscar_cliente':
        user_states[user_id] = MODO_BUSCAR_CLIENTE
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='menu_busqueda')]]
        await query.edit_message_text(
            "👥 **Búsqueda de Clientes**\n\n"
            "Ingresa el nombre o una parte del nombre de la empresa (Ej: `Olivos`, `Petramas`, `Laran`):\n\n"
            "💡 _Búsqueda flexible: no distingue mayúsculas/minúsculas ni tildes, y tolera pequeños errores de tipeo._",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
    elif query.data == 'modo_bitacora':
        user_states[user_id] = None
        keyboard = [
            [InlineKeyboardButton("✍️ Nueva Anotación (Texto, Audio, Foto)", callback_data='bitacora_add')],
            [InlineKeyboardButton("🔎 Búsqueda Inteligente (Pregúntale a Lía)", callback_data='bitacora_search')],
            [InlineKeyboardButton("📋 Ver Últimas Anotaciones", callback_data='bita_ultimas_5')],
            [InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]
        ]
        await safe_edit_or_reply(
            query,
            "📓 **Módulo de Bitácora Libre**\n\n¿Qué deseas hacer en tu Bitácora?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    elif query.data == 'bitacora_add':
        user_states[user_id] = MODO_BITACORA_ADD
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_bitacora')]]
        await safe_edit_or_reply(
            query,
            "✍️ **Añadiendo a Bitácora**\n\n"
            "Envíame lo que deseas registrar:\n"
            "• 📝 **Texto** con cualquier apunte o instrucción.\n"
            "• 🎙️ **Nota de voz o Audio** (lo transcribiré automáticamente).\n"
            "• 📸 **Foto o PDF** (extraeré el contenido con OCR e IA).\n\n"
            "💡 _Antes de guardar en la hoja, te mostraré el borrador y te preguntaré si deseas confirmarlo o editarlo._",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
    elif query.data == 'bitacora_search':
        user_states[user_id] = MODO_BITACORA_SEARCH
        kb = [
            [InlineKeyboardButton("📋 Ver Últimas 5 Anotaciones", callback_data='bita_ultimas_5')],
            [InlineKeyboardButton("🔙 Volver a Bitácora", callback_data='modo_bitacora')]
        ]
        await safe_edit_or_reply(
            query,
            "🔎 **Búsqueda Conversacional en Bitácora**\n\n"
            "Pregúntame directamente lo que necesitas saber, por ejemplo:\n"
            "• _\"¿Cómo se hacen las guías de Villacurí?\"_\n"
            "• _\"¿Qué reglas hay para los certificados de Prosembra?\"_\n"
            "• _\"¿Cuál es el RUC del transportista de Beta?\"_\n"
            "• _\"¿Qué notas o fotos se subieron recientemente?\"_\n\n"
            "💡 _Escribe en lenguaje natural. No necesitas comandos especiales y Lía tolera errores ortográficos y de tipeo._",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
    elif query.data == 'bita_ultimas_5':
        msg_wait = await query.message.reply_text("⏳ Obteniendo últimas anotaciones de la Bitácora...")
        try:
            texto_ultimos = await obtener_ultimas_anotaciones_formateadas(5)
            kb = [
                [InlineKeyboardButton("🔎 Preguntarle a Lía con IA", callback_data='bitacora_search')],
                [InlineKeyboardButton("✍️ Nueva Anotación", callback_data='bitacora_add')],
                [InlineKeyboardButton("🔙 Volver a Bitácora", callback_data='modo_bitacora')]
            ]
            await msg_wait.edit_text(texto_ultimos, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error obteniendo ultimas notas: {e}")
            await msg_wait.edit_text(f"❌ Error al obtener notas: {e}")
    elif query.data == 'bita_confirm':
        cache = user_data_cache.get(user_id, {})
        draft = cache.get("bitacora_draft")
        if not draft:
            await query.answer("⚠️ No hay borrador pendiente para guardar.", show_alert=True)
            return
            
        await query.answer("💾 Guardando en Bitácora...")
        msg_save = await query.message.reply_text("⏳ Guardando registro en Google Sheets...")
        
        try:
            from datetime import datetime, timezone, timedelta
            PET = timezone(timedelta(hours=-5))
            timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M")
            username = query.from_user.username or query.from_user.first_name
            
            file_path = draft.get("file_path")
            mime_type = draft.get("mime_type", "")
            enlace_drive = ""
            
            if file_path and os.path.exists(file_path):
                enlace_drive = await async_subir_a_drive(file_path, mime_type)
                try: os.remove(file_path)
                except: pass
            
            cat = draft.get("categoria", "GENERAL")
            tags = draft.get("tags", "")
            formato = draft.get("formato", "Texto/Anotación")
            texto_final = draft.get("texto", "")
            
            await guardar_registro_bitacora(timestamp, username, cat, tags, formato, enlace_drive, texto_final)
            
            user_states[user_id] = None
            user_data_cache[user_id] = {}
            
            cat_emoji, cat_lbl = CATEGORIAS_INFO.get(cat, ("📝", cat))
            resp_exito = (
                f"✅ **Anotación guardada en Bitácora con éxito**\n\n"
                f"📅 **Fecha:** `{timestamp}`\n"
                f"📂 **Categoría:** `{cat_emoji} {cat}`\n"
            )
            if tags:
                resp_exito += f"🏷️ **Tags:** `{tags}`\n"
            resp_exito += f"📝 **Contenido:** _{texto_final}_\n"
            if enlace_drive:
                resp_exito += f"📎 [Ver Archivo en Drive]({enlace_drive})\n"
                
            kb_post = [
                [InlineKeyboardButton("✍️ Otra Anotación", callback_data='bitacora_add')],
                [InlineKeyboardButton("🔎 Buscar en Bitácora", callback_data='bitacora_search')],
                [InlineKeyboardButton("🔙 Menú Bitácora", callback_data='modo_bitacora')]
            ]
            await msg_save.edit_text(resp_exito, reply_markup=InlineKeyboardMarkup(kb_post), parse_mode='Markdown', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error guardando bitácora: {e}")
            await msg_save.edit_text(f"❌ Error al guardar en Bitácora: {e}")
    elif query.data == 'bita_edit_text':
        user_states[user_id] = MODO_BITACORA_EDIT_TEXT
        kb = [[InlineKeyboardButton("🔙 Volver al Borrador", callback_data='bita_back_draft')]]
        await safe_edit_or_reply(
            query,
            "✏️ **Editando Contenido de la Anotación**\n\n"
            "Por favor, escribe y envíame el nuevo texto que deseas que quede registrado:\n\n"
            "💡 _Puedes corregir nombres, agregar detalles o redactarlo como prefieras._",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
    elif query.data == 'bita_pick_cat':
        kb_cat = [
            [InlineKeyboardButton("📜 Procedimiento", callback_data="bita_setcat|PROCEDIMIENTO"),
             InlineKeyboardButton("👤 Cliente/Prov", callback_data="bita_setcat|CLIENTE")],
            [InlineKeyboardButton("🚚 Transporte/Op", callback_data="bita_setcat|TRANSPORTE"),
             InlineKeyboardButton("⚠️ Incidencia", callback_data="bita_setcat|INCIDENCIA")],
            [InlineKeyboardButton("💰 Finanzas/Pagos", callback_data="bita_setcat|FINANZAS"),
             InlineKeyboardButton("📌 Recordatorio", callback_data="bita_setcat|RECORDATORIO")],
            [InlineKeyboardButton("📝 General/Otro", callback_data="bita_setcat|GENERAL")],
            [InlineKeyboardButton("🔙 Volver al Borrador", callback_data="bita_back_draft")]
        ]
        await safe_edit_or_reply(
            query,
            "🏷️ **Selecciona la Categoría Adecuada:**",
            reply_markup=InlineKeyboardMarkup(kb_cat),
            parse_mode='Markdown'
        )
    elif query.data.startswith('bita_setcat|'):
        nueva_cat = query.data.split('|')[1]
        cache = user_data_cache.get(user_id, {})
        draft = cache.get("bitacora_draft")
        if draft:
            draft["categoria"] = nueva_cat
            card_text, kb_card = build_bitacora_draft_card(draft)
            await safe_edit_or_reply(query, card_text, reply_markup=kb_card, parse_mode='Markdown')
        else:
            await query.answer("No hay borrador activo.", show_alert=True)
    elif query.data == 'bita_back_draft':
        cache = user_data_cache.get(user_id, {})
        draft = cache.get("bitacora_draft")
        if draft:
            user_states[user_id] = MODO_BITACORA_ADD
            card_text, kb_card = build_bitacora_draft_card(draft)
            await safe_edit_or_reply(query, card_text, reply_markup=kb_card, parse_mode='Markdown')
        else:
            await query.answer("No hay borrador activo.", show_alert=True)
    elif query.data == 'bita_cancel':
        cache = user_data_cache.get(user_id, {})
        draft = cache.get("bitacora_draft", {})
        file_path = draft.get("file_path")
        if file_path and os.path.exists(file_path):
            try: os.remove(file_path)
            except: pass
            
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        
        kb_back = [[InlineKeyboardButton("🔙 Volver a Bitácora", callback_data='modo_bitacora')]]
        await safe_edit_or_reply(query, "❌ **Anotación cancelada.** No se guardó ningún registro.", reply_markup=InlineKeyboardMarkup(kb_back), parse_mode='Markdown')
    elif query.data == 'man_sg_nueva':
        cache = user_data_cache.get(user_id, {})
        await process_manual_singuia_decision(update, context, user_id, cache, force_update=False)
    elif query.data == 'man_sg_actualizar':
        cache = user_data_cache.get(user_id, {})
        await process_manual_singuia_decision(update, context, user_id, cache, force_update=True)
# ====================================================================
# --- HANDLER DE TEXTO Y BÚSQUEDA ---
# ====================================================================

async def process_manual_singuia_decision(update, context, user_id, cache, force_update):
    msg_id = cache.get('msg_id')
    try:
        def save_manual():
            creds = obtener_credenciales()
            client = gspread.authorize(creds)
            book2 = client.open_by_key(SHEET_ID)
            sheet_recibidas = book2.worksheet("Guias_recibidas")
            row_data = [
                cache.get('fecha', ''),
                cache.get('num_guia', ''),
                cache.get('tipo_guia', ''),
                cache.get('empresa', ''),
                cache.get('fundo', ''),
                cache.get('enlace_drive', '')
            ]
            return sync_upsert_row(sheet_recibidas, cache.get('num_guia', ''), row_data, col_guia_index=2, col_comentario_index=7, allow_singuia_update=force_update)
        resultado = await asyncio.to_thread(save_manual)
        estado = "🔄 *Guía Actualizada*" if resultado == "updated" else "✅ *Nueva Guía Registrada Manualmente*"
        enlace = cache.get('enlace_drive', '')
        num_guia_saved = cache.get('num_guia', 'S/D')
        kb_obs = [
            [InlineKeyboardButton("Guía hecha", callback_data=f"obs|hecha_man|{num_guia_saved}")],
            [InlineKeyboardButton("Solo certificado", callback_data=f"obs|solocert|{num_guia_saved}|MANUAL_WAIT")],
            [InlineKeyboardButton("Escribir manualmente", callback_data=f"obs|escribir|{num_guia_saved}|MANUAL_WAIT")],
            [InlineKeyboardButton("❌ Sin Observación", callback_data=f"obs|cancelar|{num_guia_saved}")]
        ]
        await context.bot.edit_message_text(
            f"{estado}\n\n"
            f"📅 **Fecha:** `{cache.get('fecha', 'S/D')}`\n"
            f"📄 **N° Guía:** `{num_guia_saved}`\n"
            f"🏷️ **Tipo:** `{cache.get('tipo_guia', 'S/D')}`\n"
            f"🏢 **Empresa:** `{cache.get('empresa', 'S/D')}`\n"
            f"🏡 **Fundo:** `{cache.get('fundo', 'S/D')}`\n\n"
            f"📁 [Ver en Drive]({enlace})",
            chat_id=user_id, message_id=msg_id,
            parse_mode='Markdown', disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(kb_obs)
        )
        
        if user_id not in MEMORIA_VINCULACION:
            MEMORIA_VINCULACION[user_id] = []
        MEMORIA_VINCULACION[user_id].append({
            "num_guia": cache.get('num_guia', 'S/D'),
            "fundo": cache.get('fundo', 'S/D'),
            "message_id": cache.get('img_message_id'),
            "bot_message_id": msg_id,
            "enlace_drive": cache.get('enlace_drive', '')
        })
        
        # --- PROGRAMACIÓN DE RECORDATORIOS PARA MANUAL SINGUIA ---
        empresa_upper = str(cache.get('empresa', '')).upper()
        if "PROSEMBRA" in empresa_upper:
            context.job_queue.run_once(prosembra_notification_job, 30 * 60, chat_id=user_id, data={"guia": num_guia_saved})
        elif "LOS OLIVOS" in empresa_upper:
            context.job_queue.run_once(olivos_notification_job, 30 * 60, chat_id=user_id, data={"guia": num_guia_saved})
        if len(MEMORIA_VINCULACION[user_id]) > 5:
            MEMORIA_VINCULACION[user_id].pop(0)
        save_memoria_vinculacion(MEMORIA_VINCULACION)
            
        user_states[user_id] = None
        user_data_cache[user_id] = {}
    except Exception as e:
        logger.error(f"Error en guardado manual sg: {e}")

async def _procesar_resultado_coti_ia(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, msg_status, coti_info: dict):
    if not coti_info or not coti_info.get("items"):
        kb = [
            [InlineKeyboardButton("🎙️ Reintentar dictado", callback_data='coti_ia')],
            [InlineKeyboardButton("🔙 Volver a Cotizaciones", callback_data='menu_cotizaciones')]
        ]
        obs = coti_info.get("observaciones", "") if coti_info else ""
        aviso = f"\n\n<i>Detalle: {html.escape(obs)}</i>" if obs else ""
        await msg_status.edit_text(
            f"⚠️ <b>No se pudieron detectar residuos o precios</b> en tu mensaje.{aviso}\n\n"
            f"Por favor indica la empresa y al menos un residuo con su precio.\n"
            f"<i>Ej: 'Cotización para Pesquera Exalmar: Cartón 0.10 y Film 0.80'</i>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    from core.cotizaciones_service import get_fecha_formato_peru
    corr_sig = str(obtener_siguiente_correlativo()).strip()
    cliente = str(coti_info.get("cliente", "")).strip().upper()
    ruc = str(coti_info.get("ruc", "")).strip()
    direccion = str(coti_info.get("direccion", "")).strip().upper()
    items = coti_info.get("items", [])

    std_items = []
    for idx, it in enumerate(items, start=1):
        p_raw = str(it.get("precio", "0.00")).strip()
        if not p_raw.startswith("S/"):
            p_raw = "S/." + p_raw.replace("S/.", "").replace("S/", "").strip()
        std_items.append({
            "item": str(idx),
            "descripcion": str(it.get("descripcion", "")).strip().upper(),
            "unidad": str(it.get("unidad", "KG")).strip().upper(),
            "precio": p_raw
        })

    payload = {
        "correlativo": corr_sig,
        "fecha": get_fecha_formato_peru(),
        "cliente": cliente,
        "ruc": ruc,
        "direccion": direccion,
        "responsable": "Alexander Chamochumbi Chávez",
        "cargo": "Responsable Técnico",
        "items": std_items,
        "user_id": user_id
    }

    context.user_data[f"coti_ia_{user_id}"] = payload
    url_edit = obtener_url_webapp(correlativo=corr_sig, datos_edicion=payload, user_id=user_id)

    items_lines = []
    for it in std_items:
        items_lines.append(f"  • <b>{html.escape(it['descripcion'])}</b>: {html.escape(it['unidad'])} — <code>{html.escape(it['precio'])}</code>")
    resumen_items = "\n".join(items_lines)

    texto_preview = (
        f"📋 <b>Cotización Detectada por IA</b>\n\n"
        f"📌 <b>Correlativo asignado:</b> <code>{corr_sig}</code>\n"
        f"🏢 <b>Cliente:</b> <code>{html.escape(cliente or 'POR DEFINIR')}</code>\n"
        f"🆔 <b>RUC:</b> <code>{html.escape(ruc or 'No especificado')}</code>\n"
        f"📍 <b>Dirección:</b> <code>{html.escape(direccion or 'No especificada')}</code>\n\n"
        f"💰 <b>Residuos cotizados ({len(std_items)}):</b>\n"
        f"{resumen_items}\n\n"
        f"¿Deseas generar el PDF directamente o revisarlo en la Mini App?"
    )

    kb = [
        [InlineKeyboardButton("🚀 Confirmar y Generar PDF", callback_data=f"coti_ia_gen|{corr_sig}")],
        [InlineKeyboardButton("✏️ Revisar en Mini App", web_app=WebAppInfo(url=url_edit))],
        [InlineKeyboardButton("🎙️ Reintentar dictado", callback_data='coti_ia'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
    ]

    await msg_status.edit_text(texto_preview, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    text = update.message.text.strip()
    user_id = update.effective_user.id
    modo = user_states.get(user_id)

    if modo == MODO_PENDIENTE_ADD:
        user_states[user_id] = None
        texto_input = text.strip()
        msg = await update.message.reply_text("🧠 Analizando tarea con IA...")
        try:
            info = await procesar_pendiente_ia(texto=texto_input)
            draft = {
                "titulo": info.get("titulo", texto_input[:50]),
                "detalle": info.get("detalle", ""),
                "cliente_ref": info.get("cliente_ref", ""),
                "fecha_alerta": info.get("fecha_alerta", ""),
                "prioridad": info.get("prioridad", "MEDIA")
            }
            user_data_cache[user_id] = {"pendiente_draft": draft}
            card_text, kb_card = build_pendiente_draft_card(draft)
            await msg.edit_text(card_text, reply_markup=kb_card, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error procesando pendiente IA: {e}")
            await msg.edit_text(f"❌ Error al procesar pendiente: {html.escape(str(e))}")
        return

    elif modo == MODO_CREDENCIAL_BUSCAR:
        user_states[user_id] = None
        q = text.strip()
        msg = await update.message.reply_text(f"🔍 Buscando credencial <code>{html.escape(q)}</code> en Google Sheets...", parse_mode='HTML')
        try:
            res = await async_obtener_credenciales(query=q)
            if not res:
                url_panel_crd = obtener_url_webapp_credenciales(user_id=user_id)
                kb = [
                    [InlineKeyboardButton("🔍 Nueva Búsqueda", callback_data='crd_buscar')],
                    [InlineKeyboardButton("📱 Abrir Bóveda WebApp", web_app=WebAppInfo(url=url_panel_crd))],
                    [InlineKeyboardButton("🔙 Menú Credenciales", callback_data='menu_credenciales')]
                ]
                await msg.edit_text(f"❌ No se encontró ninguna credencial que coincida con <code>{html.escape(q)}</code>.", reply_markup=InlineKeyboardMarkup(kb), parse_mode='HTML')
                return

            texto_res = f"🔐 <b>Resultados para:</b> <code>{html.escape(q)}</code>\n\n"
            for c in res[:6]:
                srv = html.escape(str(c.get('SERVICIO', '')).strip())
                cat = html.escape(str(c.get('CATEGORIA', 'General')).strip())
                usr = html.escape(str(c.get('USUARIO_RUC', '')).strip())
                pwd = html.escape(str(c.get('CONTRASEÑA', '')).strip())
                url_log = str(c.get('URL_LOGIN', '')).strip()
                pin = html.escape(str(c.get('PIN_EXTRA', '')).strip())
                obs = html.escape(str(c.get('OBSERVACIONES', '')).strip())

                texto_res += f"🏛️ <b>{srv}</b> <code>[{cat}]</code>\n"
                if url_log:
                    url_esc = html.escape(url_log)
                    texto_res += f"🌐 Enlace: {url_esc}\n"
                if usr:
                    texto_res += f"👤 Usuario: <code>{usr}</code>\n"
                if pwd:
                    texto_res += f"🔑 Clave: <code>{pwd}</code>\n"
                if pin:
                    texto_res += f"📌 PIN: <code>{pin}</code>\n"
                if obs:
                    texto_res += f"💡 <i>{obs}</i>\n"
                texto_res += "──────────────────\n"

            url_panel_crd = obtener_url_webapp_credenciales(user_id=user_id)
            kb = [
                [InlineKeyboardButton("📱 Ver en WebApp", web_app=WebAppInfo(url=url_panel_crd))],
                [InlineKeyboardButton("🔍 Otra Búsqueda", callback_data='crd_buscar'), InlineKeyboardButton("🔙 Menú Credenciales", callback_data='menu_credenciales')]
            ]
            await msg.edit_text(texto_res, reply_markup=InlineKeyboardMarkup(kb), parse_mode='HTML', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error buscando credencial: {e}")
            await msg.edit_text(f"❌ Error en búsqueda de credencial: {html.escape(str(e))}")
        return

    elif modo == MODO_COTIZACION_BUSCAR:
        user_states[user_id] = None
        corr_query = text.strip()
        msg_wait = await update.message.reply_text(f"🔍 Buscando cotización `{corr_query}` en Google Sheets...")
        try:
            coti_data = await async_buscar_cotizacion_por_correlativo(corr_query)
            if not coti_data:
                await msg_wait.edit_text(
                    f"❌ No se encontró ninguna cotización con el número `{corr_query}`.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Volver a Cotizaciones", callback_data='menu_cotizaciones'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
                    ])
                )
                return

            corr = coti_data.get("correlativo", corr_query)
            cod = coti_data.get("codigo", f"{corr}-2026-EO-RS")
            cli = coti_data.get("cliente", "")
            fec = coti_data.get("fecha", "")
            doc_link = coti_data.get("doc_link", "")
            pdf_link = coti_data.get("pdf_link", "")

            url_edit = obtener_url_webapp(correlativo=corr, datos_edicion=coti_data, user_id=user_id)

            kb = [
                [InlineKeyboardButton("✏️ Modificar Cotización", web_app=WebAppInfo(url=url_edit))]
            ]
            links_row = []
            if doc_link and doc_link.startswith("http"):
                links_row.append(InlineKeyboardButton("📄 Doc Editable", url=doc_link))
            if pdf_link and pdf_link.startswith("http"):
                links_row.append(InlineKeyboardButton("📂 PDF en Drive", url=pdf_link))
            if links_row:
                kb.append(links_row)
            kb.append([InlineKeyboardButton("🔄 Sincronizar PDF desde Doc", callback_data=f"coti_sync|{corr}")])
            kb.append([InlineKeyboardButton("🔙 Volver a Cotizaciones", callback_data='menu_cotizaciones'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')])

            texto_coti = (
                f"📋 *Cotización Encontrada*\n\n"
                f"• *Código:* `COTIZACION N°{cod}`\n"
                f"• *Cliente:* `{cli}`\n"
                f"• *RUC:* `{coti_data.get('ruc', '')}`\n"
                f"• *Fecha:* `{fec}`\n"
                f"• *Dirección:* `{coti_data.get('direccion', '')}`\n\n"
                f"Puedes presionar *✏️ Modificar Cotización* para cambiar cualquier dato o ver los enlaces en Drive:"
            )
            await msg_wait.edit_text(texto_coti, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
        except Exception as e:
            logger.error(f"Error buscando cotización: {e}")
        return

    elif modo == MODO_COTIZACION_IA:
        user_states[user_id] = None
        msg_wait = await update.message.reply_text("🧠 Analizando cotización con IA...")
        try:
            coti_info = await interpretar_cotizacion_ia(texto=text)
            await _procesar_resultado_coti_ia(update, context, user_id, msg_wait, coti_info)
        except Exception as e:
            logger.error(f"Error interpretando texto para cotización: {e}")
            await msg_wait.edit_text(f"❌ Error al analizar la cotización: {e}")
        return

    if modo == MODO_FACTURAS_BUSCAR:
        user_states[user_id] = None
        q = text.strip()
        msg_wait = await update.message.reply_text(f"🔍 Buscando factura `{q}` en Registro_Facturas...")
        try:
            resultados = await async_buscar_facturas_en_sheet(q)
            if not resultados:
                await msg_wait.edit_text(
                    f"❌ No se encontraron facturas con el criterio `{q}`.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Volver a Facturas", callback_data='menu_facturas'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
                    ])
                )
                return

            resp_text = f"🧾 *Facturas Encontradas ({len(resultados)}):*\n\n"
            for f in resultados[:5]:
                mon_simb = "S/." if f.get("moneda") == "PEN" else "$"
                tot = f.get('importe_total', 0.0)
                tot_num = float(tot) if isinstance(tot, (int, float)) or (isinstance(tot, str) and tot.replace('.', '', 1).isdigit()) else 0.0
                resp_text += (
                    f"📄 *Factura:* `{f.get('numero_factura')}`\n"
                    f"🏢 *Emisor:* {f.get('emisor_nombre')} (RUC: `{f.get('emisor_ruc')}`)\n"
                    f"📅 *Fecha:* {f.get('fecha_emision')} | 💵 *Total:* {mon_simb} {tot_num:,.2f}\n"
                )
                if f.get("enlace_drive"):
                    resp_text += f"📁 [Ver en Drive]({f.get('enlace_drive')})\n"
                resp_text += "────────────────────\n"

            resp_text += f"\n📊 [Abrir Sheet Completo]({SHEET_URL_DIRECT})"
            kb = [
                [InlineKeyboardButton("🔍 Nueva Búsqueda", callback_data='modo_facturas_buscar'), InlineKeyboardButton("🔙 Facturas", callback_data='menu_facturas')]
            ]
            await msg_wait.edit_text(resp_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb), disable_web_page_preview=True)
            return
        except Exception as e:
            logger.error(f"Error buscando facturas: {e}")
            await msg_wait.edit_text(f"❌ Error buscando facturas: {e}")
            return
    
    if modo in [MODO_REPORTE_REGISTRO, MODO_REPORTE_RECIBIDAS]:
        raw_query = str(update.message.text).strip()
        query_upper = raw_query.upper()
        
        def simplificar_guia(g_str):
            if "-" in g_str:
                p = g_str.split("-")
                if len(p) == 2:
                    return f"{p[0]}-{p[1].lstrip('0')}"
            return g_str
            
        num_guia_simplificado = simplificar_guia(query_upper)

        msg = await update.message.reply_text(f"⏳ Buscando `{raw_query}` en la base de datos seleccionada...")
        try:
            if not rc.sheet_control: await asyncio.to_thread(conectar_servicios)
            
            todos_registros = []
            
            if modo == MODO_REPORTE_REGISTRO:
                registros_1 = await async_get_all_records(rc.sheet_control)
                for r in registros_1:
                    r["_origen"] = "Registro_Guias"
                    todos_registros.append(r)
            else:
                def fetch_recibidas():
                    creds = obtener_credenciales()
                    client = gspread.authorize(creds)
                    book2 = client.open_by_key(SHEET_ID)
                    ws = book2.worksheet("Guias_recibidas")
                    rows = ws.get_all_values()
                    if not rows:
                        return []
                    headers = rows[0]
                    records = []
                    for row in rows[1:]:
                        rec = {}
                        for idx, h in enumerate(headers):
                            if h:
                                val = row[idx] if idx < len(row) else ""
                                if h in rec:
                                    rec[f"{h}_{idx+1}"] = val
                                else:
                                    rec[h] = val
                        records.append(rec)
                    return records
                    
                try:
                    registros_2 = await asyncio.to_thread(fetch_recibidas)
                except Exception as e:
                    logger.error(f"Error cargando Guias_recibidas: {e}")
                    registros_2 = []
                for r in registros_2:
                    r["_origen"] = "Guias_recibidas"
                    todos_registros.append(r)

            encontrados = []
            for r in todos_registros:
                fecha_str = str(r.get('Fecha', '')).strip()
                guia_str = str(r.get('N° Guía', r.get('Numero Guia', r.get('Nro Guia', '')))).strip()
                
                guia_str_simplificada = simplificar_guia(guia_str.upper())
                
                if (raw_query in fecha_str) or (num_guia_simplificado == guia_str_simplificada) or (query_upper in guia_str.upper()):
                    encontrados.append(r)
            
            if encontrados:
                # Descargar registros de Historial una sola vez para cruzar certificados
                registros_historial = await async_obtener_historial_certificados()

                reporte = f"✅ **REPORTE: {len(encontrados)} Coincidencias Encontradas**\n\n"
                for r in encontrados:
                    origen = r.get('_origen')
                    num_guia = str(r.get('N° Guía', r.get('Numero Guia', r.get('Nro Guia', 'S/D'))))
                    tipo_guia = r.get('Tipo Guía', r.get('Tipo', 'S/D'))
                    empresa = r.get('Empresa Principal', r.get('Empresa', 'S/D'))
                    guia_ligada = str(r.get('Guía ligada', r.get('Guia ligada', ''))).strip()
                    
                    if origen == "Registro_Guias":
                        entidad_1 = r.get('Destinatario/Remitente', 'S/D')
                        entidad_2 = r.get('Destinario/Proveedor', 'S/D')
                        # Columna I: 'Guia hecha' | Columna J: 'Guia recibida'
                        enlace = str(r.get('Guia hecha', r.get('GUIA HECHA', r.get('Link Drive', '')))).strip()
                        enlace_recibida = str(r.get('Guia recibida', r.get('GUIA RECIBIDA', ''))).strip()
                    else:
                        vals = list(r.values())
                        entidad_1 = str(vals[4]) if len(vals) >= 5 else "S/D"
                        entidad_2 = "S/D"
                        enlace = str(r.get('Link Drive', r.get('LINK DRIVE', ''))).strip()
                        enlace_recibida = ""

                    reporte += f"🗂️ **Base:** `{origen}`\n"
                    reporte += f"📄 **Guía:** `{num_guia}`\n"
                    if guia_ligada and guia_ligada != "S/D":
                        reporte += f"🔗 **Guía Ligada:** `{guia_ligada}`\n"
                    reporte += f"🏷️ **Tipo:** `{tipo_guia}`\n"
                    reporte += f"🏢 **Empresa:** `{empresa}`\n"
                    if origen == "Registro_Guias":
                        reporte += f"👤 **Dest/Rem:** `{entidad_1}`\n"
                        reporte += f"👤 **Dest/Prov:** `{entidad_2}`\n"
                    else:
                        reporte += f"🏡 **Fundo/Planta:** `{entidad_1}`\n"
                    
                    label_link = "Guía Hecha" if origen == "Registro_Guias" else "Link"
                    if enlace.startswith("http"):
                        reporte += f"🔗 [{label_link}]({enlace})\n"
                    elif enlace:
                        link_rescatado = await async_buscar_link_en_drive(enlace)
                        if link_rescatado:
                            reporte += f"🔗 [{label_link}]({link_rescatado})\n"
                        else:
                            reporte += f"🔗 _Documento no encontrado en Drive_\n"
                    else:
                        reporte += f"🔗 _Sin enlace en base de datos_\n"

                    if origen == "Registro_Guias" and enlace_recibida:
                        if enlace_recibida.startswith("http"):
                            reporte += f"📎 [Guía Recibida]({enlace_recibida})\n"
                        else:
                            link_rec_resc = await async_buscar_link_en_drive(enlace_recibida)
                            if link_rec_resc:
                                reporte += f"📎 [Guía Recibida]({link_rec_resc})\n"

                    # --- VERIFICACIÓN DE CERTIFICADO ---
                    certs_asociados = []
                    if registros_historial:
                        for h in registros_historial:
                            guia_hist = str(h.get('Guia', h.get('GUIA', ''))).strip()
                            if not guia_hist:
                                continue
                            if match_guia_en_texto(num_guia, guia_hist) or (guia_ligada and match_guia_en_texto(guia_ligada, guia_hist)):
                                certs_asociados.append(h)

                    # Respaldo: verificar columna Certificados de la fila (si no es marca de tiempo del bot)
                    cert_col_raw = str(r.get('Certificados', r.get('CERTIFICADOS', ''))).strip()
                    cert_en_fila = ""
                    if cert_col_raw and not any(term in cert_col_raw.upper() for term in ["NUEVO", "ACTUALIZADO", "S/D", "NONE"]):
                        cert_en_fila = cert_col_raw

                    if certs_asociados:
                        reporte += "📜 **Certificado:** ✅ HECHO / EMITIDO\n"
                        for c in certs_asociados[:2]:
                            c_num = str(c.get('Certificado', '')).strip()
                            c_corr = str(c.get('Correlativo', '')).strip()
                            c_fec = str(c.get('Fecha de emision', '')).strip()
                            c_link = str(c.get('Link Documento', '')).strip()

                            detalles_cert = []
                            if c_num and c_num != "S/D":
                                detalles_cert.append(f"`{c_num}`")
                            if c_corr and c_corr != "S/D" and c_corr not in c_num:
                                detalles_cert.append(f"Correlativo: `{c_corr}`")
                            if c_fec and c_fec != "S/D":
                                detalles_cert.append(f"Fecha: `{c_fec}`")

                            if detalles_cert:
                                reporte += f"   • {' | '.join(detalles_cert)}\n"
                            
                            if c_link:
                                if c_link.startswith("http"):
                                    reporte += f"   📂 [Abrir Certificado]({c_link})\n"
                                else:
                                    url_cert = await async_buscar_link_en_drive(c_link)
                                    if url_cert:
                                        reporte += f"   📂 [Abrir Certificado]({url_cert})\n"
                                    else:
                                        reporte += f"   📂 _Certificado en Drive: `{c_link}`_\n"
                    elif cert_en_fila:
                        reporte += f"📜 **Certificado:** ✅ HECHO (`{cert_en_fila}`)\n"
                    else:
                        reporte += "📜 **Certificado:** ⚠️ PENDIENTE / NO REALIZADO\n"
                    
                    reporte += "➖➖➖➖➖➖➖➖➖➖\n"
                
                if len(reporte) > 4000:
                    reporte = reporte[:4000] + "\n\n⚠️ _[Reporte recortado por límite de caracteres de Telegram]_"
                    
                try:
                    await msg.edit_text(reporte, parse_mode='Markdown', disable_web_page_preview=True)
                except Exception:
                    await msg.edit_text(reporte, disable_web_page_preview=True)
            else: 
                await msg.edit_text("❌ No se encontraron resultados para esa búsqueda.")
        except Exception as e: 
            logger.error(f"Error en búsqueda: {e}")
            await msg.edit_text(f"❌ Error en la búsqueda: {e}")

    elif modo == MODO_COMENTAR_GUIA:
        num_guia_raw = str(update.message.text).strip().upper()
        if "-" in num_guia_raw:
            partes = num_guia_raw.split("-")
            if len(partes) == 2:
                num_guia_normalizado = f"{partes[0]}-{partes[1].zfill(8)}"
            else:
                num_guia_normalizado = num_guia_raw
        else:
            num_guia_normalizado = num_guia_raw

        msg = await update.message.reply_text(f"⏳ Buscando guía `{num_guia_normalizado}` en el Excel...")
        try:
            if not rc.sheet_control: await asyncio.to_thread(conectar_servicios)
            col_values = await asyncio.to_thread(rc.sheet_control.col_values, 2)
            
            if num_guia_normalizado in col_values:
                user_states[user_id] = MODO_COMENTAR_TEXTO
                user_data_cache[user_id] = {'guia_target': num_guia_normalizado}
                await msg.edit_text(f"✅ Guía `{num_guia_normalizado}` encontrada.\n\n✍️ Escribe a continuación la **observación o comentario** que deseas guardar:")
            else:
                await msg.edit_text(f"❌ La guía `{num_guia_normalizado}` NO existe en el registro. Verifica el número y vuelve a intentar pulsando el botón del menú.")
        except Exception as e:
            await msg.edit_text(f"❌ Error al buscar la guía: {e}")

    elif modo == MODO_COMENTAR_TEXTO:
        comentario = str(update.message.text).strip()
        num_guia = user_data_cache.get(user_id, {}).get('guia_target')
        
        if not num_guia:
            await update.message.reply_text("❌ Error de memoria. Por favor, selecciona 'Añadir Observación' en el menú nuevamente.")
            return

        msg = await update.message.reply_text("⏳ Guardando observación en el Excel...")
        try:
            def update_col_j():
                col_values = rc.sheet_control.col_values(2)
                row_idx = col_values.index(num_guia) + 1
                rc.sheet_control.update_cell(row_idx, 12, normalizar_valor_upper(comentario))

            await asyncio.to_thread(update_col_j)
            await async_log_action(user_id, num_guia, "COMENTARIO_MANUAL_GUARDADO")
            
            user_states[user_id] = None 
            user_data_cache[user_id] = {}
            
            await msg.edit_text(f"✅ ¡Listo! Observación guardada exitosamente para la guía `{num_guia}`:\n\n_{comentario}_", parse_mode='Markdown')
        except Exception as e:
            await msg.edit_text(f"❌ Error al guardar el comentario en el Excel: {e}")

    elif modo in [MODO_BUSCAR_CERT_FECHA, MODO_BUSCAR_CERT_FUNDO, MODO_BUSCAR_CERT_CORRE, MODO_BUSCAR_CERT_EMPRESA]:
        raw_query = str(update.message.text).strip()
        query_upper = raw_query.upper()
        msg = await update.message.reply_text(f"⏳ Buscando `{raw_query}` estrictamente en la categoría seleccionada...")
        
        try:
            registros = await async_obtener_historial_certificados()
            encontrados = []
            
            for r in registros:
                fecha = str(r.get('Fecha de emision', '')).strip()
                empresa = str(r.get('Empresa', '')).strip().upper()
                fundo = str(r.get('Fundo', '')).strip().upper()
                correlativo = str(r.get('Correlativo', '')).strip().upper()
                
                match = False
                if modo == MODO_BUSCAR_CERT_FECHA and (raw_query in fecha):
                    match = True
                elif modo == MODO_BUSCAR_CERT_FUNDO and (query_upper in fundo):
                    match = True
                elif modo == MODO_BUSCAR_CERT_EMPRESA and (query_upper in empresa):
                    match = True
                elif modo == MODO_BUSCAR_CERT_CORRE and (query_upper in correlativo):
                    match = True
                    
                if match:
                    encontrados.append(r)
            
            if encontrados:
                reporte = f"✅ **REPORTE: {len(encontrados)} Certificados Encontrados**\n\n"
                for r in encontrados:
                    fecha_val = str(r.get('Fecha de emision', 'S/D'))
                    empresa_val = str(r.get('Empresa', 'S/D'))
                    fundo_val = str(r.get('Fundo', 'S/D'))
                    correlativo_val = str(r.get('Correlativo', 'S/D'))
                    certificado_val = str(r.get('Certificado', 'S/D'))
                    guia_val = str(r.get('Guia', 'S/D'))
                    link_guia = str(r.get('Link Guia', '')).strip()
                    link_doc = str(r.get('Link Documento', '')).strip()
                    
                    reporte += f"📅 **Fecha:** `{fecha_val}`\n"
                    reporte += f"🏢 **Empresa:** `{empresa_val}`\n"
                    reporte += f"🏡 **Fundo:** `{fundo_val}`\n"
                    reporte += f"🔢 **Correlativo:** `{correlativo_val}`\n"
                    reporte += f"📜 **Certificado:** `{certificado_val}`\n"
                    reporte += f"📄 **Guía Relacionada:** `{guia_val}`\n"
                    
                    if link_guia:
                        if link_guia.startswith("http"):
                            reporte += f"📎 [Ver Guía]({link_guia})\n"
                        else:
                            url_guia = await async_buscar_link_en_drive(link_guia)
                            if url_guia: 
                                reporte += f"📎 [Ver Guía]({url_guia})\n"
                            else: 
                                reporte += f"📎 _Guía no encontrada en Drive_\n"
                            
                    if link_doc:
                        if link_doc.startswith("http"):
                            reporte += f"📎 [Abrir Certificado]({link_doc})\n"
                        else:
                            url_doc = await async_buscar_link_en_drive(link_doc)
                            if url_doc: 
                                reporte += f"📎 [Abrir Certificado]({url_doc})\n"
                            else: 
                                reporte += f"📎 _Certificado no encontrado en Drive_\n"
                        
                    reporte += "➖➖➖➖➖➖➖➖➖➖\n"
                
                if len(reporte) > 4000:
                    reporte = reporte[:4000] + "\n\n⚠️ _[Reporte recortado por límite de caracteres]_"
                    
                await msg.edit_text(reporte, parse_mode='Markdown', disable_web_page_preview=True)
            else:
                await msg.edit_text("❌ No se encontraron certificados con ese término.")
        except Exception as e:
            logger.error(f"Error en búsqueda de certificados: {e}")
            await msg.edit_text(f"❌ Error en la búsqueda: {e}")
            
    elif modo in [MODO_DIR_EMPRESA, MODO_DIR_FUNDO]:
        raw_query = str(update.message.text).strip()
        query_upper = raw_query.upper()
        msg = await update.message.reply_text(f"⏳ Buscando direcciones para `{raw_query}` en la agenda...")
        
        try:
            def fetch_direcciones():
                creds = obtener_credenciales()
                if not creds:
                    raise Exception("No se encontraron las credenciales de Google. Verifica GOOGLE_CREDENTIALS_JSON en Render.")
                client = gspread.authorize(creds)
                book2 = client.open_by_key(SHEET_ID)
                try:
                    return book2.worksheet("Direcciones").get_all_records()
                except gspread.exceptions.WorksheetNotFound:
                    return []
                
            registros = await asyncio.to_thread(fetch_direcciones)
            encontrados = []
            
            for r in registros:
                empresa = str(r.get('EMPRESA', '')).strip().upper()
                fundo = str(r.get('FUNDO/PLANTA', '')).strip().upper()
                
                if modo == MODO_DIR_EMPRESA and (query_upper in empresa):
                    encontrados.append(r)
                elif modo == MODO_DIR_FUNDO and (query_upper in fundo):
                    encontrados.append(r)
                    
            if encontrados:
                reporte = f"✅ **{len(encontrados)} Direcciones Encontradas**\n\n"
                for r in encontrados:
                    empresa_val = str(r.get('EMPRESA', 'S/D'))
                    fundo_val = str(r.get('FUNDO/PLANTA', 'S/D'))
                    direccion_val = str(r.get('DIRECCION', 'S/D'))
                    
                    reporte += f"🏢 **Empresa:** `{empresa_val}`\n"
                    reporte += f"🏡 **Fundo:** `{fundo_val}`\n"
                    reporte += f"📍 **Dirección:** `{direccion_val}`\n"
                    reporte += "➖➖➖➖➖➖➖➖➖➖\n"
                    
                if len(reporte) > 4000:
                    reporte = reporte[:4000] + "\n\n⚠️ _[Reporte recortado]_"
                    
                await msg.edit_text(reporte, parse_mode='Markdown')
            else:
                await msg.edit_text("❌ No se encontraron direcciones con ese término.")
        except Exception as e:
            logger.error(f"Error en búsqueda de direcciones: {e}")
            await msg.edit_text(f"❌ Error en la búsqueda de directori: {e}")

    elif modo == MODO_BUSCAR_CLIENTE:
        raw_query = str(update.message.text).strip()
        msg = await update.message.reply_text(f"⏳ Buscando cliente `{raw_query}` en la base de datos...")
        
        try:
            def fetch_clientes():
                creds = obtener_credenciales()
                if not creds:
                    raise Exception("No se encontraron las credenciales de Google.")
                client = gspread.authorize(creds)
                book2 = client.open_by_key(SHEET_ID)
                ws_target = None
                for ws in book2.worksheets():
                    if ws.title.strip().upper() == "CLIENTES":
                        ws_target = ws
                        break
                if not ws_target:
                    return []
                return ws_target.get_all_records()
                
            registros = await asyncio.to_thread(fetch_clientes)
            
            matches_scored = []
            for r in registros:
                empresa_val = str(r.get('EMPRESA', '')).strip()
                is_match, score = match_company_flexible(raw_query, empresa_val)
                if is_match:
                    matches_scored.append((r, score))
                    
            matches_scored.sort(key=lambda x: x[1], reverse=True)
            encontrados = [m[0] for m in matches_scored]
            
            kb = [
                [InlineKeyboardButton("🔙 Menú Búsquedas", callback_data='menu_busqueda')]
            ]
            
            if encontrados:
                reporte = f"✅ **{len(encontrados)} Cliente(s) Encontrado(s)**\n\n"
                for r in encontrados:
                    empresa = str(r.get('EMPRESA', 'S/D')).strip()
                    ruc = str(r.get('RUC', '')).strip()
                    registro = str(r.get('REGISTRO', '')).strip()
                    dir_cert = str(r.get('DIRECCION CERTIFICADO', '')).strip()
                    dom_fiscal = str(r.get('DOMICILIO FISCAL', '')).strip()
                    dir1 = str(r.get('DIRECCION 1', '')).strip()
                    dir2 = str(r.get('DIRECCION 2', '')).strip()
                    
                    reporte += f"🏢 **Empresa:** `{empresa}`\n"
                    if ruc:
                        reporte += f"🆔 **RUC:** `{ruc}`\n"
                    if registro:
                        reporte += f"📝 **Registro:** `{registro}`\n"
                    if dir_cert:
                        reporte += f"📜 **Dir. Certificado:** `{dir_cert}`\n"
                    if dom_fiscal:
                        reporte += f"🏠 **Domicilio Fiscal:** `{dom_fiscal}`\n"
                    if dir1:
                        reporte += f"📍 **Dirección 1:** `{dir1}`\n"
                    if dir2:
                        reporte += f"📍 **Dirección 2:** `{dir2}`\n"
                    reporte += "➖➖➖➖➖➖➖➖➖➖\n"
                    
                if len(reporte) > 4000:
                    reporte = reporte[:4000] + "\n\n⚠️ _[Reporte recortado por límite de caracteres]_"
                    
                try:
                    await msg.edit_text(reporte, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
                except Exception:
                    await msg.edit_text(reporte, reply_markup=InlineKeyboardMarkup(kb))
            else:
                await msg.edit_text(
                    f"❌ No se encontró ningún cliente con `{raw_query}`.\n\nPuedes ingresar otro nombre o pulsar el botón para volver:",
                    reply_markup=InlineKeyboardMarkup(kb)
                )
        except Exception as e:
            logger.error(f"Error en búsqueda de clientes: {e}")
            await msg.edit_text(f"❌ Error en la búsqueda de clientes: {e}")

    elif modo == MODO_GUIAS_MANUAL_FECHA:
        fecha_raw = str(update.message.text).strip()
        fecha_formateada = fecha_raw.replace("/", "-").replace(".", "-")
        user_data_cache[user_id]['fecha'] = fecha_formateada
        user_states[user_id] = MODO_GUIAS_MANUAL_NUMGUIA
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_fecha')]]
        await update.message.reply_text("📝 Paso 2/5 — Escribe el N° de Guía (Ej: T001-44 o EG03-293):", reply_markup=InlineKeyboardMarkup(kb))

    elif modo == MODO_GUIAS_MANUAL_NUMGUIA:
        user_data_cache[user_id]['num_guia'] = str(update.message.text).strip().upper()
        user_states[user_id] = MODO_GUIAS_MANUAL_TIPO
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_numguia')]]
        await update.message.reply_text("🏷️ Paso 3/5 — Escribe el Tipo Guía (Ej: REMITENTE o TRANSPORTISTA):", reply_markup=InlineKeyboardMarkup(kb))

    elif modo == MODO_GUIAS_MANUAL_TIPO:
        user_data_cache[user_id]['tipo_guia'] = str(update.message.text).strip().upper()
        user_states[user_id] = MODO_GUIAS_MANUAL_EMPRESA
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_tipo')]]
        await update.message.reply_text("🏢 Paso 4/5 — Escribe la Empresa Principal:", reply_markup=InlineKeyboardMarkup(kb))

    elif modo == MODO_GUIAS_MANUAL_EMPRESA:
        user_data_cache[user_id]['empresa'] = str(update.message.text).strip().upper()
        user_states[user_id] = MODO_GUIAS_MANUAL_FUNDO
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='manual_volver_empresa')]]
        await update.message.reply_text("🏡 Paso 5/5 — Escribe el Fundo/Planta:", reply_markup=InlineKeyboardMarkup(kb))

    elif modo == MODO_GUIAS_MANUAL_FUNDO:
        user_data_cache[user_id]['fundo'] = str(update.message.text).strip().upper()
        cache = user_data_cache.get(user_id, {})
        
        num_guia = cache.get('num_guia', '')
        num_upper = num_guia.strip().upper()
        terminos_genericos = ["SIN GUIA", "SIN GUÍA", "S/D", "BALANZA", "TICKET"]
        is_singuia_match = any(term in num_upper for term in terminos_genericos) or num_upper in ["-", ""]
        
        if is_singuia_match:
            msg = await update.message.reply_text("⏳ Procesando decisión...")
            cache['msg_id'] = msg.message_id
            kb = [
                [InlineKeyboardButton("✅ Registrar como NUEVA", callback_data='man_sg_nueva')],
                [InlineKeyboardButton("🔄 Actualizar Existente", callback_data='man_sg_actualizar')],
                [InlineKeyboardButton("❌ Cancelar Registro", callback_data='man_sg_cancelar')]
            ]
            await msg.edit_text(
                "⚠️ **ATENCIÓN:** Has ingresado esta guía manualmente como SIN GUIA.\n¿Deseas registrarla como una guía NUEVA o ACTUALIZAR la primera guía SIN GUIA que encuentre en tu Excel?",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
            return
            
        msg = await update.message.reply_text("⏳ Guardando registro manual en Guias_recibidas...")
        try:
            def save_manual():
                creds = obtener_credenciales()
                client = gspread.authorize(creds)
                book2 = client.open_by_key(SHEET_ID)
                sheet_recibidas = book2.worksheet("Guias_recibidas")
                row_data = [
                    cache.get('fecha', ''),
                    str(cache.get('num_guia', '')).upper(),
                    str(cache.get('tipo_guia', '')).upper(),
                    str(cache.get('empresa', '')).upper(),
                    str(cache.get('fundo', '')).upper(),
                    cache.get('enlace_drive', '')
                ]
                return sync_upsert_row(sheet_recibidas, cache.get('num_guia', '').upper(), row_data, col_guia_index=2, col_comentario_index=7)
            resultado = await asyncio.to_thread(save_manual)
            estado = "🔄 *Guía Actualizada*" if resultado == "updated" else "✅ *Nueva Guía Registrada Manualmente*"
            enlace = cache.get('enlace_drive', '')
            num_guia_saved = cache.get('num_guia', 'S/D')
            kb_obs = [
                [InlineKeyboardButton("Guía hecha", callback_data=f"obs|hecha_man|{num_guia_saved}")],
                [InlineKeyboardButton("Solo certificado", callback_data=f"obs|solocert|{num_guia_saved}|MANUAL_WAIT")],
                [InlineKeyboardButton("Escribir manualmente", callback_data=f"obs|escribir|{num_guia_saved}|MANUAL_WAIT")],
                [InlineKeyboardButton("❌ Sin Observación", callback_data=f"obs|cancelar|{num_guia_saved}")]
            ]
            await msg.edit_text(
                f"{estado}\n\n"
                f"📅 **Fecha:** `{cache.get('fecha', 'S/D')}`\n"
                f"📄 **N° Guía:** `{num_guia_saved}`\n"
                f"🏷️ **Tipo:** `{cache.get('tipo_guia', 'S/D')}`\n"
                f"🏢 **Empresa:** `{cache.get('empresa', 'S/D')}`\n"
                f"🏡 **Fundo:** `{cache.get('fundo', 'S/D')}`\n\n"
                f"📁 [Ver en Drive]({enlace})",
                parse_mode='Markdown', disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(kb_obs)
            )
            
            # --- MEMORIA DE VINCULACIÓN HÍBRIDA (MANUAL) ---
            if user_id not in MEMORIA_VINCULACION:
                MEMORIA_VINCULACION[user_id] = []
            MEMORIA_VINCULACION[user_id].append({
                "num_guia": cache.get('num_guia', 'S/D'),
                "fundo": cache.get('fundo', 'S/D'),
                "message_id": cache.get('img_message_id', update.message.message_id),
                "bot_message_id": msg.message_id,
                "enlace_drive": enlace
            })
            if len(MEMORIA_VINCULACION[user_id]) > 5:
                MEMORIA_VINCULACION[user_id].pop(0)
            save_memoria_vinculacion(MEMORIA_VINCULACION)
            # ----------------------------------------
            
            # --- PROGRAMACIÓN DE RECORDATORIOS PARA MANUAL REGULAR ---
            empresa_upper = str(cache.get('empresa', '')).upper()
            if "PROSEMBRA" in empresa_upper:
                context.job_queue.run_once(prosembra_notification_job, 30 * 60, chat_id=user_id, data={"guia": num_guia_saved})
            elif "LOS OLIVOS" in empresa_upper:
                context.job_queue.run_once(olivos_notification_job, 30 * 60, chat_id=user_id, data={"guia": num_guia_saved})
            
            user_states[user_id] = None
            user_data_cache[user_id] = {}
        except Exception as e:
            logger.error(f"Error en guardado manual: {e}")
            await msg.edit_text(f"❌ Error al guardar el registro manual: {e}")

    elif modo == MODO_BITACORA_EDIT_TEXT:
        cache = user_data_cache.get(user_id, {})
        draft = cache.get("bitacora_draft")
        if not draft:
            user_states[user_id] = None
            await update.message.reply_text("⚠️ No se encontró un borrador activo. Vuelve a iniciar desde el menú de Bitácora.")
            return
        draft["texto"] = text.strip()
        user_states[user_id] = MODO_BITACORA_ADD
        card_text, kb_card = build_bitacora_draft_card(draft)
        await update.message.reply_text(card_text, reply_markup=kb_card, parse_mode='Markdown')
        return

    elif modo == MODO_BITACORA_SEARCH:
        pregunta = text.strip()
        msg = await update.message.reply_text(f"⏳ Consultando la Bitácora con Lía...")
        try:
            respuesta = await buscar_conversacional_bitacora(pregunta, msg_status=msg)
            kb = [
                [InlineKeyboardButton("✍️ Nueva Anotación", callback_data='bitacora_add'),
                 InlineKeyboardButton("📋 Ver Últimas 5", callback_data='bita_ultimas_5')],
                [InlineKeyboardButton("🔙 Salir de Búsqueda", callback_data='modo_bitacora')]
            ]
            await msg.edit_text(respuesta, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error en búsqueda conversacional de bitácora: {e}")
            await msg.edit_text(f"❌ Error al consultar la Bitácora: {e}")
        return

    elif modo == MODO_BITACORA_ADD:
        texto_input = text.strip()
        msg = await update.message.reply_text("🧠 Analizando y clasificando tu anotación con IA...")
        try:
            info = await procesar_entrada_ia(texto=texto_input, msg_status=msg)
            draft = {
                "texto": info["texto"],
                "categoria": info["categoria"],
                "tags": info["tags"],
                "formato": "Texto/Anotación",
                "resumen": info.get("resumen", ""),
                "file_path": None,
                "mime_type": None
            }
            user_data_cache[user_id] = {"bitacora_draft": draft}
            card_text, kb_card = build_bitacora_draft_card(draft)
            await msg.edit_text(card_text, reply_markup=kb_card, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error procesando texto para bitacora: {e}")
            await msg.edit_text(f"❌ Error al procesar anotación: {e}")
        return

    elif modo == MODO_OBS_ESCRIBIR:
        cache = user_data_cache.get(user_id, {})
        num_guia = cache.get("obs_guia")
        pet_flag = cache.get("pet_flag", "N")
        if num_guia:
            try:
                obs_final = "Guía hecha" if texto.strip().lower() in ["guia hecha", "guía hecha"] else texto
                await asyncio.to_thread(update_observacion_sheet, num_guia, obs_final, pet_flag)
                if obs_final == "Guía hecha" and pet_flag == "MANUAL_WAIT":
                    kb_dest = [
                        [
                            InlineKeyboardButton("📦 Comercialización", callback_data=f"dest_man|COMERCIALIZACIÓN|{num_guia}"),
                            InlineKeyboardButton("♻️ Disposición Final", callback_data=f"dest_man|DISPOSICIÓN FINAL|{num_guia}")
                        ]
                    ]
                    await update.message.reply_text(
                        f"✅ Observación '{texto}' guardada para la guía `{num_guia}`.\n\n❓ **¿Esta guía corresponde a Comercialización o Disposición Final?**",
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup(kb_dest)
                    )
                else:
                    kb_preg_reg = [
                        [
                            InlineKeyboardButton("✅ Sí, Registrar Guía", callback_data=f"preg_reg|si|{num_guia}"),
                            InlineKeyboardButton("❌ No, terminar", callback_data=f"preg_reg|no|{num_guia}")
                        ]
                    ]
                    await update.message.reply_text(
                        f"✅ Observación '{texto}' guardada para la guía `{num_guia}`.\n\n❓ **¿Deseas registrar esta guía ahora?**",
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup(kb_preg_reg)
                    )
            except Exception as e:
                await update.message.reply_text(f"❌ Error al guardar observación: {e}")
        user_states.pop(user_id, None)
        user_data_cache.pop(user_id, None)
        return

    elif modo == MODO_LIGAR_ESCRIBIR:
        cache = user_data_cache.get(user_id, {})
        num_hecha = cache.get("ligar_hecha")
        num_recibida = texto.strip().upper()
        if not num_hecha:
            await update.message.reply_text("❌ Error de sesión al vincular.")
            user_states.pop(user_id, None)
            return

        msg = await update.message.reply_text(f"⏳ Vinculando guía `{num_hecha}` con `{num_recibida}`...")
        try:
            if not rc.sheet_control: await asyncio.to_thread(conectar_servicios)
            def do_link():
                col_values = rc.sheet_control.col_values(2)
                norm_hecha = normalize_guide_number(num_hecha)
                row_idx = -1
                for idx, val in enumerate(col_values):
                    if normalize_guide_number(val) == norm_hecha:
                        row_idx = idx + 1
                        break
                if row_idx != -1:
                    if "-" in num_recibida:
                        p = num_recibida.split("-")
                        num_recibida_l = f"{p[0]}-{p[1].lstrip('0')}"
                    else:
                        num_recibida_l = num_recibida.lstrip('0')

                    # Buscar si existe enlace Drive de la guía recibida en memoria
                    enlace_recibida = ""
                    norm_recibida = normalize_guide_number(num_recibida)
                    fundo_recibido = ""
                    if user_id in MEMORIA_VINCULACION:
                        for reg in MEMORIA_VINCULACION[user_id]:
                            if normalize_guide_number(reg.get("num_guia", "")) == norm_recibida:
                                enlace_recibida = reg.get("enlace_drive", "")
                                fundo_recibido = reg.get("fundo", "")
                                break

                    rc.sheet_control.update_cell(row_idx, 3, normalizar_valor_upper(num_recibida_l))
                    if enlace_recibida:
                        rc.sheet_control.update_cell(row_idx, 10, enlace_recibida)
                    if fundo_recibido and fundo_recibido != "S/D":
                        rc.sheet_control.update_cell(row_idx, 13, normalizar_valor_upper(fundo_recibido))
                    return True
                return False

            ok = await asyncio.to_thread(do_link)
            if ok:
                await msg.edit_text(f"✅ Guía `{num_hecha}` vinculada exitosamente con la guía origen `{num_recibida}`.", parse_mode='Markdown')
            else:
                await msg.edit_text(f"⚠️ No se encontró la fila de la guía `{num_hecha}` en el Excel para vincular.")
        except Exception as e:
            await msg.edit_text(f"❌ Error al vincular guía: {e}")
        user_states.pop(user_id, None)
        user_data_cache.pop(user_id, None)
        return

# ====================================================================
# --- PROMPT AUDITOR SUNAT Y FUNCIONES MODULARES DE PROCESAMIENTO ---
# ====================================================================
PROMPT_ANALISIS_GUIA = """
Eres un auditor de SUNAT evaluando una Guía de Remisión (GRE) en Perú. Tienes PROHIBIDO alucinar datos.

[[RULES]]
1. TIPO DE GUÍA (CRÍTICO): Lee el título central del documento. Responde "REMITENTE" o "TRANSPORTISTA".
2. EMPRESA PRINCIPAL: Es la empresa dueña de la guía. Extrae su RUC también.
3. LÓGICA DINÁMICA DE ENTIDADES (NOMBRES REALES):
   - Si es TRANSPORTISTA: En 'entidad_1' extrae el NOMBRE REAL de la empresa Remitente. En 'entidad_2' extrae el NOMBRE REAL de la empresa Destinatario.
   - Si es REMITENTE: En 'entidad_1' extrae el NOMBRE REAL de la empresa Destinatario. En 'entidad_2' extrae el NOMBRE REAL del Proveedor (O pon "S/D" si no hay).
4. NÚMERO DE GUÍA: Divídelo estrictamente en "serie" y "correlativo".
5. MOTIVO: Si es "TRANSPORTISTA", el motivo es OBLIGATORIAMENTE "Servicio de Transporte". Si es "REMITENTE", extrae el motivo real (Venta, Traslado, etc.).
6. PRODUCTOS (SOLO DESCRIPCIÓN - PROHIBIDO CÓDIGOS): DEBES extraer ABSOLUTAMENTE TODOS los productos listados. TÚ TRABAJO CRÍTICO ES ELIMINAR CUALQUIER CÓDIGO NUMÉRICO, SKU O REFERENCIA (ej: '0001 Fertilizante', borra '0001'). Quédate ÚNICAMENTE con la descripción real del producto. Usa backticks (`) alrededor del nombre y peso para facilitar la copia:
   `[PRODUCTO LIMPIO 1]`
   `[PESO NUMERICO 1] [UNIDAD 1]`

   `[PRODUCTO LIMPIO 2]`
   `[PESO NUMERICO 2] [UNIDAD 2]`
7. PESO TOTAL: Busca en el documento el "Peso Bruto Total de la carga" y extráelo.
8. FUNDO O PLANTA: Busca atentamente en el documento (observaciones o punto de partida/llegada) si menciona algún "Fundo", "Planta" o nombre de local específico. Si no menciona ninguno, pon "S/D".
9. TRANSPORTE (PLACA): Busca la información del vehículo (Placa). A menudo viene acompañada de la marca y modelo (ej: 'VOLVO FMX PLACA XYZ-123'). TU TRABAJO FIRME ES AISLAR Y EXTRAER ÚNICAMENTE LA PLACA (ej: 'XYZ-123'). No incluyas marcas, modelos, colores o el texto 'Placa:'.
10. UBIGEO ANTI-ALUCINACIONES: 
   - Si dice "PISCO", "MINSUR" o "PARACAS" -> Dpto: ICA | Prov: PISCO | Dist: PARACAS.
   - Si dice "CHOSICA" -> Dpto: LIMA | Prov: LIMA | Dist: LURIGANCHO.

[[OUTPUT_STRUCTURE]]
Responde ÚNICAMENTE con este JSON válido. No uses backticks en las llaves JSON, solo en el texto interno del full_text:
{
  "datos_sheet": {
    "fecha": "[FECHA]", "serie": "[SERIE]", "correlativo": "[CORRELATIVO]", "tipo": "[REMITENTE/TRANSPORTISTA]", 
    "empresa": "[EMPRESA_PRINCIPAL]", "ruc_emisor": "[RUC_EMISOR]", "motivo": "[MOTIVO]",
    "entidad_1": "[NOMBRE ENTIDAD 1]", "entidad_2": "[NOMBRE ENTIDAD 2]",
    "peso_total": "[PESO TOTAL BRUTO]", "fundo_planta": "[FUNDO O PLANTA]",
    "dpto_partida": "[DPTO_P]", "prov_partida": "[PROV_P]", "dist_partida": "[DIST_P]",
    "dpto_llegada": "[DPTO_LL]", "prov_llegada": "[PROV_LL]", "dist_llegada": "[DIST_LL]"
  },
  "full_text": "📅 **Datos Principales**\\nFecha: `[FECHA]`\\nTipo: `[TIPO]`\\nSerie: `[SERIE]`\\nNúmero: `[CORRELATIVO]`\\n🔄 Motivo: `[MOTIVO]`\\n\\n🏢 **Empresa Emisora**: `[EMPRESA_PRINCIPAL]`\\n🆔 **RUC Emisor**: `[RUC_EMISOR]`\\n👤 **Entidad 1 (Rem/Dest)**: `[ENTIDAD_1]`\\n👤 **Entidad 2 (Dest/Prov)**: `[ENTIDAD_2]`\\n\\n📍 **Partida**: `[DIR_PARTIDA]`\\n🗺️ Dpto: `[DPTO_P]` | Prov: `[PROV_P]` | Dist: `[DIST_P]`\\n\\n🏁 **Llegada**: `[DIR_LLEGADA]`\\n🗺️ Dpto: `[DPTO_LL]` | Prov: `[PROV_LL]` | Dist: `[DIST_LL]`\\n\\n🚚 **Transporte**\\nPlaca: `[PLACA]`\\nChofer: `[CHOFER]`\\nLicencia: `[LICENCIA]`\\n\\n📦 **Productos Detallados**\\n[LISTA_DE_TODOS_LOS_PRODUCTOS]\\n\\n⚖️ **Peso Bruto Total:** `[PESO TOTAL BRUTO]`"
}
"""

async def ejecutar_lectura_ocr(user_id, file_path, mime_type, context, msg_status, original_msg_id=None):
    try:
        with open(file_path, "rb") as bf: content = bf.read()
        part = types.Part.from_bytes(data=content, mime_type=mime_type)

        response = await generar_con_reintento([part], PROMPT_ANALISIS_GUIA, msg_status, is_json=True)
        data = json.loads(clean_json_response(response.text))
        datos_sheet = data.get("datos_sheet", {})
        
        if datos_sheet.get("tipo", "").upper() == "TRANSPORTISTA":
            datos_sheet["motivo"] = "Servicio de Transporte"
            
        numero_completo = f"{datos_sheet.get('serie', '')}-{datos_sheet.get('correlativo', '')}"
        
        full_report = data.get("full_text", "Error formateando el reporte")
        full_report = re.sub(r'👤 \*\*Entidad 2 \(Dest/Prov\)\*\*: `S/D`\n?', '', full_report)
        full_report = full_report.replace("Motivo: `None`", "Motivo: `Servicio de Transporte`")

        # --- BÚSQUEDA DE RUC EN CATÁLOGO SI NO SE LEYÓ CON OCR (PDF) ---
        ruc_actual = str(datos_sheet.get("ruc_emisor", "")).strip()
        ruc_digits = re.sub(r'\D', '', ruc_actual)
        es_ruc_valido = len(ruc_digits) == 11 and ruc_digits.startswith(('10', '15', '17', '20'))

        if not es_ruc_valido:
            empresa_nombre = datos_sheet.get("empresa", "")
            ruc_cat, empresa_cat = await async_buscar_ruc_por_empresa(empresa_nombre)

            # Si no encontró por empresa emisora y es transportista, intentar con entidad_1
            if not ruc_cat and datos_sheet.get("tipo", "").upper() == "TRANSPORTISTA":
                entidad_1 = datos_sheet.get("entidad_1", "")
                if entidad_1 and entidad_1.upper() != "S/D":
                    ruc_cat, empresa_cat = await async_buscar_ruc_por_empresa(entidad_1)

            if ruc_cat:
                datos_sheet["ruc_emisor"] = ruc_cat
                logger.info(f"✅ RUC recuperado de catálogo para '{empresa_nombre}': {ruc_cat} ({empresa_cat})")
                patron_ruc = r'🆔 \*\*RUC Emisor\*\*:\s*(`[^`\n]*`|[^\n]+)'
                nuevo_ruc_line = f"🆔 **RUC Emisor**: `{ruc_cat}` *(Catálogo)*"
                if re.search(patron_ruc, full_report):
                    full_report = re.sub(patron_ruc, nuevo_ruc_line, full_report)
                elif "🏢 **Empresa Emisora**:" in full_report:
                    full_report = re.sub(
                        r'(🏢 \*\*Empresa Emisora\*\*:[^\n]+)',
                        rf'\1\n{nuevo_ruc_line}',
                        full_report
                    )

        folder_solo_leer = DRIVE_FOLDER_LEER
        enlace_drive = await async_subir_a_drive(file_path, mime_type, folder_id=folder_solo_leer)
        
        try:
            second_sheet_id = SHEET_ID
            def register_audit_sheet():
                creds = obtener_credenciales()
                client = gspread.authorize(creds)
                book2 = client.open_by_key(second_sheet_id)
                sheet_recibidas = book2.worksheet("Guias_recibidas")
                row_data = [
                    datos_sheet.get("fecha", ""), 
                    numero_completo, 
                    datos_sheet.get("tipo", ""), 
                    datos_sheet.get("empresa", ""), 
                    datos_sheet.get("fundo_planta", "S/D"), 
                    enlace_drive
                ]
                return sync_upsert_row(sheet_recibidas, numero_completo, row_data, col_guia_index=2, col_comentario_index=7)
            
            resultado_upsert = await asyncio.to_thread(register_audit_sheet)
            audit_status = "🔄 *Auditoría: Guía Actualizada*" if resultado_upsert == "updated" else "📌 *Auditoría: Nueva Guía Registrada*"
            await async_log_action(user_id, numero_completo, f"LEER_AUDIT_{resultado_upsert.upper()}")
        except Exception as e:
            audit_status = f"⚠️ Error registro Audit: {str(e)[:20]}"

        footer = f"\n\n{audit_status}\n📁 [Drive]({enlace_drive})\n📊 [Excel]({SHEET_URL_DIRECT})"
        
        texto_para_petramas = f"{full_report} {json.dumps(datos_sheet)}"
        is_petramas = check_is_petramas(texto_para_petramas)
        pet_flag = "P" if is_petramas else "N"
        kb_obs = [
            [InlineKeyboardButton("Guía hecha", callback_data=f"obs|hecha|{numero_completo}|{pet_flag}")],
            [InlineKeyboardButton("Solo certificado", callback_data=f"obs|solocert|{numero_completo}|{pet_flag}")],
            [InlineKeyboardButton("Escribir manualmente", callback_data=f"obs|escribir|{numero_completo}|{pet_flag}")],
            [InlineKeyboardButton("❌ Sin Observación", callback_data=f"obs|cancelar|{numero_completo}")]
        ]
        reply_markup = InlineKeyboardMarkup(kb_obs)
        
        bot_reply = await msg_status.edit_text(full_report + footer, parse_mode='Markdown', reply_markup=reply_markup, disable_web_page_preview=True)

        # --- MEMORIA DE VINCULACIÓN HÍBRIDA ---
        if user_id not in MEMORIA_VINCULACION:
            MEMORIA_VINCULACION[user_id] = []
        MEMORIA_VINCULACION[user_id].append({
            "num_guia": numero_completo,
            "fundo": datos_sheet.get("fundo_planta", "S/D"),
            "message_id": original_msg_id,
            "bot_message_id": bot_reply.message_id,
            "enlace_drive": enlace_drive
        })
        if len(MEMORIA_VINCULACION[user_id]) > 5:
            MEMORIA_VINCULACION[user_id].pop(0)
        save_memoria_vinculacion(MEMORIA_VINCULACION)
        
        texto_analisis = f"{datos_sheet.get('empresa', '')} {datos_sheet.get('entidad_1', '')}".upper()
        if "PROSEMBRA" in texto_analisis:
            context.job_queue.run_once(prosembra_notification_job, 30 * 60, chat_id=user_id, data={"guia": numero_completo})
        elif "LOS OLIVOS" in texto_analisis:
            context.job_queue.run_once(olivos_notification_job, 30 * 60, chat_id=user_id, data={"guia": numero_completo})

    except Exception as e:
        logger.error(f"Error en ejecutar_lectura_ocr: {e}")
        await msg_status.edit_text(f"❌ Error durante la lectura de la guía: {e}")
    finally:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception: pass
        user_states[user_id] = None

async def ejecutar_lectura_manual(user_id, file_path, mime_type, context, msg_status, original_msg_id=None):
    try:
        folder_solo_leer = DRIVE_FOLDER_LEER
        enlace_drive = await async_subir_a_drive(file_path, mime_type, folder_id=folder_solo_leer)
        if user_id not in user_data_cache:
            user_data_cache[user_id] = {}
        user_data_cache[user_id]['enlace_drive'] = enlace_drive
        user_data_cache[user_id]['img_message_id'] = original_msg_id
        user_states[user_id] = MODO_GUIAS_MANUAL_FECHA
        kb = [[InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]]
        await msg_status.edit_text("✅ Imagen subida a Drive.\n\n📅 Paso 1/5 — Escribe la Fecha de la guía (Ej: 04/04/2026):", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        logger.error(f"Error en ejecutar_lectura_manual: {e}")
        await msg_status.edit_text(f"❌ Error al subir imagen a Drive: {e}")
    finally:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception: pass

async def ejecutar_registro_guia(user_id, file_path, mime_type, context, msg_status, guia_origen_auto=None, original_msg_id=None, reply_to_message=None):
    try:
        with open(file_path, "rb") as bf: content = bf.read()
        part = types.Part.from_bytes(data=content, mime_type=mime_type)

        response = await generar_con_reintento([part], PROMPT_ANALISIS_GUIA, msg_status, is_json=True)
        data = json.loads(clean_json_response(response.text))
        datos_sheet = data.get("datos_sheet", {})
        
        if datos_sheet.get("tipo", "").upper() == "TRANSPORTISTA":
            datos_sheet["motivo"] = "Servicio de Transporte"
            
        numero_completo = f"{datos_sheet.get('serie', '')}-{datos_sheet.get('correlativo', '')}"

        # --- BÚSQUEDA DE RUC EN CATÁLOGO SI NO SE LEYÓ CON OCR (PDF) ---
        ruc_actual = str(datos_sheet.get("ruc_emisor", "")).strip()
        ruc_digits = re.sub(r'\D', '', ruc_actual)
        if not (len(ruc_digits) == 11 and ruc_digits.startswith(('10', '15', '17', '20'))):
            empresa_nombre = datos_sheet.get("empresa", "")
            ruc_cat, _ = await async_buscar_ruc_por_empresa(empresa_nombre)
            if not ruc_cat and datos_sheet.get("tipo", "").upper() == "TRANSPORTISTA":
                entidad_1 = datos_sheet.get("entidad_1", "")
                if entidad_1 and entidad_1.upper() != "S/D":
                    ruc_cat, _ = await async_buscar_ruc_por_empresa(entidad_1)
            if ruc_cat:
                datos_sheet["ruc_emisor"] = ruc_cat
        
        enlace_drive = await async_subir_a_drive(file_path, mime_type)
        if not rc.sheet_control: await asyncio.to_thread(conectar_servicios)
        
        guia_ligada_limpia = ""
        fundo_vinculado = ""
        enlace_guia_recibida = ""
        
        # Caso 1: Se pasó guia_origen_auto
        if guia_origen_auto:
            norm_origen = normalize_guide_number(guia_origen_auto)
            if "-" in guia_origen_auto:
                partes_g = guia_origen_auto.split("-")
                guia_ligada_limpia = f"{partes_g[0]}-{partes_g[1].lstrip('0')}"
            else:
                guia_ligada_limpia = guia_origen_auto.lstrip('0')
                
            if user_id in MEMORIA_VINCULACION:
                for reg in MEMORIA_VINCULACION[user_id]:
                    if normalize_guide_number(reg.get("num_guia", "")) == norm_origen:
                        fundo_vinculado = reg.get("fundo", "")
                        enlace_guia_recibida = reg.get("enlace_drive", "")
                        break
        # Caso 2: El usuario usó reply_to_message
        elif reply_to_message:
            reply_id = reply_to_message.message_id
            if user_id in MEMORIA_VINCULACION:
                for reg in MEMORIA_VINCULACION[user_id]:
                    if reg["message_id"] == reply_id or reg.get("bot_message_id") == reply_id:
                        n_guia = reg["num_guia"]
                        if "-" in n_guia:
                            partes_g = n_guia.split("-")
                            guia_ligada_limpia = f"{partes_g[0]}-{partes_g[1].lstrip('0')}"
                        else:
                            guia_ligada_limpia = n_guia.lstrip('0')
                        fundo_vinculado = reg["fundo"]
                        enlace_guia_recibida = reg.get("enlace_drive", "")
                        break

        fundo_final = fundo_vinculado if fundo_vinculado and fundo_vinculado != "S/D" else datos_sheet.get("fundo_planta", "S/D")

        row_data = [
            datos_sheet.get("fecha", ""),                # A: Fecha
            numero_completo,                             # B: N° Guía
            guia_ligada_limpia,                          # C: Guía ligada
            datos_sheet.get("tipo", ""),                 # D: Tipo Guía
            datos_sheet.get("motivo", ""),               # E: Motivo
            datos_sheet.get("empresa", ""),              # F: Empresa Principal
            datos_sheet.get("entidad_1", ""),            # G: Destinatario/Remitente
            datos_sheet.get("entidad_2", ""),            # H: Destinario/Proveedor
            enlace_drive,                                # I: Guia hecha
            enlace_guia_recibida,                        # J: Guia recibida
            "",                                          # K: Sistema/IA
            datos_sheet.get("observacion", ""),          # L: Observacion Manual
            fundo_final,                                 # M: Fundo/Planta
            "",                                          # N: Certificados
            "",                                          # O: Mes
            ""                                           # P: Sigersol
        ]
        
        resultado_upsert = await async_upsert_row(rc.sheet_control, numero_completo, row_data, col_guia_index=2, col_comentario_index=11)
        await async_log_action(user_id, numero_completo, f"REGISTRAR_{resultado_upsert.upper()}")
        
        estado_registro = "🔄 *Guía Actualizada (Sobrescrita)*" if resultado_upsert == "updated" else "✅ *Nueva Guía Registrada*"
        motivo_visual = datos_sheet.get('motivo', 'S/D')
        if not motivo_visual or str(motivo_visual).strip().lower() == 'none':
            motivo_visual = 'S/D'

        resumen_registro = (
            f"{estado_registro}\n\n"
            f"📄 **Guía:** `{numero_completo}`\n"
            f"🏢 **Empresa:** `{datos_sheet.get('empresa', 'S/D')}`\n"
            f"🔄 **Motivo:** `{motivo_visual}`\n"
        )
        
        if guia_ligada_limpia:
            resumen_registro += f"🔗 **Guía Origen Ligada:** `{guia_ligada_limpia}`\n"
        if fundo_final and fundo_final != "S/D":
            resumen_registro += f"🏡 **Fundo/Planta:** `{fundo_final}`\n"
            
        resumen_registro += (
            f"\n📁 [Ver PDF en Drive]({enlace_drive})\n"
            f"📊 [Abrir Excel]({SHEET_URL_DIRECT})"
        )
        
        user_states[user_id] = None
        user_data_cache[user_id] = {}

        if guia_ligada_limpia:
            await msg_status.edit_text(resumen_registro, parse_mode='Markdown', disable_web_page_preview=True)
            return

        kb_preg_ligar = [
            [
                InlineKeyboardButton("🔗 Sí, Ligar", callback_data=f"preg_ligar|si|{numero_completo}"),
                InlineKeyboardButton("❌ No Ligar", callback_data=f"preg_ligar|no|{numero_completo}")
            ]
        ]
        resumen_registro += "\n\n❓ **¿Deseas ligar esta guía a una guía recibida?**"
        await msg_status.edit_text(resumen_registro, parse_mode='Markdown', disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(kb_preg_ligar))

    except Exception as e:
        logger.error(f"Error en ejecutar_registro_guia: {e}")
        await msg_status.edit_text(f"❌ Error durante el procesamiento: {e}")
    finally:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception: pass

# ====================================================================
# --- EJECUTOR DE REGISTRO DE FACTURAS (PDF / XML) ---
# ====================================================================
async def ejecutar_registro_factura(user_id, file_path, mime_type, context, msg_status, is_xml=False, original_msg_id=None):
    try:
        if is_xml:
            if msg_status:
                try: await msg_status.edit_text("⏳ Procesando Factura XML con motor nativo UBL 2.1 SUNAT...")
                except: pass
            datos = await async_parse_factura_xml(file_path)
        else:
            if msg_status:
                try: await msg_status.edit_text("⏳ Analizando Factura (PDF) con IA (Gemini)...")
                except: pass
            datos = await parse_factura_pdf(file_path, msg_status)

        numero_factura = datos.get("numero_factura", "S/N")
        emisor_nombre = datos.get("emisor_nombre", "EMISOR DESCONOCIDO")
        emisor_ruc = datos.get("emisor_ruc", "")
        fecha_emision = datos.get("fecha_emision", "")
        moneda = datos.get("moneda", "PEN")
        cantidad = datos.get("cantidad", 1)
        valor_unitario = datos.get("valor_unitario", 0.0)
        precio_unitario = datos.get("precio_unitario", 0.0)
        valor_total = datos.get("valor_total", 0.0)
        igv = datos.get("igv", 0.0)
        importe_total = datos.get("importe_total", 0.0)

        if msg_status:
            try: await msg_status.edit_text(f"⏳ Verificando carpeta en Drive para '{emisor_nombre}'...")
            except: pass
        folder_empresa_id, folder_empresa_nombre = await async_obtener_o_crear_carpeta_empresa(emisor_nombre, emisor_ruc)

        if msg_status:
            try: await msg_status.edit_text(f"⏳ Subiendo comprobante a Drive (Carpeta '{folder_empresa_nombre}')...")
            except: pass
        enlace_drive = await async_subir_a_drive(file_path, mime_type, folder_id=folder_empresa_id)
        datos["enlace_drive"] = enlace_drive

        if msg_status:
            try: await msg_status.edit_text("⏳ Guardando datos en Google Sheets (Registro_Facturas)...")
            except: pass
        accion, _ = await async_guardar_factura_en_sheet(datos)
        await async_log_action(user_id, numero_factura, f"FACTURA_{accion.upper()}")

        estado_registro = "🔄 *Factura Actualizada (Sobrescrita)*" if accion == "updated" else "✅ *Nueva Factura Registrada*"
        simb = "S/." if moneda == "PEN" else "$"

        resumen = (
            f"{estado_registro}\n\n"
            f"📄 *N° Factura:* `{numero_factura}`\n"
            f"🏢 *Emisor:* `{emisor_nombre}`\n"
        )
        if emisor_ruc:
            resumen += f"🆔 *RUC:* `{emisor_ruc}`\n"
        resumen += (
            f"📅 *Fecha Emisión:* `{fecha_emision}`\n"
            f"💰 *Moneda:* `{moneda}`\n"
            f"📦 *Cantidad:* `{cantidad}`\n"
            f"🏷 *Valor Unitario:* `{simb} {valor_unitario:,.4f}`\n"
            f"🏷 *Precio Unitario:* `{simb} {precio_unitario:,.4f}`\n"
            f"📊 *Valor Total:* `{simb} {valor_total:,.2f}`\n"
            f"🧾 *IGV:* `{simb} {igv:,.2f}`\n"
            f"💵 *Importe Total:* `{simb} {importe_total:,.2f}`\n"
            f"📂 *Carpeta Drive:* `{folder_empresa_nombre}`\n\n"
            f"📁 [Ver Archivo en Drive]({enlace_drive})\n"
            f"📊 [Abrir Google Sheet]({SHEET_URL_DIRECT})"
        )

        kb = [
            [InlineKeyboardButton("➕ Registrar Otra Factura", callback_data="modo_facturas_registrar")],
            [InlineKeyboardButton("🔙 Menú Principal", callback_data="volver_inicio")]
        ]

        if msg_status:
            try:
                await msg_status.edit_text(resumen, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb), disable_web_page_preview=True)
            except Exception:
                await context.bot.send_message(chat_id=user_id, text=resumen, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb), disable_web_page_preview=True)
        else:
            await context.bot.send_message(chat_id=user_id, text=resumen, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb), disable_web_page_preview=True)

        user_states[user_id] = None
        user_data_cache.pop(user_id, None)

    except Exception as e:
        logger.error(f"❌ Error durante el registro de factura: {e}")
        if msg_status:
            try: await msg_status.edit_text(f"❌ Error durante el registro de la factura:\n`{e}`", parse_mode='Markdown')
            except: pass
    finally:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception: pass

# ====================================================================
# --- HANDLER DE ARCHIVOS Y MULTIMEDIA ---
# ====================================================================
async def handle_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    user_id = update.effective_user.id
    modo = user_states.get(user_id)

    # Modo Pendientes (Audio, Nota de voz o Imagen)
    if modo == MODO_PENDIENTE_ADD:
        msg = await update.message.reply_text("⏳ Descargando audio/archivo para el pendiente...")
        file_path = f"pnd_{user_id}_{update.message.id}.ogg"
        try:
            mime_type = "audio/ogg"
            if update.message.voice:
                f = await update.message.voice.get_file()
                mime_type = "audio/ogg"
            elif update.message.audio:
                f = await update.message.audio.get_file()
                mime_type = update.message.audio.mime_type or "audio/mp3"
                file_path = file_path.replace('.ogg', '.mp3')
            elif update.message.photo:
                f = await update.message.photo[-1].get_file()
                mime_type = "image/jpeg"
                file_path = file_path.replace('.ogg', '.jpg')
            elif update.message.document:
                f = await update.message.document.get_file()
                mime_type = update.message.document.mime_type or "application/pdf"
                file_path = file_path.replace('.ogg', '.pdf')
            else:
                await msg.edit_text("⚠️ Tipo de archivo no compatible para registrar pendiente.")
                return

            await f.download_to_drive(file_path)
            await msg.edit_text("🎙️🧠 Escuchando audio y extrayendo tarea con IA...")
            caption = update.message.caption or ""
            info = await procesar_pendiente_ia(texto=caption, file_path=file_path, mime_type=mime_type)

            try:
                if os.path.exists(file_path): os.remove(file_path)
            except: pass

            draft = {
                "titulo": info.get("titulo", "Nuevo pendiente"),
                "detalle": info.get("detalle", ""),
                "cliente_ref": info.get("cliente_ref", ""),
                "fecha_alerta": info.get("fecha_alerta", ""),
                "prioridad": info.get("prioridad", "MEDIA")
            }
            user_data_cache[user_id] = {"pendiente_draft": draft}
            user_states[user_id] = None
            card_text, kb_card = build_pendiente_draft_card(draft)
            await msg.edit_text(card_text, reply_markup=kb_card, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error procesando audio pendiente: {e}")
            await msg.edit_text(f"❌ Error al procesar audio: {html.escape(str(e))}")
        return

    # Modo Cotización por Voz / Audio
    if modo == MODO_COTIZACION_IA:
        msg = await update.message.reply_text("⏳ Descargando audio para la cotización...")
        file_path = f"coti_{user_id}_{update.message.id}.ogg"
        try:
            mime_type = "audio/ogg"
            if update.message.voice:
                f = await update.message.voice.get_file()
                mime_type = "audio/ogg"
            elif update.message.audio:
                f = await update.message.audio.get_file()
                mime_type = update.message.audio.mime_type or "audio/mp3"
                file_path = file_path.replace('.ogg', '.mp3')
            else:
                await msg.edit_text("⚠️ Envía una nota de voz o audio dictando la cotización.")
                return

            await f.download_to_drive(file_path)
            await msg.edit_text("🎙️🧠 Escuchando audio y analizando cotización con IA...")
            caption = update.message.caption or ""
            coti_info = await interpretar_cotizacion_ia(texto=caption, file_path=file_path, mime_type=mime_type)

            try:
                if os.path.exists(file_path): os.remove(file_path)
            except: pass

            user_states[user_id] = None
            await _procesar_resultado_coti_ia(update, context, user_id, msg, coti_info)
        except Exception as e:
            logger.error(f"Error procesando audio para cotización: {e}")
            await msg.edit_text(f"❌ Error al procesar audio: {html.escape(str(e))}")
        return

    # 1. Modo Bitácora
    if modo == MODO_BITACORA_ADD:
        msg = await update.message.reply_text("⏳ Descargando archivo...")
        file_path = f"archivo_{user_id}_{update.message.id}.jpg"
        try:
            mime_type = "image/jpeg"
            caption = update.message.caption or ""
            
            if update.message.photo:
                f = await update.message.photo[-1].get_file()
                mime_type = "image/jpeg"
            elif update.message.document:
                f = await update.message.document.get_file()
                mime_type = update.message.document.mime_type or "application/pdf"
                if 'pdf' in mime_type: file_path = file_path.replace('.jpg', '.pdf')
            elif update.message.voice:
                f = await update.message.voice.get_file()
                mime_type = "audio/ogg"
                file_path = file_path.replace('.jpg', '.ogg')
            elif update.message.audio:
                f = await update.message.audio.get_file()
                mime_type = update.message.audio.mime_type or "audio/mp3"
                file_path = file_path.replace('.jpg', '.mp3')
            elif update.message.video:
                f = await update.message.video.get_file()
                mime_type = update.message.video.mime_type or "video/mp4"
                file_path = file_path.replace('.jpg', '.mp4')
            else:
                await msg.edit_text("⚠️ Tipo de archivo no reconocido para la Bitácora.")
                return

            await f.download_to_drive(file_path)
            await msg.edit_text("🎙️🧠 Analizando contenido con IA (transcripción / OCR)...")
            
            info = await procesar_entrada_ia(
                file_path=file_path,
                mime_type=mime_type,
                caption=caption,
                msg_status=msg
            )
            
            draft = {
                "texto": info["texto"],
                "categoria": info["categoria"],
                "tags": info["tags"],
                "formato": info["formato"],
                "resumen": info.get("resumen", ""),
                "file_path": file_path,
                "mime_type": mime_type,
                "caption": caption
            }
            user_data_cache[user_id] = {"bitacora_draft": draft}
            
            card_text, kb_card = build_bitacora_draft_card(draft)
            await msg.edit_text(card_text, reply_markup=kb_card, parse_mode='Markdown')
            return
        except Exception as e:
            logger.error(f"Error procesando archivo en bitácora: {e}")
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except: pass
            await msg.edit_text(f"❌ Error al procesar el archivo en Bitácora: {e}")
            return

    # 2. Detección de Archivo (XML, PDF, Imagen)
    is_xml = False
    is_pdf = False
    is_image = False
    file_obj = None
    mime_type = ""
    tipo_label = "Archivo"
    file_name = ""

    if update.message.photo:
        file_obj = await update.message.photo[-1].get_file()
        mime_type = "image/jpeg"
        is_image = True
        tipo_label = "Foto"
    elif update.message.document:
        file_obj = await update.message.document.get_file()
        mime_type = update.message.document.mime_type or ""
        file_name = update.message.document.file_name or ""
        if 'xml' in mime_type.lower() or file_name.lower().endswith('.xml'):
            is_xml = True
            mime_type = "application/xml"
            tipo_label = "Archivo XML (Factura)"
        elif 'pdf' in mime_type.lower() or file_name.lower().endswith('.pdf'):
            is_pdf = True
            mime_type = "application/pdf"
            tipo_label = "Documento PDF"
        elif mime_type.startswith('image/') or any(file_name.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.webp']):
            is_image = True
            tipo_label = "Imagen"
        else:
            if modo is None:
                return
    else:
        return

    if not file_obj or not (is_xml or is_pdf or is_image):
        return

    ext = "xml" if is_xml else ("pdf" if is_pdf else "jpg")
    file_path = f"archivo_{user_id}_{update.message.id}.{ext}"
    await file_obj.download_to_drive(file_path)

    # REGLA PRIORITARIA: Archivo XML -> Factura Electrónica INMEDIATA
    # (Los archivos XML siempre corresponden a facturas y no requieren confirmación)
    if is_xml:
        msg_status = await update.message.reply_text("⏳ Factura XML detectada. Procesando y registrando en el Sheet...")
        await ejecutar_registro_factura(
            user_id=user_id,
            file_path=file_path,
            mime_type=mime_type,
            context=context,
            msg_status=msg_status,
            is_xml=True,
            original_msg_id=update.message.id
        )
        return

    # CASO: Modo Registro de Facturas activo
    if modo == MODO_FACTURAS_REGISTRAR:
        if is_pdf:
            msg_status = await update.message.reply_text("⏳ Procesando registro de factura (PDF) con IA...")
            await ejecutar_registro_factura(
                user_id=user_id,
                file_path=file_path,
                mime_type=mime_type,
                context=context,
                msg_status=msg_status,
                is_xml=False,
                original_msg_id=update.message.id
            )
        else:
            await update.message.reply_text("⚠️ En el modo Facturas debes enviar un archivo PDF o XML.")
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except: pass
        return

    # CASO A: Subida directa sin comando /start previo (modo is None)
    if modo is None:
        if user_id not in user_data_cache:
            user_data_cache[user_id] = {}
        
        display_name = file_name if file_name else ("documento.pdf" if is_pdf else "imagen.jpg")

        if is_pdf:
            tipo_detectado = await async_detectar_tipo_documento_pdf(file_path)
            user_data_cache[user_id]['direct_file'] = {
                'file_path': file_path,
                'mime_type': mime_type,
                'is_pdf': is_pdf,
                'is_image': is_image,
                'is_xml': False,
                'tipo_label': tipo_label,
                'tipo_detectado': tipo_detectado,
                'message_id': update.message.id
            }

            if tipo_detectado == "FACTURA":
                kb = [
                    [InlineKeyboardButton("🧾 Registrar Factura", callback_data="direct_action|factura_registrar")],
                    [InlineKeyboardButton("📘 Es una Guía", callback_data="direct_action|guia_options")],
                    [InlineKeyboardButton("❌ Cancelar", callback_data="direct_action|cancelar")]
                ]
                await update.message.reply_text(
                    f"🧾 **Factura Electrónica detectada** (`{display_name}`)\n\n"
                    f"He analizado el documento y corresponde a una **Factura**.\n"
                    f"¿Deseas registrarla en el Google Sheet?",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode='Markdown'
                )
                return

            elif tipo_detectado == "GUIA":
                kb = [
                    [
                        InlineKeyboardButton("📁 Registrar Guía", callback_data="direct_action|registrar"),
                        InlineKeyboardButton("📖 Leer Guía", callback_data="direct_action|leer")
                    ],
                    [InlineKeyboardButton("🧾 Es una Factura", callback_data="direct_action|factura_registrar")],
                    [InlineKeyboardButton("❌ Cancelar", callback_data="direct_action|cancelar")]
                ]
                await update.message.reply_text(
                    f"📘 **Guía de Remisión detectada** (`{display_name}`)\n\n"
                    f"¿Deseas **registrar** la guía o **leerla**?",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode='Markdown'
                )
                return

            else:
                # PDF Escaneado o no clasificado directamente con texto
                kb = [
                    [InlineKeyboardButton("🧾 Registrar Factura", callback_data="direct_action|factura_registrar")],
                    [
                        InlineKeyboardButton("📁 Registrar Guía", callback_data="direct_action|registrar"),
                        InlineKeyboardButton("📖 Leer Guía", callback_data="direct_action|leer")
                    ],
                    [InlineKeyboardButton("❌ Cancelar", callback_data="direct_action|cancelar")]
                ]
                await update.message.reply_text(
                    f"📄 **Documento PDF recibido** (`{display_name}`)\n\n"
                    f"¿Qué tipo de documento deseas procesar?",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode='Markdown'
                )
                return

        elif is_image:
            user_data_cache[user_id]['direct_file'] = {
                'file_path': file_path,
                'mime_type': mime_type,
                'is_pdf': is_pdf,
                'is_image': is_image,
                'is_xml': False,
                'tipo_label': tipo_label,
                'message_id': update.message.id
            }
            kb = [
                [
                    InlineKeyboardButton("📖 Leer Guía", callback_data="direct_action|leer"),
                    InlineKeyboardButton("📁 Registrar Guía", callback_data="direct_action|registrar")
                ],
                [
                    InlineKeyboardButton("❌ Cancelar", callback_data="direct_action|cancelar")
                ]
            ]
            await update.message.reply_text(
                f"📸 **Imagen recibida**\n\n¿Deseas **leer** la guía o **registrarla**?",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
            return

    # CASO B: Modo Lectura activo
    if modo == MODO_GUIAS_LEER:
        if is_pdf:
            msg_status = await update.message.reply_text("⏳ Leyendo guía (PDF) con OCR e IA...")
            await ejecutar_lectura_ocr(user_id, file_path, mime_type, context, msg_status, original_msg_id=update.message.id)
        else:
            msg_status = await update.message.reply_text("⏳ Subiendo imagen a Drive para registro manual...")
            await ejecutar_lectura_manual(user_id, file_path, mime_type, context, msg_status, original_msg_id=update.message.id)
        return

    # CASO C: Modo Registro activo
    if modo == MODO_GUIAS_REGISTRAR:
        if is_pdf:
            tipo_detectado = await async_detectar_tipo_documento_pdf(file_path)
            if tipo_detectado == "FACTURA":
                if user_id not in user_data_cache:
                    user_data_cache[user_id] = {}
                display_name = file_name if file_name else "documento.pdf"
                user_data_cache[user_id]['direct_file'] = {
                    'file_path': file_path,
                    'mime_type': mime_type,
                    'is_pdf': is_pdf,
                    'is_image': is_image,
                    'is_xml': False,
                    'tipo_label': tipo_label,
                    'tipo_detectado': tipo_detectado,
                    'message_id': update.message.id
                }
                kb = [
                    [InlineKeyboardButton("🧾 Sí, Registrar como Factura", callback_data="direct_action|factura_registrar")],
                    [InlineKeyboardButton("📁 Forzar como Guía", callback_data="direct_action|registrar")],
                    [InlineKeyboardButton("❌ Cancelar", callback_data="direct_action|cancelar")]
                ]
                await update.message.reply_text(
                    f"⚠️ **Atención:** Estabas en modo Registrar Guía, pero este documento parece ser una **Factura Electrónica** (`{display_name}`).\n\n"
                    f"¿Cómo deseas registrarlo?",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode='Markdown'
                )
                return

        guia_origen = user_data_cache.get(user_id, {}).get('guia_origen_vinculada')
        msg_status = await update.message.reply_text("⏳ Procesando registro de guía con IA...")
        await ejecutar_registro_guia(
            user_id=user_id,
            file_path=file_path,
            mime_type=mime_type,
            context=context,
            msg_status=msg_status,
            guia_origen_auto=guia_origen,
            original_msg_id=update.message.id,
            reply_to_message=update.message.reply_to_message
        )
        return


    # CASO D: Modo Manual activo
    if modo == MODO_GUIAS_MANUAL:
        msg_status = await update.message.reply_text("⏳ Subiendo imagen a Drive para registro manual...")
        await ejecutar_lectura_manual(user_id, file_path, mime_type, context, msg_status, original_msg_id=update.message.id)
        return

# ====================================================================
# --- HANDLER CALLBACK OBSERVACION ---
# ====================================================================
def update_observacion_sheet(num_guia, observacion, pet_flag="N"):
    try:
        creds = obtener_credenciales()
        client = gspread.authorize(creds)
        book2 = client.open_by_key(SHEET_ID)
        sheet_recibidas = book2.worksheet("Guias_recibidas")
        col_values = sheet_recibidas.col_values(2) 
        
        norm_guia = normalize_guide_number(num_guia)
        row_idx = -1
        # Iterar al revés para encontrar el registro más reciente (corrige error con "S/D")
        for idx in range(len(col_values)-1, -1, -1):
            if normalize_guide_number(col_values[idx]) == norm_guia:
                row_idx = idx + 1
                break
                
        if row_idx != -1:
            # Escribir la observación en la columna I (9)
            sheet_recibidas.update_cell(row_idx, 9, normalizar_valor_upper(observacion))
            logger.info(f"✅ Observación {observacion} guardada en Guias_recibidas columna I (9), fila {row_idx}")
            
            # Lógica para columna J (10) según petramas exclusivamente cuando la observación sea "Guía hecha"
            if observacion == "Guía hecha":
                if pet_flag == "MANUAL_WAIT":
                    logger.info(f"⏳ Subida manual: Esperando selección interactiva para Columna J (10)")
                    return
                is_pet = (pet_flag == "P")
                if not is_pet:
                    try:
                        # Doble verificación contra los datos guardados de la fila en Guias_recibidas
                        row_vals = sheet_recibidas.row_values(row_idx)
                        if check_is_petramas(" ".join(str(v) for v in row_vals)):
                            is_pet = True
                    except Exception as e:
                        logger.warning(f"No se pudo verificar fila para petramas: {e}")

                valor_col_j = "DISPOSICIÓN FINAL" if is_pet else "COMERCIALIZACIÓN"
                sheet_recibidas.update_cell(row_idx, 10, valor_col_j)
                logger.info(f"✅ Guía hecha: Columna J (10) guardada como '{valor_col_j}' en Guias_recibidas fila {row_idx}")
                    
    except Exception as e:
        logger.error(f"Error actualizando observación: {e}")

async def handle_callback_observacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query.data.startswith('obs|'): return
    
    await query.answer("Procesando...")
    partes = query.data.split('|')
    accion = partes[1]
    num_guia = partes[2]
    pet_flag = partes[3] if len(partes) > 3 else "N"
    user_id = update.effective_user.id
    
    if accion == "hecha_man":
        await asyncio.to_thread(update_observacion_sheet, num_guia, "Guía hecha", "MANUAL_WAIT")
        nuevo_texto = query.message.text + "\n\n📝 *Observación Guardada:* Guía hecha\n\n❓ **¿Esta guía corresponde a Comercialización o Disposición Final?**"
        kb_dest = [
            [
                InlineKeyboardButton("📦 Comercialización", callback_data=f"dest_man|COMERCIALIZACIÓN|{num_guia}"),
                InlineKeyboardButton("♻️ Disposición Final", callback_data=f"dest_man|DISPOSICIÓN FINAL|{num_guia}")
            ]
        ]
        try:
            await query.edit_message_text(nuevo_texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb_dest))
        except Exception:
            pass
        return

    if accion == "escribir":
        user_states[user_id] = MODO_OBS_ESCRIBIR
        user_data_cache[user_id] = {"obs_guia": num_guia, "msg_id": query.message.message_id, "pet_flag": pet_flag}
        await query.message.reply_text(f"✍️ Escribe la observación para la guía `{num_guia}`:", parse_mode='Markdown')
        return
        
    if accion == "cancelar":
        nuevo_texto = query.message.text + f"\n\n✅ Registro completado sin observaciones adicionales.\n\n❓ **¿Deseas registrar esta guía ahora?**"
        kb_preg_reg = [
            [
                InlineKeyboardButton("✅ Sí, Registrar Guía", callback_data=f"preg_reg|si|{num_guia}"),
                InlineKeyboardButton("❌ No, terminar", callback_data=f"preg_reg|no|{num_guia}")
            ]
        ]
        try:
            await query.edit_message_text(nuevo_texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb_preg_reg))
        except Exception:
            pass
        return
        
    observacion = "Guía hecha" if accion == "hecha" else "Solo certificado"
    
    await asyncio.to_thread(update_observacion_sheet, num_guia, observacion, pet_flag)
    
    nuevo_texto = query.message.text + f"\n\n📝 *Observación Guardada:* {observacion}\n\n❓ **¿Deseas registrar esta guía ahora?**"
    kb_preg_reg = [
        [
            InlineKeyboardButton("✅ Sí, Registrar Guía", callback_data=f"preg_reg|si|{num_guia}"),
            InlineKeyboardButton("❌ No, terminar", callback_data=f"preg_reg|no|{num_guia}")
        ]
    ]
    try:
        await query.edit_message_text(nuevo_texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb_preg_reg))
    except Exception:
        pass

def update_destino_manual_sheet(num_guia, destino):
    try:
        creds = obtener_credenciales()
        client = gspread.authorize(creds)
        book2 = client.open_by_key(SHEET_ID)
        sheet_recibidas = book2.worksheet("Guias_recibidas")
        col_values = sheet_recibidas.col_values(2) 
        
        norm_guia = normalize_guide_number(num_guia)
        row_idx = -1
        for idx in range(len(col_values)-1, -1, -1):
            if normalize_guide_number(col_values[idx]) == norm_guia:
                row_idx = idx + 1
                break
                
        if row_idx != -1:
            sheet_recibidas.update_cell(row_idx, 10, destino)
            logger.info(f"✅ Destino manual '{destino}' guardado en Guias_recibidas columna J (10), fila {row_idx}")
    except Exception as e:
        logger.error(f"Error actualizando destino manual: {e}")

async def handle_callback_destino_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query.data.startswith('dest_man|'): return
    
    await query.answer("Guardando...")
    partes = query.data.split('|')
    destino = partes[1]
    num_guia = partes[2]
    
    await asyncio.to_thread(update_destino_manual_sheet, num_guia, destino)
    
    nuevo_texto = query.message.text + f"\n\n🏷️ *Tipo de Destino:* {destino}\n\n❓ **¿Deseas registrar esta guía ahora?**"
    kb_preg_reg = [
        [
            InlineKeyboardButton("✅ Sí, Registrar Guía", callback_data=f"preg_reg|si|{num_guia}"),
            InlineKeyboardButton("❌ No, terminar", callback_data=f"preg_reg|no|{num_guia}")
        ]
    ]
    try:
        await query.edit_message_text(nuevo_texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb_preg_reg))
    except Exception:
        pass

async def handle_callback_pregunta_registro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query.data.startswith('preg_reg|'): return
    await query.answer()
    
    partes = query.data.split('|')
    accion = partes[1]
    num_guia = partes[2] if len(partes) > 2 else "S/D"
    user_id = update.effective_user.id
    
    if accion == "si":
        user_states[user_id] = MODO_GUIAS_REGISTRAR
        if user_id not in user_data_cache:
            user_data_cache[user_id] = {}
        user_data_cache[user_id]['guia_origen_vinculada'] = num_guia
        
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_operacion')]]
        await context.bot.send_message(
            chat_id=user_id,
            text=f"📁 *Modo Registro Activado*\n\nPor favor, sube el PDF o foto de la guía emitida/hecha que deseas registrar en el sistema.\n_(Se vinculará automáticamente con la guía origen `{num_guia}`)_",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(kb)
        )
    elif accion == "no":
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        try:
            texto_base = query.message.text or ""
            await query.edit_message_text(
                texto_base + "\n\n👍 Proceso completado.",
                parse_mode='Markdown',
                reply_markup=None,
                disable_web_page_preview=True
            )
        except Exception:
            await query.edit_message_reply_markup(reply_markup=None)

async def handle_callback_pregunta_ligar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query.data.startswith('preg_ligar|'): return
    await query.answer()
    
    partes = query.data.split('|')
    accion = partes[1]
    num_hecha = partes[2]
    user_id = update.effective_user.id
    
    if accion == "no":
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        texto_base = query.message.text or ""
        nuevo_texto = texto_base + "\n\n✅ *Guía registrada sin vincular a otra guía.*"
        try:
            await query.edit_message_text(nuevo_texto, parse_mode='Markdown', reply_markup=None, disable_web_page_preview=True)
        except Exception:
            await query.edit_message_reply_markup(reply_markup=None)
            
    elif accion == "si":
        botones = []
        if user_id in MEMORIA_VINCULACION and len(MEMORIA_VINCULACION[user_id]) > 0:
            for reg in reversed(MEMORIA_VINCULACION[user_id]):
                n_rec = reg["num_guia"]
                f_rec = reg["fundo"]
                f_rec_short = f_rec[:10] + "..." if len(f_rec) > 10 else f_rec
                cb_data = f"vinc|{num_hecha}|{n_rec}|{f_rec}"
                if len(cb_data) > 64:
                    cb_data = cb_data[:64]
                botones.append([InlineKeyboardButton(f"🔗 Con {n_rec} ({f_rec_short})", callback_data=cb_data)])
        
        botones.append([InlineKeyboardButton("✍️ Escribir N° de Guía", callback_data=f"preg_ligar|escribir|{num_hecha}")])
        botones.append([InlineKeyboardButton("❌ No Ligar / Cancelar", callback_data=f"preg_ligar|no|{num_hecha}")])
        
        texto_base = query.message.text or ""
        await query.edit_message_text(
            texto_base + "\n\n🔗 **Selecciona la guía recibida con la que deseas vincularla:**",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(botones),
            disable_web_page_preview=True
        )
        
    elif accion == "escribir":
        user_states[user_id] = MODO_LIGAR_ESCRIBIR
        if user_id not in user_data_cache:
            user_data_cache[user_id] = {}
        user_data_cache[user_id]["ligar_hecha"] = num_hecha
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text(
            f"✍️ Escribe el N° de la Guía Recibida (Ej: `EG03-293`) a la que deseas vincular la guía `{num_hecha}`:",
            parse_mode='Markdown'
        )

async def handle_callback_direct_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query.data.startswith('direct_action|'): return
    await query.answer()
    
    user_id = update.effective_user.id
    partes = query.data.split('|')
    accion = partes[1]
    
    direct_file = user_data_cache.get(user_id, {}).get('direct_file')
    if not direct_file:
        await query.edit_message_text("❌ No se encontró el archivo temporal o la sesión expiró. Por favor, vuelve a subir la guía.")
        return
        
    file_path = direct_file.get('file_path', '')
    mime_type = direct_file.get('mime_type', '')
    is_pdf = direct_file.get('is_pdf', False)
    original_msg_id = direct_file.get('message_id')
    
    if accion == "cancelar":
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception: pass
        user_data_cache.pop(user_id, None)
        user_states[user_id] = None
        await query.edit_message_text("❌ Operación cancelada. Archivo descartado.")
        return
        
    elif accion == "registrar":
        msg_status = await query.edit_message_text("⏳ Procesando registro de guía con IA...")
        await ejecutar_registro_guia(
            user_id=user_id,
            file_path=file_path,
            mime_type=mime_type,
            context=context,
            msg_status=msg_status,
            guia_origen_auto=None,
            original_msg_id=original_msg_id
        )
        
    elif accion == "leer":
        if is_pdf:
            msg_status = await query.edit_message_text("⏳ Leyendo guía (PDF) con OCR e IA...")
            await ejecutar_lectura_ocr(
                user_id=user_id,
                file_path=file_path,
                mime_type=mime_type,
                context=context,
                msg_status=msg_status,
                original_msg_id=original_msg_id
            )
        else:
            msg_status = await query.edit_message_text("⏳ Subiendo imagen a Drive para registro manual...")
            await ejecutar_lectura_manual(
                user_id=user_id,
                file_path=file_path,
                mime_type=mime_type,
                context=context,
                msg_status=msg_status,
                original_msg_id=original_msg_id
            )

    elif accion == "factura_registrar":
        msg_status = await query.edit_message_text("⏳ Procesando registro de factura...")
        await ejecutar_registro_factura(
            user_id=user_id,
            file_path=file_path,
            mime_type=mime_type,
            context=context,
            msg_status=msg_status,
            is_xml=direct_file.get('is_xml', False),
            original_msg_id=original_msg_id
        )

    elif accion == "guia_options":
        kb = [
            [
                InlineKeyboardButton("📁 Registrar Guía", callback_data="direct_action|registrar"),
                InlineKeyboardButton("📖 Leer Guía", callback_data="direct_action|leer")
            ],
            [InlineKeyboardButton("❌ Cancelar", callback_data="direct_action|cancelar")]
        ]
        await query.edit_message_text("📘 **Opciones para Guía de Remisión:**\n\n¿Deseas registrarla o leerla?", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

# ====================================================================
# --- HANDLER CALLBACK VINCULACION ---
# ====================================================================
async def handle_callback_vinculacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    if not query.data.startswith('vinc|'):
        return
        
    await query.answer("Procesando vinculación...")
    
    partes = query.data.split("|")
    if len(partes) >= 4:
        num_hecha = partes[1]
        num_recibida = partes[2]
        fundo = partes[3]
        
        try:
            if not rc.sheet_control: 
                await asyncio.to_thread(conectar_servicios)
            def update_origen():
                col_values = rc.sheet_control.col_values(2) 
                norm_hecha = normalize_guide_number(num_hecha)
                row_idx = -1
                for idx, val in enumerate(col_values):
                    if normalize_guide_number(val) == norm_hecha:
                        row_idx = idx + 1
                        break
                        
                if row_idx != -1:
                    if "-" in num_recibida:
                        p = num_recibida.split("-")
                        num_recibida_l = f"{p[0]}-{p[1].lstrip('0')}"
                    else:
                        num_recibida_l = num_recibida.lstrip('0')
                        
                    # Buscar enlace_drive de la guia recibida en MEMORIA_VINCULACION con normalización robusta
                    enlace_recibida = ""
                    norm_recibida = normalize_guide_number(num_recibida)
                    if user_id in MEMORIA_VINCULACION:
                        for reg in MEMORIA_VINCULACION[user_id]:
                            if normalize_guide_number(reg.get("num_guia", "")) == norm_recibida:
                                enlace_recibida = reg.get("enlace_drive", "")
                                break

                    rc.sheet_control.update_cell(row_idx, 3, normalizar_valor_upper(num_recibida_l)) 
                    rc.sheet_control.update_cell(row_idx, 10, enlace_recibida)  # J: Guia recibida (URL)
                    rc.sheet_control.update_cell(row_idx, 13, normalizar_valor_upper(fundo))            # M: Fundo/Planta
            await asyncio.to_thread(update_origen)
            
            await query.edit_message_reply_markup(reply_markup=None)
            
            # Actualizar el mensaje original para que el usuario no piense que está "desordenado" o "no ligado"
            texto_original = query.message.text if query.message.text else ""
            if "Guía Origen Ligada" not in texto_original:
                nuevo_texto = texto_original + f"\n🔗 **Guía Origen Ligada:** `{num_recibida}`"
                if fundo and fundo != "S/D" and "Fundo/Planta" not in texto_original:
                    nuevo_texto += f"\n🏡 **Fundo/Planta:** `{fundo}`"
                try:
                    await query.edit_message_text(nuevo_texto, parse_mode='Markdown')
                except Exception:
                    pass # Si falla al editar por formato, lo ignoramos

            await query.message.reply_text(
                f"✅ Guía `{num_hecha}` vinculada exitosamente con `{num_recibida}`.\n"
                f"Fundo asignado: `{fundo}`", 
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Error vinculando guía: {e}")
            await query.message.reply_text(f"❌ Error al vincular: {e}")

# ====================================================================
# --- HANDLER PARA TELEGRAM MINI APP (COTIZACIONES) ---
# ====================================================================
async def handle_web_app_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja el envío de datos desde la Telegram Mini App para generar o modificar cotizaciones."""
    try:
        if not update.message or not update.message.web_app_data:
            return

        raw_data = update.message.web_app_data.data
        try:
            payload = json.loads(raw_data)
        except Exception as e:
            logger.error(f"Error decodificando web_app_data: {e}")
            await update.message.reply_text("❌ Error al procesar los datos de la cotización.")
            return

        if payload.get("tipo") == "COMPLETADO":
            return

        correlativo = payload.get("correlativo", "---")
        cliente = payload.get("cliente", "---")

        msg_status = await update.message.reply_text(
            f"⏳ Procesando Cotización N°{correlativo} para {cliente}...\n"
            f"1. Clonando plantilla de Google Docs...\n"
            f"2. Insertando propuesta económica y reemplazos...\n"
            f"3. Exportando PDF y registrando en Google Sheets..."
        )

        try:
            from io import BytesIO
            res = await async_generar_cotizacion(payload)

            pdf_bytes = res["pdf_bytes"]
            nombre_pdf = res["nombre_archivo"]
            doc_link = res["doc_link"]
            pdf_link = res["pdf_link"]
            codigo = res["codigo"]
            fecha = res["fecha"]

            url_edit = obtener_url_webapp(correlativo=correlativo, datos_edicion=res["datos_json"], user_id=update.effective_user.id)

            kb = [
                [InlineKeyboardButton("✏️ Modificar Cotización", web_app=WebAppInfo(url=url_edit))],
                [InlineKeyboardButton("📄 Doc Editable", url=doc_link), InlineKeyboardButton("📂 Ver en Drive", url=pdf_link)],
                [InlineKeyboardButton("🔄 Sincronizar PDF desde Doc", callback_data=f"coti_sync|{correlativo}")],
                [InlineKeyboardButton("📋 Menú Cotizaciones", callback_data='menu_cotizaciones'), InlineKeyboardButton("❌ Cancelar", callback_data='cancelar_start')]
            ]

            caption = (
                f"✅ Cotización Generada Exitosamente\n\n"
                f"📌 Código: COTIZACION N°{codigo}\n"
                f"🏢 Cliente: {cliente}\n"
                f"📅 Fecha: {fecha}\n"
                f"💰 Ítems cotizados: {len(payload.get('items', []))} residuos\n\n"
                f"💾 Guardada en Google Drive y registrada en Sheets."
            )

            bio = BytesIO(pdf_bytes)
            bio.name = nombre_pdf

            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=bio,
                caption=caption,
                reply_markup=InlineKeyboardMarkup(kb)
            )
            try:
                await msg_status.delete()
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Error generando cotización: {e}", exc_info=True)
            err_msg = f"❌ Ocurrió un error al generar la cotización:\n{e}"
            try:
                await msg_status.edit_text(err_msg)
            except Exception:
                await update.message.reply_text(err_msg)

    except Exception as e:
        logger.error(f"Error crítico en handle_web_app_data: {e}", exc_info=True)
        if update.effective_chat:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Error en recepción: {e}")


