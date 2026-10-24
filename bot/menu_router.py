# ====================================================================
# --- bot/menu_router.py: Enrutador de Botones Persistentes ---
# ====================================================================
import html
import asyncio
from typing import Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes

from config.settings import (
    logger, MODO_GUIAS_LEER, MODO_GUIAS_REGISTRAR, MODO_GUIAS_MANUAL, MODO_COMENTAR_GUIA,
    MODO_FACTURAS_REGISTRAR, MODO_FACTURAS_BUSCAR,
    MODO_COTIZACION_BUSCAR, MODO_COTIZACION_BUSCAR_EMPRESA, MODO_COTIZACION_IA,
    MODO_PENDIENTE_ADD, MODO_CREDENCIAL_BUSCAR,
    MODO_FLOTA_CAMION_ADD, MODO_FLOTA_CONDUCTOR_ADD,
    MODO_BITACORA_ADD, MODO_BITACORA_SEARCH, MODO_BUSCAR_CLIENTE
)
from bot.keyboards import (
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
    BTN_VALORIZ_APP,
    get_persistent_main_keyboard, get_persistent_docs_keyboard, get_persistent_guias_keyboard,
    get_persistent_facturas_keyboard, get_persistent_cotizaciones_keyboard, get_persistent_certificados_keyboard,
    get_persistent_flota_keyboard, get_persistent_pendientes_keyboard, get_persistent_bitacora_keyboard,
    get_persistent_busqueda_keyboard, get_persistent_credenciales_keyboard, get_persistent_valorizaciones_keyboard,
    get_persistent_cancel_keyboard,
    _get_url_cotizaciones, _get_url_certificados, _get_url_flota,
    _get_url_pendientes, _get_url_credenciales, _get_url_valorizaciones
)


async def route_persistent_button(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    user_id: int,
    user_states: Dict[int, Any],
    user_data_cache: Dict[int, Any],
    render_cotizaciones_func=None
) -> bool:
    """
    Enruta los toques en los botones del teclado inferior persistente.
    Retorna True si el texto fue manejado como un botón de menú.
    """
    # ----------------------------------------------------------------
    # 1. SALIR / CANCELAR / VOLVER AL INICIO
    # ----------------------------------------------------------------
    if text == BTN_SALIR:
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        await update.message.reply_text(
            "👋 Entendido. Me quedo atenta cuando me necesites.\n"
            "Pulsa cualquier botón del menú inferior para continuar. ¡Que tengas un excelente día! 🌟",
            reply_markup=get_persistent_main_keyboard(user_id)
        )
        return True

    if text in (BTN_VOLVER_MENU, BTN_CANCELAR):
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        await update.message.reply_text(
            "👋 ¡Hola! Soy Lía.\nSelecciona el módulo al que deseas acceder:",
            reply_markup=get_persistent_main_keyboard(user_id)
        )
        return True

    # ----------------------------------------------------------------
    # 2. GESTIÓN DE DOCUMENTOS (SUBMENÚ Y VOLVER A DOCS)
    # ----------------------------------------------------------------
    if text in (BTN_DOCS, BTN_VOLVER_DOCS):
        user_states[user_id] = None
        texto = (
            "📑 *Gestión de Documentos*\n\n"
            "Selecciona el tipo de documento que deseas gestionar:\n\n"
            "• 📘 *Guías:* Lectura inteligente, registro en Sheets y carga manual.\n"
            "• 🧾 *Facturas:* Carga XML/PDF y búsqueda rápida.\n"
            "• 📋 *Cotizaciones:* Emisión y búsqueda de cotizaciones.\n"
            "• 📜 *Certificados:* Emisión IA, búsqueda y modificación.\n"
            "• 📊 *Valorizaciones:* Formato oficial LIQ-F-001."
        )
        await update.message.reply_text(
            texto,
            parse_mode='Markdown',
            reply_markup=get_persistent_docs_keyboard()
        )
        return True

    # ----------------------------------------------------------------
    # 3. GUÍAS DE REMISIÓN
    # ----------------------------------------------------------------
    if text == BTN_GUIAS:
        user_states[user_id] = None
        texto = (
            "📘 *Módulo de Guías de Remisión*\n\n"
            "• *Leer Guía:* Extrae texto y datos clave sin guardar.\n"
            "• *Registrar Guía:* Procesa con IA y registra en Sheets + Drive.\n"
            "• *Carga Manual:* Carga paso a paso en caso de fotos difíciles.\n"
            "• *Comentar Guía:* Añade observaciones a guías existentes."
        )
        await update.message.reply_text(
            texto,
            parse_mode='Markdown',
            reply_markup=get_persistent_guias_keyboard()
        )
        return True

    if text == BTN_GUIA_OCR:
        user_states[user_id] = MODO_GUIAS_LEER
        texto = (
            "📷 *Modo Lectura de Guía (OCR)*\n\n"
            "Envía la foto o archivo PDF de la guía.\n\n"
            "💡 _Lía extraerá todos los datos clave y los auditará según reglas de SUNAT sin registrarlos en Google Sheets._"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_GUIA_REGISTRAR:
        user_states[user_id] = MODO_GUIAS_REGISTRAR
        texto = (
            "📁 *Modo Registro de Guía*\n\n"
            "Envía la foto o archivo PDF de la guía.\n\n"
            "💡 _Lía extraerá los datos, validará la consistencia y la registrará en Google Sheets y Google Drive._"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_GUIA_MANUAL:
        user_states[user_id] = MODO_GUIAS_MANUAL
        texto = (
            "✍️ *Modo Carga Manual de Guía*\n\n"
            "Envía la foto de la guía que no sea legible por OCR.\n\n"
            "💡 _Se guardará en Drive y te pedirá los datos paso a paso (Fecha, Serie, Número, etc.)._"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_GUIA_COMENTAR:
        user_states[user_id] = MODO_COMENTAR_GUIA
        texto = (
            "💬 <b>Añadir Observación a Guía</b>\n\n"
            "Escribe el <b>N° de Guía</b> al que deseas añadir una observación (ej: <code>T001-1234</code>):"
        )
        await update.message.reply_text(texto, parse_mode='HTML', reply_markup=get_persistent_cancel_keyboard())
        return True

    # ----------------------------------------------------------------
    # 4. FACTURAS SUNAT
    # ----------------------------------------------------------------
    if text == BTN_FACTURAS:
        user_states[user_id] = None
        texto = (
            "🧾 *Módulo de Facturas SUNAT*\n\n"
            "• *Archivos XML:* Procesamiento instantáneo (100% exacto UBL 2.1 SUNAT).\n"
            "• *Archivos PDF:* Lectura inteligente y extracción con IA.\n"
            "• *Google Drive:* Respaldo automático en carpeta Facturas.\n\n"
            "Selecciona una opción en el menú inferior:"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_facturas_keyboard())
        return True

    if text == BTN_FACT_SUBIR:
        user_states[user_id] = MODO_FACTURAS_REGISTRAR
        texto = (
            "📥 *Modo Registro de Facturas*\n\n"
            "Envía el archivo de la factura (**XML** o **PDF**).\n\n"
            "💡 _Si tienes el archivo XML, envíalo directamente: se procesa al instante con precisión exacta._"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_FACT_BUSCAR:
        user_states[user_id] = MODO_FACTURAS_BUSCAR
        texto = (
            "🔍 <b>Buscar Factura en el Sheet</b>\n\n"
            "Escribe el <b>N° de Factura</b> (ej: <code>F001-4567</code>), el <b>RUC</b> o el <b>nombre del emisor</b>:"
        )
        await update.message.reply_text(texto, parse_mode='HTML', reply_markup=get_persistent_cancel_keyboard())
        return True

    # ----------------------------------------------------------------
    # 5. COTIZACIONES
    # ----------------------------------------------------------------
    if text == BTN_COTIZ:
        user_states[user_id] = None
        url_app = _get_url_cotizaciones(user_id=user_id)
        texto = (
            "📋 *Módulo de Cotizaciones - EPMI SAC*\n\n"
            "• Pulsa *➕ Nueva Cotiz.* para abrir el formulario en tu celular.\n"
            "• Pulsa *📋 5 Cotiz.* o *🏢 Por Empresa* para ver links directos y modificar.\n"
            "• Pulsa *🔍 Por Correl.* para buscar una cotización específica.\n"
            "• Pulsa *🎙️ Dictar IA* para dictar la cotización a Lía."
        )
        # Opcionalmente se agrega botón inline para abrir Mini App directo desde el mensaje
        kb_inline = [[InlineKeyboardButton("➕ Abrir Formulario en Mini App", web_app=WebAppInfo(url=url_app))]] if url_app else None
        await update.message.reply_text(
            texto,
            parse_mode='Markdown',
            reply_markup=get_persistent_cotizaciones_keyboard(user_id)
        )
        return True

    if text == BTN_COTIZ_NUEVA:
        url_app = _get_url_cotizaciones(user_id=user_id)
        kb_inline = [[InlineKeyboardButton("➕ Abrir Formulario Cotizaciones", web_app=WebAppInfo(url=url_app))]]
        await update.message.reply_text(
            "📋 Pulsa el botón abajo para abrir la Mini App de Cotizaciones:",
            reply_markup=InlineKeyboardMarkup(kb_inline)
        )
        return True

    if text == BTN_COTIZ_ULTIMAS:
        user_states[user_id] = None
        msg_wait = await update.message.reply_text("⏳ Consultando últimas cotizaciones en Sheets...")
        try:
            from core.cotizaciones_service import async_obtener_todas_cotizaciones
            cotis = await async_obtener_todas_cotizaciones(limite=5)
            if render_cotizaciones_func:
                await render_cotizaciones_func(update, context, user_id, cotis, titulo="📋 <b>Últimas 5 Cotizaciones Registradas:</b>", msg_wait=msg_wait)
            else:
                await msg_wait.edit_text(f"✅ Se encontraron {len(cotis)} cotizaciones.")
        except Exception as e:
            logger.error(f"Error listando cotizaciones desde menú persistente: {e}")
            await msg_wait.edit_text(f"❌ Error al consultar cotizaciones: {e}")
        return True

    if text == BTN_COTIZ_EMPRESA:
        user_states[user_id] = MODO_COTIZACION_BUSCAR_EMPRESA
        texto = (
            "🏢 <b>Buscar Cotizaciones por Empresa (Últimas 5):</b>\n\n"
            "Escribe el <b>nombre</b> o <b>RUC</b> de la empresa (por ejemplo: <code>Exalmar</code>, <code>Agrícola Andrea</code>, <code>Petramas</code>):"
        )
        await update.message.reply_text(texto, parse_mode='HTML', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_COTIZ_CORREL:
        user_states[user_id] = MODO_COTIZACION_BUSCAR
        texto = (
            "🔍 <b>Buscar Cotización por N° Correlativo:</b>\n\n"
            "Escribe el número correlativo (ej: <code>080</code> o <code>080-2026-EO-RS</code>):"
        )
        await update.message.reply_text(texto, parse_mode='HTML', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_COTIZ_IA:
        user_states[user_id] = MODO_COTIZACION_IA
        texto = (
            "🎙️ <b>Cotizar por Voz o Texto con IA:</b>\n\n"
            "Envía un mensaje de texto o una nota de voz explicando el servicio:\n"
            "• Cliente y RUC\n"
            "• Descripción del servicio y residuos\n"
            "• Precio o condiciones pactadas"
        )
        await update.message.reply_text(texto, parse_mode='HTML', reply_markup=get_persistent_cancel_keyboard())
        return True

    # ----------------------------------------------------------------
    # 6. CERTIFICADOS IA
    # ----------------------------------------------------------------
    if text == BTN_CERTS:
        user_states[user_id] = None
        url_app = _get_url_certificados(user_id=user_id)
        url_mod = _get_url_certificados(user_id=user_id, modo="modificar")
        texto = (
            "📄 *Sistema de Certificados IA - EPMI & INECOVE*\n\n"
            "• **🚀 App Certs:** Emisión rápida con OCR, manual o repositorio.\n"
            "• **🔄 Modif. Exp:** Sustitución quirúrgica en Google Drive conservando guías intactas.\n"
            "• **🔍 Buscar Cert:** Búsqueda rápida por fecha, empresa, fundo o correlativo."
        )
        kb_inline = [
            [InlineKeyboardButton("🚀 Abrir Certificados IA (Mini App)", web_app=WebAppInfo(url=url_app))],
            [InlineKeyboardButton("🔄 Modificar Expediente (Docs/PDF)", web_app=WebAppInfo(url=url_mod))]
        ]
        await update.message.reply_text(
            texto,
            parse_mode='Markdown',
            reply_markup=get_persistent_certificados_keyboard(user_id)
        )
        return True

    if text == BTN_CERT_APP:
        url_app = _get_url_certificados(user_id=user_id)
        kb_inline = [[InlineKeyboardButton("🚀 Abrir Mini App Certificados", web_app=WebAppInfo(url=url_app))]]
        await update.message.reply_text("📄 Pulsa abajo para abrir Certificados IA:", reply_markup=InlineKeyboardMarkup(kb_inline))
        return True

    if text == BTN_CERT_MODIF:
        url_mod = _get_url_certificados(user_id=user_id, modo="modificar")
        kb_inline = [[InlineKeyboardButton("🔄 Abrir Modificador de Expediente", web_app=WebAppInfo(url=url_mod))]]
        await update.message.reply_text("🔄 Pulsa abajo para modificar expediente:", reply_markup=InlineKeyboardMarkup(kb_inline))
        return True

    if text == BTN_CERT_BUSCAR:
        user_states[user_id] = None
        keyboard = [
            [InlineKeyboardButton("📅 Por Fecha", callback_data='criterio_fecha'), InlineKeyboardButton("🌱 Por Fundo", callback_data='criterio_fundo')],
            [InlineKeyboardButton("🔢 Por Correlativo", callback_data='criterio_corre'), InlineKeyboardButton("🏢 Por Empresa", callback_data='criterio_empresa')],
            [InlineKeyboardButton("🔙 Menú Certificados", callback_data='menu_certificados')]
        ]
        texto = (
            "🔍 *Buscar Certificado en el Repositorio*\n\n"
            "Selecciona el criterio por el cual deseas buscar:"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
        return True

    # ----------------------------------------------------------------
    # 7. VALORIZACIONES
    # ----------------------------------------------------------------
    if text == BTN_VALORIZ:
        user_states[user_id] = None
        url_app = _get_url_valorizaciones(user_id=user_id)
        texto = (
            "📊 *Módulo de Valorizaciones - EPMI SAC*\n"
            "_Formato Oficial LIQ-F-001 (Versión 1.0)_\n\n"
            "• **⚡ Heredar Pestaña:** Duplica pestaña existente con logos y fórmulas intactas.\n"
            "• **➕ Nueva:** Genera liquidación desde plantilla en blanco.\n"
            "• **✏️ Modificar:** Corrige ítems o cantidades en pestañas existentes.\n"
            "• **📄 Exportar a PDF:** Guarda directamente en Google Drive y envía por Telegram."
        )
        kb_inline = [[InlineKeyboardButton("🚀 Abrir Valorizaciones (Mini App)", web_app=WebAppInfo(url=url_app))]]
        await update.message.reply_text(
            texto,
            parse_mode='Markdown',
            reply_markup=get_persistent_valorizaciones_keyboard(user_id)
        )
        return True

    if text == BTN_VALORIZ_APP:
        url_app = _get_url_valorizaciones(user_id=user_id)
        kb_inline = [[InlineKeyboardButton("🚀 Abrir Valorizaciones", web_app=WebAppInfo(url=url_app))]]
        await update.message.reply_text("📊 Pulsa abajo para abrir Valorizaciones:", reply_markup=InlineKeyboardMarkup(kb_inline))
        return True

    # ----------------------------------------------------------------
    # 8. FLOTA & CONDUCTORES
    # ----------------------------------------------------------------
    if text == BTN_FLOTA:
        user_states[user_id] = None
        from core.flota_service import listar_camiones, listar_conductores
        camiones, conductores = await asyncio.to_thread(lambda: (listar_camiones(), listar_conductores()))
        texto = (
            "🚛 <b>Gestión de Flota & Conductores</b>\n\n"
            "Administra los camiones y choferes por separado en Google Sheets:\n\n"
            f"• 🚛 <b>Camiones registrados:</b> <code>{len(camiones)}</code>\n"
            f"• 👨‍✈️ <b>Conductores registrados:</b> <code>{len(conductores)}</code>\n\n"
            "💡 <i>Usa los botones del menú inferior para consultar catálogos o registrar nuevos.</i>"
        )
        await update.message.reply_text(texto, parse_mode='HTML', reply_markup=get_persistent_flota_keyboard(user_id))
        return True

    if text == BTN_FLOTA_APP:
        url_app = _get_url_flota(user_id=user_id)
        kb_inline = [[InlineKeyboardButton("📱 Abrir Gestor de Flota", web_app=WebAppInfo(url=url_app))]]
        await update.message.reply_text("🚛 Pulsa abajo para abrir la Mini App de Flota:", reply_markup=InlineKeyboardMarkup(kb_inline))
        return True

    if text == BTN_FLOTA_CAMIONES:
        from core.flota_service import listar_camiones
        camiones = await asyncio.to_thread(listar_camiones)
        if not camiones:
            await update.message.reply_text("ℹ️ No hay camiones registrados en la hoja.")
            return True
        PAGE_SIZE = 5
        chunk = camiones[:PAGE_SIZE]
        texto = f"🚛 <b>Catálogo de Camiones (Pág. 1 - {len(camiones)} total):</b>\n\n"
        kb_items = []
        for c in chunk:
            est_icon = "🟢" if c.get('estado') == 'Activo' else ("🟡" if c.get('estado') == 'Mantenimiento' else "🔴")
            marca_mod = f"{c.get('marca', '')} {c.get('modelo', '')}".strip()
            texto += (
                f"{est_icon} <b>Placa:</b> <code>{c.get('placa')}</code> | <b>{c.get('tipo', 'Camión')}</b>\n"
                f"   Marca/Mod: <i>{marca_mod or 'S/D'}</i>\n"
                f"   Capacidad: <code>{c.get('capacidad_peso_tm', '0')} TM</code> | <code>{c.get('capacidad_volumen_m3', '0')} m³</code>\n\n"
            )
            kb_items.append([InlineKeyboardButton(f"👁️ Placa {c.get('placa')}", callback_data=f"flota_view_camion|{c.get('placa')}")])
        if len(camiones) > PAGE_SIZE:
            kb_items.append([InlineKeyboardButton("Siguiente ➡️", callback_data="flota_list_camiones|1")])
        await update.message.reply_text(texto, reply_markup=InlineKeyboardMarkup(kb_items), parse_mode='HTML')
        return True

    if text == BTN_FLOTA_CHOFERES:
        from core.flota_service import listar_conductores
        conductores = await asyncio.to_thread(listar_conductores)
        if not conductores:
            await update.message.reply_text("ℹ️ No hay conductores registrados en la hoja.")
            return True
        PAGE_SIZE = 5
        chunk = conductores[:PAGE_SIZE]
        texto = f"👨‍✈️ <b>Directorio de Conductores (Pág. 1 - {len(conductores)} total):</b>\n\n"
        kb_items = []
        for c in chunk:
            est_icon = "🟢" if c.get('estado') == 'Activo' else ("🟡" if c.get('estado') == 'Vacaciones' else "🔴")
            texto += (
                f"{est_icon} <b>{c.get('nombre_completo')}</b>\n"
                f"   DNI: <code>{c.get('dni', 'S/D')}</code> | Lic: <code>{c.get('licencia', 'S/D')}</code> ({c.get('categoria_licencia', 'A-IIIc')})\n"
                f"   Tel: <code>{c.get('telefono', 'S/D')}</code>\n\n"
            )
            kb_items.append([InlineKeyboardButton(f"👁️ {c.get('nombre_completo')[:22]}", callback_data=f"flota_view_cond|{c.get('dni')}")])
        if len(conductores) > PAGE_SIZE:
            kb_items.append([InlineKeyboardButton("Siguiente ➡️", callback_data="flota_list_cond|1")])
        await update.message.reply_text(texto, reply_markup=InlineKeyboardMarkup(kb_items), parse_mode='HTML')
        return True

    if text == BTN_FLOTA_ADD_CAMION:
        user_states[user_id] = MODO_FLOTA_CAMION_ADD
        texto = (
            "➕ <b>Agregar Nuevo Camión a la Flota</b>\n\n"
            "Escribe los datos del camión en un mensaje. Por ejemplo:\n\n"
            "<code>Placa: AZU-781\n"
            "Marca: Volvo FMX\n"
            "Tipo: Volquete\n"
            "Capacidad: 25 TM | 20 m3</code>\n\n"
            "💡 <i>Lía identificará los campos y te mostrará el borrador antes de guardarlo en Sheets.</i>"
        )
        await update.message.reply_text(texto, reply_markup=get_persistent_cancel_keyboard(), parse_mode='HTML')
        return True

    if text == BTN_FLOTA_ADD_CHOFER:
        user_states[user_id] = MODO_FLOTA_CONDUCTOR_ADD
        texto = (
            "➕ <b>Agregar Nuevo Conductor a la Flota</b>\n\n"
            "Escribe los datos del chofer en un mensaje. Por ejemplo:\n\n"
            "<code>Nombre: Carlos Mendoza Gómez\n"
            "DNI: 45879632\n"
            "Licencia: Q45879632 (A-IIIc)\n"
            "Teléfono: 987654321</code>\n\n"
            "💡 <i>Lía armará la ficha y te pedirá confirmar antes de registrarlo.</i>"
        )
        await update.message.reply_text(texto, reply_markup=get_persistent_cancel_keyboard(), parse_mode='HTML')
        return True

    # ----------------------------------------------------------------
    # 9. PENDIENTES & ALERTAS
    # ----------------------------------------------------------------
    if text == BTN_PENDIENTES:
        user_states[user_id] = None
        url_app = _get_url_pendientes(user_id=user_id)
        texto = (
            "📌 *Módulo de Pendientes & Alertas*\n\n"
            "• Registra tareas con fecha y hora programada.\n"
            "• Lía te notificará automáticamente cuando se cumpla el plazo.\n"
            "• Puedes dictar notas de voz o escribir en lenguaje natural.\n\n"
            "Selecciona una opción en el menú inferior:"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_pendientes_keyboard(user_id))
        return True

    if text == BTN_PEND_APP:
        url_app = _get_url_pendientes(user_id=user_id)
        kb_inline = [[InlineKeyboardButton("📱 Abrir en WebApp", web_app=WebAppInfo(url=url_app))]]
        await update.message.reply_text("📌 Pulsa abajo para ver Pendientes en WebApp:", reply_markup=InlineKeyboardMarkup(kb_inline))
        return True

    if text == BTN_PEND_NUEVO:
        user_states[user_id] = MODO_PENDIENTE_ADD
        texto = (
            "✍️ *Crear Nuevo Pendiente*\n\n"
            "Escribe o envía una nota de voz con lo que necesitas recordar. Por ejemplo:\n"
            "• _\"Recordar mañana a las 9am pedir certificado a Cerro Prieto\"_\n"
            "• _\"Pedir peso de guía T001-45 en 30 minutos\"_\n"
            "• _\"Pagar flete de transporte el viernes a las 3pm\"_\n\n"
            "💡 _Lía calculará automáticamente la fecha/hora y te pedirá confirmar el borrador._"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_PEND_ACTIVOS:
        msg_wait = await update.message.reply_text("⏳ Consultando pendientes activos en Google Sheets...")
        try:
            from core.pendientes_service import async_obtener_pendientes
            pendientes = await async_obtener_pendientes(solo_activos=True)
            if not pendientes:
                await msg_wait.edit_text("🎉 <b>¡Excelente! No tienes pendientes activos por el momento.</b>", parse_mode='HTML')
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
                await msg_wait.edit_text(texto_lista, reply_markup=InlineKeyboardMarkup(kb_items), parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error listando pendientes desde menú: {e}")
            await msg_wait.edit_text(f"❌ Error al consultar pendientes: {html.escape(str(e))}")
        return True

    # ----------------------------------------------------------------
    # 10. BITÁCORA LIBRE
    # ----------------------------------------------------------------
    if text == BTN_BITACORA:
        user_states[user_id] = None
        texto = (
            "📓 *Módulo de Bitácora Libre*\n\n"
            "Registra y consulta información operativa sin formatos rígidos:\n\n"
            "• *📝 Anotar:* Envía texto, audio o foto para clasificar y guardar.\n"
            "• *🔍 Preguntar IA:* Consulta en lenguaje natural lo guardado.\n"
            "• *📋 5 Anotac:* Revisa las últimas 5 anotaciones registradas."
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_bitacora_keyboard())
        return True

    if text == BTN_BITA_ANOTAR:
        user_states[user_id] = MODO_BITACORA_ADD
        texto = (
            "✍️ **Añadiendo a Bitácora**\n\n"
            "Envíame lo que deseas registrar:\n"
            "• 📝 **Texto** con cualquier apunte o instrucción.\n"
            "• 🎙️ **Nota de voz o Audio** (lo transcribiré automáticamente).\n"
            "• 📷 **Foto o PDF** (extraeré el contenido con OCR e IA).\n\n"
            "💡 _Antes de guardar en la hoja, te mostraré el borrador para que lo confirmes._"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_BITA_PREGUNTAR:
        user_states[user_id] = MODO_BITACORA_SEARCH
        texto = (
            "🔍 **Búsqueda Conversacional en Bitácora**\n\n"
            "Pregúntame directamente lo que necesitas saber, por ejemplo:\n"
            "• _\"¿Cómo se hacen las guías de Villacurí?\"_\n"
            "• _\"¿Qué reglas hay para los certificados de Prosembra?\"_\n"
            "• _\"¿Cuál es el RUC del transportista de Beta?\"_\n\n"
            "💡 _Escribe en lenguaje natural. No necesitas comandos especiales._"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_BITA_ULTIMAS:
        msg_wait = await update.message.reply_text("⏳ Obteniendo últimas anotaciones de la Bitácora...")
        try:
            from core.bitacora_service import obtener_ultimas_anotaciones_formateadas
            texto_ultimos = await obtener_ultimas_anotaciones_formateadas(5)
            await msg_wait.edit_text(texto_ultimos, parse_mode='Markdown', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error obteniendo últimas notas: {e}")
            await msg_wait.edit_text(f"❌ Error al obtener notas: {e}")
        return True

    # ----------------------------------------------------------------
    # 11. CENTRO DE BÚSQUEDA
    # ----------------------------------------------------------------
    if text == BTN_BUSCAR:
        user_states[user_id] = None
        texto = (
            "🔍 *Centro de Búsqueda y Consultas*\n\n"
            "Selecciona la consulta que deseas realizar:\n\n"
            "• 📊 *Rep. Guías:* Consulta en 'Registro Guías' o 'Guías Recibidas'.\n"
            "• 👥 *Clientes:* Búsqueda flexible de empresas registradas.\n"
            "• 📍 *Direcciones:* Ubicaciones por empresa o fundo/planta.\n"
            "• 💬 *Comentar:* Añade observaciones a guías existentes."
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_busqueda_keyboard())
        return True

    if text == BTN_BUSQ_GUIAS:
        keyboard = [
            [InlineKeyboardButton("📘 Reporte Registro Guías", callback_data='rep_registro')],
            [InlineKeyboardButton("📥 Reporte Guías Recibidas", callback_data='rep_recibidas')],
            [InlineKeyboardButton("🔙 Menú Búsqueda", callback_data='menu_busqueda')]
        ]
        await update.message.reply_text(
            "📊 **Consultar Reportes de Guías**\n\nSelecciona qué reporte deseas consultar:",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True

    if text == BTN_BUSQ_CLIENTES:
        user_states[user_id] = MODO_BUSCAR_CLIENTE
        await update.message.reply_text(
            "👥 <b>Búsqueda de Clientes:</b>\n\nEscribe el <b>nombre</b> o <b>RUC</b> de la empresa a consultar:",
            parse_mode='HTML',
            reply_markup=get_persistent_cancel_keyboard()
        )
        return True

    if text == BTN_BUSQ_DIR:
        keyboard = [
            [InlineKeyboardButton("🏢 Por Empresa", callback_data='dir_empresa'),
             InlineKeyboardButton("🌱 Por Fundo/Planta", callback_data='dir_fundo')],
            [InlineKeyboardButton("🔙 Menú Búsqueda", callback_data='menu_busqueda')]
        ]
        await update.message.reply_text(
            "📍 **Buscar Direcciones**\n\n¿Deseas buscar por Empresa o por Fundo/Planta?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True

    # ----------------------------------------------------------------
    # 12. CREDENCIALES & ACCESOS
    # ----------------------------------------------------------------
    if text == BTN_CREDS:
        user_states[user_id] = None
        texto = (
            "🔐 *Bóveda de Credenciales & Accesos*\n\n"
            "• Consulta usuarios, contraseñas y portales.\n"
            "• SUNAT, Balanzas de Plantas, Portales de Clientes, etc.\n\n"
            "Selecciona una opción en el menú inferior:"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_credenciales_keyboard(user_id))
        return True

    if text == BTN_CREDS_APP:
        url_app = _get_url_credenciales(user_id=user_id)
        kb_inline = [[InlineKeyboardButton("📱 Abrir Bóveda en WebApp", web_app=WebAppInfo(url=url_app))]]
        await update.message.reply_text("🔐 Pulsa abajo para abrir la Bóveda:", reply_markup=InlineKeyboardMarkup(kb_inline))
        return True

    if text == BTN_CREDS_BUSCAR:
        user_states[user_id] = MODO_CREDENCIAL_BUSCAR
        texto = (
            "🔍 *Buscar Credencial o Acceso*\n\n"
            "Escribe el nombre del servicio o plataforma (ej: `SUNAT`, `Petramás`, `Balanza`, `Ventanilla`):"
        )
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=get_persistent_cancel_keyboard())
        return True

    if text == BTN_CREDS_LISTAR:
        msg_wait = await update.message.reply_text("⏳ Consultando credenciales registradas...")
        try:
            from core.credenciales_service import async_obtener_credenciales
            creds = await async_obtener_credenciales()
            if not creds:
                await msg_wait.edit_text("ℹ️ <b>No hay credenciales registradas aún en la hoja.</b>", parse_mode='HTML')
            else:
                texto_creds = f"🔐 <b>Bóveda de Credenciales ({len(creds)} registradas):</b>\n\n"
                for c in creds[:8]:
                    srv = html.escape(str(c.get('SERVICIO', '')).strip())
                    cat = html.escape(str(c.get('CATEGORIA', 'General')).strip())
                    usr = html.escape(str(c.get('USUARIO_RUC', '')).strip())
                    pwd = html.escape(str(c.get('CONTRASEÑA', '')).strip())
                    texto_creds += f"🏛️ <b>{srv}</b> <code>[{cat}]</code>\n"
                    if usr: texto_creds += f"👤 Usuario: <code>{usr}</code>\n"
                    if pwd: texto_creds += f"🔑 Clave: <code>{pwd}</code>\n"
                    texto_creds += "──────────────────\n"
                await msg_wait.edit_text(texto_creds, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error listando credenciales: {e}")
            await msg_wait.edit_text(f"❌ Error al consultar credenciales: {html.escape(str(e))}")
        return True

    return False
