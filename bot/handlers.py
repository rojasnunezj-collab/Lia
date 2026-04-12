# ====================================================================
# --- IMPORTS ---
# ====================================================================
import os
import json
import asyncio
import re
import vertexai  # <--- Agregado para inicializar la IA
from google.oauth2 import service_account  # <--- Agregado para las llaves
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import gspread
from vertexai.generative_models import Part

from config.settings import (
    logger, MODO_GUIAS_LEER, MODO_GUIAS_REGISTRAR,
    MODO_GUIAS_MANUAL, MODO_GUIAS_MANUAL_FECHA, MODO_GUIAS_MANUAL_NUMGUIA,
    MODO_GUIAS_MANUAL_TIPO, MODO_GUIAS_MANUAL_EMPRESA, MODO_GUIAS_MANUAL_FUNDO,
    MODO_REPORTE_REGISTRO, MODO_REPORTE_RECIBIDAS, MODO_COMENTAR_GUIA, MODO_COMENTAR_TEXTO,
    MODO_BUSCAR_CERT_FECHA, MODO_BUSCAR_CERT_FUNDO, 
    MODO_BUSCAR_CERT_CORRE, MODO_BUSCAR_CERT_EMPRESA,
    MODO_DIR_EMPRESA, MODO_DIR_FUNDO,
    MODO_BITACORA_ADD, MODO_BITACORA_SEARCH,
    DRIVE_FOLDER_LEER
)
from utils.helpers import clean_json_response, async_log_action
from core.ai_client import generar_con_reintento
from core.sheets_client import (
    conectar_servicios, async_get_all_records, async_buscar_link_en_drive, 
    async_subir_a_drive, sync_upsert_row, obtener_credenciales, SHEET_ID,
    SHEET_URL_DIRECT, async_upsert_row
)
import core.sheets_client as rc 

# ====================================================================
# --- INICIALIZACIÓN DE IA Y MEMORIA (BLINDAJE) ---
# ====================================================================
# --- ESTADOS (CACHÉ) ---
# ====================================================================
user_states = {}
user_data_cache = {}
MEMORIA_VINCULACION = {}

# ====================================================================
# --- HANDLERS BÁSICOS ---
# ====================================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📘 Guías", callback_data='menu_guias')],
        [InlineKeyboardButton("🔍 Búsqueda", callback_data='menu_busqueda')],
        [InlineKeyboardButton("📜 Certificados", callback_data='menu_certificados')],
        [InlineKeyboardButton("📓 Bitácora Libre", callback_data='modo_bitacora')]
    ]
    await update.message.reply_text("👋 ¡Hola! Soy Lía.\nSelecciona el módulo al que deseas acceder:", reply_markup=InlineKeyboardMarkup(keyboard))

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏓 Pong!")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if query.data == 'cancelar_operacion':
        user_states[user_id] = None
        user_data_cache[user_id] = {}
        keyboard = [
            [InlineKeyboardButton("📘 Guías", callback_data='menu_guias')],
            [InlineKeyboardButton("🔍 Búsqueda", callback_data='menu_busqueda')],
            [InlineKeyboardButton("📜 Certificados", callback_data='menu_certificados')],
            [InlineKeyboardButton("📓 Bitácora Libre", callback_data='modo_bitacora')]
        ]
        await query.edit_message_text("👋 ¡Hola! Soy Lía.\nSelecciona el módulo al que deseas acceder:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == 'volver_inicio':
        user_states[user_id] = None
        keyboard = [
            [InlineKeyboardButton("📘 Guías", callback_data='menu_guias')],
            [InlineKeyboardButton("🔍 Búsqueda", callback_data='menu_busqueda')],
            [InlineKeyboardButton("📜 Certificados", callback_data='menu_certificados')],
            [InlineKeyboardButton("📓 Bitácora Libre", callback_data='modo_bitacora')]
        ]
        await query.edit_message_text("👋 Hola! Soy Lía.\nSelecciona el módulo al que deseas acceder:", reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif query.data == 'menu_guias':
        keyboard = [
            [InlineKeyboardButton("📝 Leer Guía", callback_data='modo_guias_leer')],
            [InlineKeyboardButton("📁 Registrar Guía", callback_data='modo_guias_registrar')],
            [InlineKeyboardButton("📸 Subida Manual (Sin IA)", callback_data='modo_guias_manual')],
            [InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]
        ]
        await query.edit_message_text("📘 Módulo: Guías\nSelecciona la operación:", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'menu_busqueda':
        keyboard = [
            [InlineKeyboardButton("🔍 Buscar Reporte", callback_data='modo_buscar')],
            [InlineKeyboardButton("💬 Añadir Observación", callback_data='modo_comentar')],
            [InlineKeyboardButton("📍 Buscar Direcciones", callback_data='modo_direcciones')],
            [InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]
        ]
        await query.edit_message_text("🔍 Módulo: Búsquedas\nSelecciona la operación:", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'menu_certificados':
        keyboard = [
            [InlineKeyboardButton("📜 Buscar Certificados", callback_data='modo_buscar_cert')],
            [InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]
        ]
        await query.edit_message_text("📜 Módulo: Certificados\nSelecciona la operación:", reply_markup=InlineKeyboardMarkup(keyboard))

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
            [InlineKeyboardButton("Registro Guias", callback_data='reporte_registro')],
            [InlineKeyboardButton("Guias Recibidas", callback_data='reporte_recibidas')],
            [InlineKeyboardButton("❌ Cancelar", callback_data='menu_busqueda')]
        ]
        await query.edit_message_text("🔍 Buscar Reporte\n¿En qué base de datos deseas buscar?", reply_markup=InlineKeyboardMarkup(keyboard))
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
    elif query.data == 'modo_buscar_cert':
        keyboard = [
            [InlineKeyboardButton("📅 Por Fecha", callback_data='cert_search_fecha'),
             InlineKeyboardButton("🏡 Por Fundo", callback_data='cert_search_fundo')],
            [InlineKeyboardButton("🏢 Por Empresa", callback_data='cert_search_empresa'),
             InlineKeyboardButton("🔢 Por Correlativo", callback_data='cert_search_corre')],
            [InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]
        ]
        await query.edit_message_text("📜 Buscar Certificados\n¿Por qué criterio deseas buscar?", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'cert_search_fecha':
        user_states[user_id] = MODO_BUSCAR_CERT_FECHA
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_buscar_cert')]]
        await query.edit_message_text("📅 Modo Certificados: Fecha\nEscribe la fecha (Ej: 30/03/2026 o 30/03):", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'cert_search_fundo':
        user_states[user_id] = MODO_BUSCAR_CERT_FUNDO
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_buscar_cert')]]
        await query.edit_message_text("🏡 Modo Certificados: Fundo\nEscribe el nombre del Fundo:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'cert_search_empresa':
        user_states[user_id] = MODO_BUSCAR_CERT_EMPRESA
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_buscar_cert')]]
        await query.edit_message_text("🏢 Modo Certificados: Empresa\nEscribe el nombre de la Empresa:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'cert_search_corre':
        user_states[user_id] = MODO_BUSCAR_CERT_CORRE
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_buscar_cert')]]
        await query.edit_message_text("🔢 Modo Certificados: Correlativo\nEscribe el correlativo a buscar:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_direcciones':
        keyboard = [
            [InlineKeyboardButton("🏢 Por Empresa", callback_data='dir_buscar_empresa'),
             InlineKeyboardButton("🏡 Por Fundo/Planta", callback_data='dir_buscar_fundo')],
            [InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]
        ]
        await query.edit_message_text("📍 **Buscador de Direcciones**\n¿Por qué criterio deseas buscar?", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'dir_buscar_empresa':
        user_states[user_id] = MODO_DIR_EMPRESA
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_direcciones')]]
        await query.edit_message_text("🏢 Direcciones: Por Empresa\nIngresa un nombre parcial o clave de la empresa (Ej: 'Villa'):", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'dir_buscar_fundo':
        user_states[user_id] = MODO_DIR_FUNDO
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_direcciones')]]
        await query.edit_message_text("🏡 Direcciones: Por Fundo/Planta\nIngresa un nombre parcial o clave del Fundo/Planta:", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'modo_bitacora':
        keyboard = [
            [InlineKeyboardButton("✍️ Nueva Anotación/Archivo", callback_data='bitacora_add')],
            [InlineKeyboardButton("🔎 Buscar en Bitácora", callback_data='bitacora_search')],
            [InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]
        ]
        await query.edit_message_text("📓 Bitácora Libre\n¿Qué deseas hacer en tu Bitácora?", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'bitacora_add':
        user_states[user_id] = MODO_BITACORA_ADD
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_bitacora')]]
        await query.edit_message_text("✍️ Añadiendo a Bitácora\nSube una Foto, PDF, Audio o Video para adjuntar. O envíame texto directamente para anotar.", reply_markup=InlineKeyboardMarkup(kb))
    elif query.data == 'bitacora_search':
        user_states[user_id] = MODO_BITACORA_SEARCH
        kb = [[InlineKeyboardButton("❌ Cancelar", callback_data='modo_bitacora')]]
        await query.edit_message_text("🔎 Buscando en Bitácora\nEscribe el texto, nombre de usuario o fecha que deseas encontrar en tus anotaciones guardadas:", reply_markup=InlineKeyboardMarkup(kb))
# ====================================================================
# --- HANDLER DE TEXTO Y BÚSQUEDA ---
# ====================================================================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    modo = user_states.get(user_id)
    
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
                    return book2.worksheet("Guias_recibidas").get_all_records()
                    
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
                reporte = f"✅ **REPORTE: {len(encontrados)} Coincidencias Encontradas**\n\n"
                for r in encontrados:
                    origen = r.get('_origen')
                    num_guia = str(r.get('N° Guía', r.get('Numero Guia', r.get('Nro Guia', 'S/D'))))
                    tipo_guia = r.get('Tipo Guía', r.get('Tipo', 'S/D'))
                    empresa = r.get('Empresa Principal', r.get('Empresa', 'S/D'))
                    
                    if origen == "Registro_Guias":
                        entidad_1 = r.get('Destinatario/Remitente', 'S/D')
                        entidad_2 = r.get('Destinario/Proveedor', 'S/D')
                    else:
                        vals = list(r.values())
                        entidad_1 = str(vals[4]) if len(vals) >= 5 else "S/D"
                        entidad_2 = "S/D"
                        
                    enlace = str(r.get('Link Drive', '')).strip()

                    reporte += f"🗂️ **Base:** `{origen}`\n"
                    reporte += f"📄 **Guía:** `{num_guia}`\n"
                    reporte += f"🏷️ **Tipo:** `{tipo_guia}`\n"
                    reporte += f"🏢 **Empresa:** `{empresa}`\n"
                    if origen == "Registro_Guias":
                        reporte += f"👤 **Dest/Rem:** `{entidad_1}`\n"
                        reporte += f"👤 **Dest/Prov:** `{entidad_2}`\n"
                    else:
                        reporte += f"🏡 **Fundo/Planta:** `{entidad_1}`\n"
                    
                    if enlace.startswith("http"):
                        reporte += f"🔗 [Link]({enlace})\n"
                    elif enlace:
                        link_rescatado = await async_buscar_link_en_drive(enlace)
                        if link_rescatado:
                            reporte += f"🔗 [Link]({link_rescatado})\n"
                        else:
                            reporte += f"🔗 _Documento no encontrado en Drive_\n"
                    else:
                        reporte += f"🔗 _Sin enlace en base de datos_\n"
                    
                    reporte += "➖➖➖➖➖➖➖➖➖➖\n"
                
                if len(reporte) > 4000:
                    reporte = reporte[:4000] + "\n\n⚠️ _[Reporte recortado por límite de caracteres de Telegram]_"
                    
                await msg.edit_text(reporte, parse_mode='Markdown', disable_web_page_preview=True)
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
                rc.sheet_control.update_cell(row_idx, 11, comentario)

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
            def fetch_historial():
                creds = obtener_credenciales()
                client = gspread.authorize(creds)
                book2 = client.open_by_key(SHEET_ID)
                return book2.worksheet("Historial").get_all_records()
                
            registros = await asyncio.to_thread(fetch_historial)
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

    elif modo == MODO_GUIAS_MANUAL_FECHA:
        user_data_cache[user_id]['fecha'] = str(update.message.text).strip()
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
        msg = await update.message.reply_text("⏳ Guardando registro manual en Guias_recibidas...")
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
                return sync_upsert_row(sheet_recibidas, cache.get('num_guia', ''), row_data, col_guia_index=2, col_comentario_index=7)
            resultado = await asyncio.to_thread(save_manual)
            estado = "🔄 *Guía Actualizada*" if resultado == "updated" else "✅ *Nueva Guía Registrada Manualmente*"
            enlace = cache.get('enlace_drive', '')
            await msg.edit_text(
                f"{estado}\n\n"
                f"📅 **Fecha:** `{cache.get('fecha', 'S/D')}`\n"
                f"📄 **N° Guía:** `{cache.get('num_guia', 'S/D')}`\n"
                f"🏷️ **Tipo:** `{cache.get('tipo_guia', 'S/D')}`\n"
                f"🏢 **Empresa:** `{cache.get('empresa', 'S/D')}`\n"
                f"🏡 **Fundo:** `{cache.get('fundo', 'S/D')}`\n\n"
                f"📁 [Ver en Drive]({enlace})",
                parse_mode='Markdown', disable_web_page_preview=True
            )
            
            # --- MEMORIA DE VINCULACIÓN HÍBRIDA (MANUAL) ---
            if user_id not in MEMORIA_VINCULACION:
                MEMORIA_VINCULACION[user_id] = []
            MEMORIA_VINCULACION[user_id].append({
                "num_guia": cache.get('num_guia', 'S/D'),
                "fundo": cache.get('fundo', 'S/D'),
                "message_id": cache.get('img_message_id', update.message.message_id),
                "bot_message_id": msg.message_id
            })
            if len(MEMORIA_VINCULACION[user_id]) > 5:
                MEMORIA_VINCULACION[user_id].pop(0)
            # ----------------------------------------
            
            user_states[user_id] = None
            user_data_cache[user_id] = {}
        except Exception as e:
            logger.error(f"Error en guardado manual: {e}")
            await msg.edit_text(f"❌ Error al guardar el registro manual: {e}")

    elif modo == MODO_BITACORA_SEARCH:
        raw_query = str(update.message.text).strip()
        query_upper = raw_query.upper()
        msg = await update.message.reply_text(f"⏳ Buscando `{raw_query}` en la Bitácora...")
        
        try:
            def fetch_bitacora():
                creds = obtener_credenciales()
                client = gspread.authorize(creds)
                book2 = client.open_by_key(SHEET_ID)
                try:
                    return book2.worksheet("Bitacora").get_all_records()
                except gspread.exceptions.WorksheetNotFound:
                    return []
                    
            registros = await asyncio.to_thread(fetch_bitacora)
            encontrados = []
            
            for r in registros:
                fecha = str(r.get('Fecha', '')).strip()
                usuario = str(r.get('Usuario', '')).strip().upper()
                nota = str(r.get('Nota/Comentario', '')).strip().upper()
                tipo_archivo = str(r.get('Tipo Archivo', '')).strip().upper()
                
                if (query_upper in fecha.upper()) or (query_upper in usuario) or (query_upper in nota) or (query_upper in tipo_archivo):
                    encontrados.append(r)
            
            if encontrados:
                reporte = f"✅ **REPORTE: {len(encontrados)} Registros Encontrados**\n\n"
                for r in encontrados:
                    fecha_val = str(r.get('Fecha', 'S/D'))
                    usuario_val = str(r.get('Usuario', 'S/D'))
                    tipo_archivo_val = str(r.get('Tipo Archivo', 'S/D'))
                    nota_val = str(r.get('Nota/Comentario', '')).strip()
                    enlace_drive = str(r.get('Enlace Drive', '')).strip()
                    
                    reporte += f"📅 **Fecha:** `{fecha_val}`\n"
                    reporte += f"👤 **Usuario:** `{usuario_val}`\n"
                    reporte += f"📂 **Tipo:** `{tipo_archivo_val}`\n"
                    if nota_val:
                        reporte += f"📝 **Nota:** _{nota_val}_\n"
                    if enlace_drive:
                        reporte += f"📎 [Ver Archivo]({enlace_drive})\n"
                        
                    reporte += "➖➖➖➖➖➖➖➖➖➖\n"
                
                if len(reporte) > 4000:
                    reporte = reporte[:4000] + "\n\n⚠️ _[Reporte recortado]_"
                    
                await msg.edit_text(reporte, parse_mode='Markdown', disable_web_page_preview=True)
            else:
                await msg.edit_text("❌ No se encontraron registros con ese término en la Bitácora.")
        except Exception as e:
            logger.error(f"Error en búsqueda de bitácora: {e}")
            await msg.edit_text(f"❌ Error en la búsqueda: {e}")
            
    elif modo == MODO_BITACORA_ADD:
        texto = str(update.message.text).strip()
        msg = await update.message.reply_text("⏳ Guardando anotación en la Bitácora libre...")
        
        try:
            from datetime import datetime, timezone, timedelta
            PET = timezone(timedelta(hours=-5))
            timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M")
            username = update.effective_user.username or update.effective_user.first_name
            
            def save_bitacora():
                creds = obtener_credenciales()
                client = gspread.authorize(creds)
                book2 = client.open_by_key(SHEET_ID)
                
                try: 
                    sheet_bitacora = book2.worksheet("Bitacora")
                except gspread.exceptions.WorksheetNotFound:
                    sheet_bitacora = book2.add_worksheet(title="Bitacora", rows="1000", cols="8")
                    sheet_bitacora.append_row(["Fecha", "Usuario", "Tipo Archivo", "Enlace Drive", "Nota/Comentario"])

                row_data = [timestamp, username, "Texto/Anotación", "", texto]
                sheet_bitacora.append_row(row_data, value_input_option='USER_ENTERED')
                
            await asyncio.to_thread(save_bitacora)
            await msg.edit_text(f"📓✅ Anotación registrada en Bitácora con éxito:\n\n_{texto}_", parse_mode='Markdown')
        except Exception as e:
            await msg.edit_text(f"❌ Error al guardar en Bitácora: {e}")

# ====================================================================
# --- HANDLER DE ARCHIVOS Y MULTIMEDIA ---
# ====================================================================
async def handle_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    modo = user_states.get(user_id)
    if modo not in [MODO_GUIAS_LEER, MODO_GUIAS_REGISTRAR, MODO_GUIAS_MANUAL, MODO_BITACORA_ADD]: return

    msg = await update.message.reply_text("⏳ Analizando y procesando archivo...")
    file_path = f"archivo_{user_id}_{update.message.id}.jpg"
    
    try:
        tipo_archivo = "Desconocido"
        if update.message.photo:
            f = await update.message.photo[-1].get_file()
            mime_type = "image/jpeg"
            tipo_archivo = "Foto"
        elif update.message.document:
            f = await update.message.document.get_file()
            mime_type = update.message.document.mime_type
            if 'pdf' in mime_type: file_path = file_path.replace('.jpg', '.pdf')
            tipo_archivo = "Documento"
        elif update.message.voice:
            f = await update.message.voice.get_file()
            mime_type = update.message.voice.mime_type
            file_path = file_path.replace('.jpg', '.ogg')
            tipo_archivo = "Nota de Voz"
        elif update.message.audio:
            f = await update.message.audio.get_file()
            mime_type = update.message.audio.mime_type
            file_path = file_path.replace('.jpg', '.mp3')
            tipo_archivo = "Audio"
        elif update.message.video:
            f = await update.message.video.get_file()
            mime_type = update.message.video.mime_type
            file_path = file_path.replace('.jpg', '.mp4')
            tipo_archivo = "Video"
        else: return

        await f.download_to_drive(file_path)
        
        if modo == MODO_GUIAS_MANUAL:
            folder_solo_leer = DRIVE_FOLDER_LEER
            enlace_drive = await async_subir_a_drive(file_path, mime_type, folder_id=folder_solo_leer)
            user_data_cache[user_id]['enlace_drive'] = enlace_drive
            user_data_cache[user_id]['img_message_id'] = update.message.message_id
            user_states[user_id] = MODO_GUIAS_MANUAL_FECHA
            kb = [[InlineKeyboardButton("🔙 Volver", callback_data='volver_inicio')]]
            await msg.edit_text("✅ Imagen subida a Drive.\n\n📅 Paso 1/5 — Escribe la Fecha de la guía (Ej: 04/04/2026):", reply_markup=InlineKeyboardMarkup(kb))
            return
        
        if modo == MODO_BITACORA_ADD:
            enlace_drive = await async_subir_a_drive(file_path, mime_type)
            comentario = update.message.caption if update.message.caption else ""
            
            from datetime import datetime, timezone, timedelta
            PET = timezone(timedelta(hours=-5))
            timestamp = datetime.now(PET).strftime("%d/%m/%Y %H:%M")
            username = update.effective_user.username or update.effective_user.first_name
            
            def save_bitacora_file():
                creds = obtener_credenciales()
                client = gspread.authorize(creds)
                book2 = client.open_by_key(SHEET_ID)
                try: 
                    sheet_bitacora = book2.worksheet("Bitacora")
                except gspread.exceptions.WorksheetNotFound:
                    sheet_bitacora = book2.add_worksheet(title="Bitacora", rows="1000", cols="8")
                    sheet_bitacora.append_row(["Fecha", "Usuario", "Tipo Archivo", "Enlace Drive", "Nota/Comentario"])

                row_data = [timestamp, username, tipo_archivo, enlace_drive, comentario]
                sheet_bitacora.append_row(row_data, value_input_option='USER_ENTERED')
                
            await asyncio.to_thread(save_bitacora_file)
            await msg.delete()
            await update.message.reply_text(f"📓✅ {tipo_archivo} subido a la Bitácora con éxito.\n📁 [Acceder al Archivo]({enlace_drive})", parse_mode='Markdown', disable_web_page_preview=True)
            return

        with open(file_path, "rb") as bf: content = bf.read()
        part = Part.from_data(data=content, mime_type=mime_type)

        prompt = f"""
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
        {{
          "datos_sheet": {{
            "fecha": "[FECHA]", "serie": "[SERIE]", "correlativo": "[CORRELATIVO]", "tipo": "[REMITENTE/TRANSPORTISTA]", 
            "empresa": "[EMPRESA_PRINCIPAL]", "ruc_emisor": "[RUC_EMISOR]", "motivo": "[MOTIVO]",
            "entidad_1": "[NOMBRE ENTIDAD 1]", "entidad_2": "[NOMBRE ENTIDAD 2]",
            "peso_total": "[PESO TOTAL BRUTO]", "fundo_planta": "[FUNDO O PLANTA]",
            "dpto_partida": "[DPTO_P]", "prov_partida": "[PROV_P]", "dist_partida": "[DIST_P]",
            "dpto_llegada": "[DPTO_LL]", "prov_llegada": "[PROV_LL]", "dist_llegada": "[DIST_LL]"
          }},
          "full_text": "📅 **Datos Principales**\\nFecha: `[FECHA]`\\nTipo: `[TIPO]`\\nSerie: `[SERIE]`\\nNúmero: `[CORRELATIVO]`\\n🔄 Motivo: `[MOTIVO]`\\n\\n🏢 **Empresa Emisora**: `[EMPRESA_PRINCIPAL]`\\n🆔 **RUC Emisor**: `[RUC_EMISOR]`\\n👤 **Entidad 1 (Rem/Dest)**: `[ENTIDAD_1]`\\n👤 **Entidad 2 (Dest/Prov)**: `[ENTIDAD_2]`\\n\\n📍 **Partida**: `[DIR_PARTIDA]`\\n🗺️ Dpto: `[DPTO_P]` | Prov: `[PROV_P]` | Dist: `[DIST_P]`\\n\\n🏁 **Llegada**: `[DIR_LLEGADA]`\\n🗺️ Dpto: `[DPTO_LL]` | Prov: `[PROV_LL]` | Dist: `[DIST_LL]`\\n\\n🚚 **Transporte**\\nPlaca: `[PLACA]`\\nChofer: `[CHOFER]`\\nLicencia: `[LICENCIA]`\\n\\n📦 **Productos Detallados**\\n[LISTA_DE_TODOS_LOS_PRODUCTOS]\\n\\n⚖️ **Peso Bruto Total:** `[PESO TOTAL BRUTO]`"
        }}
        """

        response = await generar_con_reintento([part], prompt, msg, is_json=True)
        data = json.loads(clean_json_response(response.text))
        datos_sheet = data.get("datos_sheet", {})
        
        if datos_sheet.get("tipo", "").upper() == "TRANSPORTISTA":
            datos_sheet["motivo"] = "Servicio de Transporte"
            
        numero_completo = f"{datos_sheet.get('serie', '')}-{datos_sheet.get('correlativo', '')}"
        
        full_report = data.get("full_text", "Error formateando el reporte")
        full_report = re.sub(r'👤 \*\*Entidad 2 \(Dest/Prov\)\*\*: `S/D`\n?', '', full_report)
        full_report = full_report.replace("Motivo: `None`", "Motivo: `Servicio de Transporte`")
        
        if modo == MODO_GUIAS_LEER:
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
            await msg.delete()
            bot_reply = await update.message.reply_text(full_report + footer, parse_mode='Markdown')

            # --- MEMORIA DE VINCULACIÓN HÍBRIDA ---
            if user_id not in MEMORIA_VINCULACION:
                MEMORIA_VINCULACION[user_id] = []
            MEMORIA_VINCULACION[user_id].append({
                "num_guia": numero_completo,
                "fundo": datos_sheet.get("fundo_planta", "S/D"),
                "message_id": update.message.message_id,
                "bot_message_id": bot_reply.message_id
            })
            if len(MEMORIA_VINCULACION[user_id]) > 5:
                MEMORIA_VINCULACION[user_id].pop(0)
            # ----------------------------------------

        elif modo == MODO_GUIAS_REGISTRAR:
            enlace_drive = await async_subir_a_drive(file_path, mime_type)
            if not rc.sheet_control: await asyncio.to_thread(conectar_servicios)
            
            # --- MEMORIA DE VINCULACIÓN HÍBRIDA (LECTURA) ---
            guia_ligada_limpia = ""
            fundo_vinculado = ""
            if update.message.reply_to_message:
                reply_id = update.message.reply_to_message.message_id
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
                            break
            # ------------------------------------------------
            
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
                enlace_drive,                                # I: Link Drive
                "",                                          # J: Observacion ia 
                "",                                          # K: Observacion
                fundo_final,                                 # L: Fundo/Planta
                ""                                           # M: Certificados
            ]
            
            resultado_upsert = await async_upsert_row(rc.sheet_control, numero_completo, row_data, col_guia_index=2, col_comentario_index=10)
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
            
            await msg.delete()
            
            # --- TECLADO DE VINCULACIÓN ---
            markup = None
            if not update.message.reply_to_message and user_id in MEMORIA_VINCULACION and len(MEMORIA_VINCULACION[user_id]) > 0:
                botones = []
                for reg in reversed(MEMORIA_VINCULACION[user_id]):
                    n_rec = reg["num_guia"]
                    f_rec = reg["fundo"]
                    f_rec_short = f_rec[:10] + "..." if len(f_rec) > 10 else f_rec
                    cb_data = f"vinc|{numero_completo}|{n_rec}|{f_rec}"
                    if len(cb_data) > 64:
                        cb_data = cb_data[:64]
                    botones.append([InlineKeyboardButton(f"Vincular con {n_rec} ({f_rec_short})", callback_data=cb_data)])
                markup = InlineKeyboardMarkup(botones)
                
            if markup:
                await update.message.reply_text(resumen_registro, parse_mode='Markdown', disable_web_page_preview=True, reply_markup=markup)
            else:
                await update.message.reply_text(resumen_registro, parse_mode='Markdown', disable_web_page_preview=True)
                
    except Exception as e:
        logger.error(f"Error: {e}")
        await msg.edit_text(f"❌ Error durante el procesamiento: {e}")
    finally:
        if os.path.exists(file_path): os.remove(file_path)

# ====================================================================
# --- HANDLER CALLBACK VINCULACION ---
# ====================================================================
async def handle_callback_vinculacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
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
                if num_hecha in col_values:
                    row_idx = col_values.index(num_hecha) + 1
                    
                    if "-" in num_recibida:
                        p = num_recibida.split("-")
                        num_recibida_l = f"{p[0]}-{p[1].lstrip('0')}"
                    else:
                        num_recibida_l = num_recibida.lstrip('0')
                        
                    rc.sheet_control.update_cell(row_idx, 3, num_recibida_l) 
                    rc.sheet_control.update_cell(row_idx, 12, fundo) 
            await asyncio.to_thread(update_origen)
            
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text(
                f"✅ Guía `{num_hecha}` vinculada exitosamente con `{num_recibida}`.\n"
                f"Fundo asignado: `{fundo}`", 
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Error vinculando guía: {e}")
            await query.message.reply_text(f"❌ Error al vincular: {e}")

