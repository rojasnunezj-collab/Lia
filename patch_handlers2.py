import re

with open('bot/handlers.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Insert the interception block in handle_document
interception_code = """
        if numero_completo in ["SIN GUIA", "SIN GUIA-SIN GUIA", "S/D-S/D", "-", "S/D", ""]:
            reply_id = update.message.reply_to_message.message_id if (update.message and update.message.reply_to_message) else None
            user_data_cache[user_id] = user_data_cache.get(user_id, {})
            user_data_cache[user_id]['pending_singuia'] = {
                'datos_sheet': datos_sheet,
                'full_report': full_report,
                'file_path': file_path,
                'mime_type': mime_type,
                'modo': modo,
                'msg_id': msg.message_id,
                'reply_id': reply_id
            }
            kb = [
                [InlineKeyboardButton("✅ Registrar como NUEVA", callback_data='doc_sg_nueva')],
                [InlineKeyboardButton("🔄 Actualizar Existente", callback_data='doc_sg_actualizar')]
            ]
            await msg.edit_text(
                full_report + "\\n\\n⚠️ **ATENCIÓN:** La IA detectó esta imagen como SIN GUIA.\\n¿Deseas registrarla como una guía NUEVA o ACTUALIZAR la primera guía SIN GUIA que encuentre en tu Excel?",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
            return
"""

# Insert before 'if modo == MODO_GUIAS_LEER:'
target = '        if modo == MODO_GUIAS_LEER:'
content = content.replace(target, interception_code + '\\n' + target)

# 2. Extract the rest of handle_document to create the new helper
with open('dump_chunk.py', 'r', encoding='utf-8') as f:
    helper_logic = f.read()

# Replace msg.delete() with context.bot.delete_message
helper_logic = helper_logic.replace('await msg.delete()', 'try:\\n                await context.bot.delete_message(chat_id=user_id, message_id=msg_id)\\n            except: pass')
# Replace update.message.reply_text with update.callback_query.message.reply_text for callback
helper_logic = helper_logic.replace('await update.message.reply_text', 'await (update.message or update.callback_query.message).reply_text')
# Fix reply_to_message check
helper_logic = helper_logic.replace('if update.message.reply_to_message:', 'if reply_id:')
helper_logic = helper_logic.replace('reply_id = update.message.reply_to_message.message_id', 'pass')

helper_function = f"""
async def process_singuia_decision(update, context, user_id, pending_data, force_update):
    datos_sheet = pending_data['datos_sheet']
    full_report = pending_data['full_report']
    file_path = pending_data['file_path']
    mime_type = pending_data['mime_type']
    modo = pending_data['modo']
    msg_id = pending_data['msg_id']
    reply_id = pending_data['reply_id']
    numero_completo = "SIN GUIA"
    
{helper_logic}
"""
helper_function = helper_function.replace('col_comentario_index=7)', 'col_comentario_index=7, allow_singuia_update=force_update)')
helper_function = helper_function.replace('col_comentario_index=10)', 'col_comentario_index=10, allow_singuia_update=force_update)')

# Add helper function right before handle_document
target2 = 'async def handle_document('
content = content.replace(target2, helper_function + '\\n' + target2)

# 3. Add callback handlers in button_handler
callback_code = """
    elif query.data in ['doc_sg_nueva', 'doc_sg_actualizar']:
        force_update = (query.data == 'doc_sg_actualizar')
        pending_data = user_data_cache.get(user_id, {}).get('pending_singuia')
        if not pending_data:
            await query.answer("Datos expirados.")
            return
        await query.answer("Procesando...")
        try:
            await context.bot.edit_message_text("⏳ Guardando decisión en Sheets...", chat_id=user_id, message_id=pending_data['msg_id'])
        except: pass
        await process_singuia_decision(update, context, user_id, pending_data, force_update)
        return
"""
# Insert in button_handler after 'query.answer()'
target3 = 'async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):\\n    query = update.callback_query\\n    user_id = update.effective_user.id\\n    await query.answer()'
content = content.replace(target3, target3 + '\\n' + callback_code)

with open('bot/handlers.py', 'w', encoding='utf-8') as f:
    f.write(content)
