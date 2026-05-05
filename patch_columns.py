import re

with open('bot/handlers.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update MEMORIA_VINCULACION in MODO_GUIAS_LEER
content = content.replace(
    '                "bot_message_id": bot_reply.message_id\\n            })',
    '                "bot_message_id": bot_reply.message_id,\\n                "enlace_drive": enlace_drive\\n            })'
)

# 2. Update MEMORIA_VINCULACION in process_manual_singuia_decision
content = content.replace(
    '            "bot_message_id": msg_id\\n        })',
    '            "bot_message_id": msg_id,\\n            "enlace_drive": cache.get("enlace_drive", "")\\n        })'
)

# 3. Update MEMORIA_VINCULACION in MODO_GUIAS_MANUAL_FUNDO
content = content.replace(
    '                "bot_message_id": msg.message_id\\n            })',
    '                "bot_message_id": msg.message_id,\\n                "enlace_drive": cache.get("enlace_drive", "")\\n            })'
)

# 4. Update MODO_GUIAS_REGISTRAR row_data
old_row_data = '''            row_data = [
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
            
            resultado_upsert = await async_upsert_row(rc.sheet_control, numero_completo, row_data, col_guia_index=2, col_comentario_index=10)'''

new_row_data = '''            # Extraer enlace_guia_recibida de la memoria vinculada si existe
            enlace_guia_recibida = ""
            if update.message.reply_to_message and user_id in MEMORIA_VINCULACION:
                reply_id = update.message.reply_to_message.message_id
                for reg in MEMORIA_VINCULACION[user_id]:
                    if reg["message_id"] == reply_id or reg.get("bot_message_id") == reply_id:
                        enlace_guia_recibida = reg.get("enlace_drive", "")
                        break

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
                "",                                          # K: Observacion ia
                "",                                          # L: Observacion
                fundo_final,                                 # M: Fundo/Planta
                "",                                          # N: Certificados
                "",                                          # O: Mes
                ""                                           # P: Sigersol
            ]
            
            resultado_upsert = await async_upsert_row(rc.sheet_control, numero_completo, row_data, col_guia_index=2, col_comentario_index=11)'''

content = content.replace(old_row_data, new_row_data)

# 5. Update handle_callback_vinculacion
old_update_cells = '''                    rc.sheet_control.update_cell(row_idx, 3, num_recibida_l) 
                    rc.sheet_control.update_cell(row_idx, 12, fundo)'''

new_update_cells = '''                    # Buscar enlace_drive de la guia recibida en MEMORIA_VINCULACION
                    enlace_recibida = ""
                    if user_id in MEMORIA_VINCULACION:
                        for reg in MEMORIA_VINCULACION[user_id]:
                            if reg["num_guia"] == num_recibida:
                                enlace_recibida = reg.get("enlace_drive", "")
                                break

                    rc.sheet_control.update_cell(row_idx, 3, num_recibida_l) 
                    rc.sheet_control.update_cell(row_idx, 10, enlace_recibida)  # J: Guia recibida
                    rc.sheet_control.update_cell(row_idx, 13, fundo)            # M: Fundo/Planta'''

content = content.replace(old_update_cells, new_update_cells)

with open('bot/handlers.py', 'w', encoding='utf-8') as f:
    f.write(content)
