# ====================================================================
# --- MÓDULO DE VALORIZACIONES (LIQUIDACIÓN DE SERVICIOS) - EPMI SAC ---
# ====================================================================
import os
import json
import time
import requests
from datetime import datetime, timezone, timedelta
import gspread
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from google.auth.transport.requests import Request

from config.settings import (
    logger,
    SHEET_VALORIZACIONES_ID,
    DRIVE_FOLDER_VALORIZACIONES_PDF,
    WEBAPP_VALORIZACIONES_URL,
    RENDER_EXTERNAL_URL
)
from core.sheets_client import obtener_credenciales

PET = timezone(timedelta(hours=-5))

def obtener_url_webapp_valorizaciones(user_id=None, modo="heredar", tab_fuente=None):
    """Construye la URL para abrir la Telegram Mini App de Valorizaciones."""
    import urllib.parse
    base_url = WEBAPP_VALORIZACIONES_URL
    if not base_url and RENDER_EXTERNAL_URL:
        base_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/valorizaciones"
    if not base_url:
        base_url = "https://lia-w2yb.onrender.com/valorizaciones"

    params = {'v': str(int(time.time()))}
    if user_id:
        params['uid'] = str(user_id)
    if modo:
        params['modo'] = str(modo)
    if tab_fuente:
        params['tab'] = str(tab_fuente)

    sep = '&' if '?' in base_url else '?'
    return f"{base_url}{sep}{urllib.parse.urlencode(params)}"

def conectar_sheet_valorizaciones():
    """Abre el libro de Valorizaciones usando credenciales de Google."""
    creds = obtener_credenciales()
    if not creds:
        raise ValueError("No se pudieron obtener credenciales válidas de Google.")
    gc = gspread.authorize(creds)
    book = gc.open_by_key(SHEET_VALORIZACIONES_ID)
    return book, creds

def listar_pestanas_valorizacion():
    """
    Lista todas las pestañas disponibles en el libro para permitir heredar o modificar.
    Retorna lista de diccionarios con info resumida de cada pestaña.
    """
    try:
        book, _ = conectar_sheet_valorizaciones()
        pestanas = []
        for ws in book.worksheets():
            # Omitir pestañas internas o de plantilla
            if ws.title.startswith('_'):
                continue
            
            # Obtener datos clave de cabecera en lote
            try:
                valores_cabecera = ws.batch_get(['C5', 'C6', 'C9', 'C10'])
                cliente = valores_cabecera[0][0][0] if valores_cabecera and valores_cabecera[0] else ""
                ruc = valores_cabecera[1][0][0] if len(valores_cabecera) > 1 and valores_cabecera[1] else ""
                liq = valores_cabecera[2][0][0] if len(valores_cabecera) > 2 and valores_cabecera[2] else ""
                fecha = valores_cabecera[3][0][0] if len(valores_cabecera) > 3 and valores_cabecera[3] else ""
            except Exception:
                cliente = ""
                ruc = ""
                liq = ""
                fecha = ""

            pestanas.append({
                "id": ws.id,
                "title": ws.title,
                "cliente": str(cliente).strip(),
                "ruc": str(ruc).strip(),
                "liquidacion": str(liq).strip(),
                "fecha": str(fecha).strip()
            })
        return pestanas
    except Exception as e:
        logger.error(f"Error listando pestañas de valorización: {e}")
        return []

def obtener_detalle_pestana(tab_id_o_nombre):
    """Lee y estructura los datos de una pestaña para edición o herencia."""
    try:
        book, _ = conectar_sheet_valorizaciones()
        if str(tab_id_o_nombre).isdigit():
            ws = book.get_worksheet_by_id(int(tab_id_o_nombre))
        else:
            ws = book.worksheet(str(tab_id_o_nombre))
            
        if not ws:
            return None

        # Leer celdas clave
        datos = {
            "title": ws.title,
            "id": ws.id,
            "cliente": str(ws.acell('C5').value or "").strip(),
            "ruc": str(ws.acell('C6').value or "").strip(),
            "planta": str(ws.acell('C7').value or "").strip(),
            "responsable": str(ws.acell('C8').value or "").strip(),
            "liquidacion": str(ws.acell('C9').value or "").strip(),
            "fecha": str(ws.acell('C10').value or "").strip(),
            "items": []
        }

        # Leer filas de servicios (fila 13 en adelante hasta encontrar fila de totales)
        filas = ws.get_all_values()
        for idx in range(12, len(filas)):
            row = filas[idx]
            if not row or not any(row):
                continue
            primera_celda = str(row[0]).strip().upper()
            if "TOTAL GENERADO" in primera_celda or "SUB TOTAL" in primera_celda or "* METODO DE PAGO" in primera_celda:
                break
            
            # Columnas: A: N°, B: Descripción, D: Fecha, E: N° Guía, F: Cantidad, G: Costo, H: Total
            desc = row[1] if len(row) > 1 else ""
            if not desc and len(row) > 0 and not row[0]:
                continue

            item_data = {
                "num": row[0] if len(row) > 0 else "",
                "descripcion": desc,
                "fecha": row[3] if len(row) > 3 else "",
                "guia": row[4] if len(row) > 4 else "",
                "cantidad": row[5] if len(row) > 5 else "1.000",
                "costo": row[6] if len(row) > 6 else "",
                "total": row[7] if len(row) > 7 else ""
            }
            datos["items"].append(item_data)

        return datos
    except Exception as e:
        logger.error(f"Error obteniendo detalle de pestaña {tab_id_o_nombre}: {e}")
        return None

def guardar_valorizacion(datos):
    """
    Crea, hereda o modifica una pestaña en el Google Sheet de Valorizaciones.
    Garantiza fórmulas automáticas (=F*G, SUM, IGV 18%, TOTAL).
    """
    try:
        book, _ = conectar_sheet_valorizaciones()
        modo = datos.get("modo", "heredar") # 'heredar', 'nueva', 'modificar'
        fuente_tab = datos.get("fuente_tab") # ID o título de la pestaña base
        nombre_pestana = str(datos.get("nombre_pestana", "")).strip()
        cliente = str(datos.get("cliente", "")).strip().upper()
        ruc = str(datos.get("ruc", "")).strip()
        planta = str(datos.get("planta", "")).strip()
        responsable = str(datos.get("responsable", "")).strip()
        liquidacion = str(datos.get("liquidacion", "")).strip()
        fecha_emision = str(datos.get("fecha", "")).strip() or datetime.now(PET).strftime("%d/%m/%Y")
        items = datos.get("items", [])

        if not nombre_pestana:
            # Autogenerar nombre de pestaña ej: AGROLATINA(SER 23)
            sigla = cliente.replace("CORPORACION ", "").replace("S.A.C.", "").replace("S.A.", "").strip()[:10]
            nombre_pestana = f"{sigla}(SER {liquidacion.lstrip('0') or '1'})"

        target_ws = None

        if modo == "modificar":
            try:
                target_ws = book.worksheet(nombre_pestana)
            except gspread.exceptions.WorksheetNotFound:
                modo = "heredar" # fallback si no existe

        if modo in ["heredar", "nueva"]:
            # Determinar pestaña a duplicar
            source_ws = None
            if modo == "heredar" and fuente_tab:
                try:
                    if str(fuente_tab).isdigit():
                        source_ws = book.get_worksheet_by_id(int(fuente_tab))
                    else:
                        source_ws = book.worksheet(str(fuente_tab))
                except Exception:
                    pass

            if not source_ws:
                # Usar plantilla base _FORMATO_VACIO
                try:
                    source_ws = book.worksheet('_FORMATO_VACIO')
                except gspread.exceptions.WorksheetNotFound:
                    # Usar la primera disponible
                    source_ws = book.worksheets()[0]

            # Si ya existe una pestaña con ese nombre, agregar sufijo o usarla
            try:
                existente = book.worksheet(nombre_pestana)
                nombre_pestana = f"{nombre_pestana}_{int(time.time()) % 1000}"
            except gspread.exceptions.WorksheetNotFound:
                pass

            target_ws = book.duplicate_sheet(source_sheet_id=source_ws.id, new_sheet_name=nombre_pestana)
            logger.info(f"📑 Pestaña '{nombre_pestana}' duplicada con éxito desde {source_ws.title} (ID: {target_ws.id})")

        # 1. Actualizar Encabezado
        updates_header = [
            {'range': 'C5', 'values': [[cliente]]},
            {'range': 'C6', 'values': [[ruc]]},
            {'range': 'C7', 'values': [[planta]]},
            {'range': 'C8', 'values': [[responsable]]},
            {'range': 'C9', 'values': [[liquidacion]]},
            {'range': 'C10', 'values': [[fecha_emision]]},
            {'range': 'E31', 'values': [[cliente]]} # Línea de firma del cliente
        ]
        target_ws.batch_update(updates_header, value_input_option='USER_ENTERED')

        # 2. Escribir filas de servicios
        # Limpiar filas 13 a 20 previas para asegurar datos nuevos limpios
        clean_ranges = [{'range': f'A{r}:H{r}', 'values': [['', '', '', '', '', '', '', '']]} for r in range(13, 22)]
        target_ws.batch_update(clean_ranges)

        fila_inicio = 13
        nuevas_filas = []
        for i, it in enumerate(items):
            r = fila_inicio + i
            num_str = str(it.get("num", "")).strip()
            desc = str(it.get("descripcion", "")).strip()
            fec = str(it.get("fecha", "-")).strip()
            guia = str(it.get("guia", "-")).strip()
            
            # Limpiar formatos numéricos
            cant_raw = str(it.get("cantidad", "1")).replace(",", ".").strip()
            costo_raw = str(it.get("costo", "0")).replace("S/.", "").replace("S/", "").replace(",", "").strip()
            try:
                cant = float(cant_raw)
            except ValueError:
                cant = 1.0
            try:
                costo = float(costo_raw)
            except ValueError:
                costo = 0.0

            formula_total = f"=F{r}*G{r}"
            fila_vals = [num_str, desc, "", fec, guia, cant, costo, formula_total]
            nuevas_filas.append({
                'range': f'A{r}:H{r}',
                'values': [fila_vals]
            })

        if nuevas_filas:
            target_ws.batch_update(nuevas_filas, value_input_option='USER_ENTERED')

        # 3. Totales con Fórmulas
        ultima_fila_items = fila_inicio + len(items) - 1 if items else 13
        fila_total_mes = ultima_fila_items + 1
        fila_subtotal = fila_total_mes + 2
        fila_igv = fila_subtotal + 1
        fila_total = fila_igv + 1

        updates_totales = [
            {'range': f'A{fila_total_mes}', 'values': [['TOTAL GENERADO EN EL MESES']]},
            {'range': f'H{fila_total_mes}', 'values': [[f'=SUM(H{fila_inicio}:H{ultima_fila_items})']]},
            {'range': f'G{fila_subtotal}', 'values': [['SUB TOTAL']]},
            {'range': f'H{fila_subtotal}', 'values': [[f'=H{fila_total_mes}']]},
            {'range': f'G{fila_igv}', 'values': [['IGV']]},
            {'range': f'H{fila_igv}', 'values': [[f'=H{fila_total}-H{fila_subtotal}']]},
            {'range': f'G{fila_total}', 'values': [['TOTAL']]},
            {'range': f'H{fila_total}', 'values': [[f'=H{fila_subtotal}*1.18']]},
        ]
        target_ws.batch_update(updates_totales, value_input_option='USER_ENTERED')

        return {
            "success": True,
            "message": f"Pestaña '{nombre_pestana}' guardada con éxito en Google Sheets.",
            "sheet_name": nombre_pestana,
            "gid": target_ws.id,
            "cliente": cliente,
            "liquidacion": liquidacion
        }
    except Exception as e:
        logger.error(f"Error guardando valorización: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def exportar_valorizacion_pdf(gid, nombre_archivo=None):
    """
    Exporta la pestaña indicada a PDF de alta fidelidad con membrete y firmas,
    lo guarda en la carpeta especificada de Google Drive (1IO6gvBVxfN73n_GC_SbH89uudkDcEmuO)
    y retorna los bytes junto con el link de descarga.
    """
    try:
        creds = obtener_credenciales()
        if (creds.expired or not creds.valid) and creds.refresh_token:
            creds.refresh(Request())

        url_export = (
            f"https://docs.google.com/spreadsheets/d/{SHEET_VALORIZACIONES_ID}/export"
            f"?format=pdf&gid={gid}&size=A4&portrait=true&fitw=true&gridlines=false"
        )
        headers = {'Authorization': f'Bearer {creds.token}'}
        resp = requests.get(url_export, headers=headers, timeout=40)

        if resp.status_code != 200:
            raise ValueError(f"Google Drive devolvió status {resp.status_code}: {resp.text[:200]}")

        pdf_bytes = resp.content

        # Nombre de archivo
        if not nombre_archivo:
            nombre_archivo = f"Valorizacion_{gid}_{int(time.time())}.pdf"
        if not nombre_archivo.lower().endswith(".pdf"):
            nombre_archivo += ".pdf"

        # Subir a Google Drive en la carpeta de PDFs designada
        drive = build('drive', 'v3', credentials=creds)
        file_meta = {
            'name': nombre_archivo,
            'parents': [DRIVE_FOLDER_VALORIZACIONES_PDF],
            'mimeType': 'application/pdf'
        }
        media = MediaInMemoryUpload(pdf_bytes, mimetype='application/pdf', resumable=True)
        uploaded = drive.files().create(
            body=file_meta,
            media_body=media,
            fields='id, name, webViewLink, webContentLink',
            supportsAllDrives=True
        ).execute()

        drive_link = uploaded.get('webViewLink', '')
        file_id = uploaded.get('id', '')
        logger.info(f"✅ PDF de Valorización guardado en Drive: {nombre_archivo} (ID: {file_id})")

        return {
            "success": True,
            "pdf_bytes": pdf_bytes,
            "drive_link": drive_link,
            "file_id": file_id,
            "filename": nombre_archivo
        }
    except Exception as e:
        logger.error(f"Error exportando valorización a PDF: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }
