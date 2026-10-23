# ====================================================================
# --- MÓDULO DE VALORIZACIONES (LIQUIDACIÓN DE SERVICIOS) - EPMI SAC ---
# ====================================================================
import os
import io
import time
import re
import datetime
from datetime import timezone, timedelta
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload, MediaInMemoryUpload
from google.auth.transport.requests import Request
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
import openpyxl

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

def formatear_nombre_pdf(nombre_pestana):
    """Genera el nombre estándar: VALORIZACION DE SERVICIOS - EPMI - {nombre_pestana}.pdf"""
    clean_tab = str(nombre_pestana or "VALORIZACION").strip()
    # Asegurar espacio antes de (SER ej: AGROLATINA(SER 23) -> AGROLATINA (SER 23)
    clean_tab = re.sub(r'\(SER\s*(\d+)\)', r' (SER \1)', clean_tab, flags=re.IGNORECASE)
    clean_tab = re.sub(r'\(SERV\s*(\d+)\)', r' (SER \1)', clean_tab, flags=re.IGNORECASE)
    clean_tab = clean_tab.replace("/", "-").replace("\\", "-").replace(":", "-")
    return f"VALORIZACION DE SERVICIOS - EPMI - {clean_tab}.pdf"

def descargar_workbook_drive(drive_service, doc_id=None, data_only=False):
    """Descarga el archivo Excel de Google Drive y lo abre con openpyxl."""
    if not doc_id:
        doc_id = SHEET_VALORIZACIONES_ID
    request = drive_service.files().get_media(fileId=doc_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    wb = openpyxl.load_workbook(fh, data_only=data_only)
    return wb

def subir_workbook_drive(drive_service, wb, doc_id=None):
    """Guarda el workbook de openpyxl y actualiza el archivo en Google Drive."""
    if not doc_id:
        doc_id = SHEET_VALORIZACIONES_ID
    out_fh = io.BytesIO()
    wb.save(out_fh)
    out_fh.seek(0)
    media = MediaIoBaseUpload(
        out_fh,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        resumable=True
    )
    drive_service.files().update(fileId=doc_id, media_body=media).execute()
    logger.info(f"💾 Archivo Excel {doc_id} actualizado en Google Drive exitosamente.")

def limpiar_monto_numerico(val):
    """Convierte un valor de celda a float numérico limpio."""
    if val is None or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    val_str = str(val).replace("S/.", "").replace("S/", "").replace("$", "").replace(" ", "").strip()
    if "." in val_str and "," in val_str:
        val_str = val_str.replace(".", "").replace(",", ".")
    elif "," in val_str and "." not in val_str:
        val_str = val_str.replace(",", ".")
    try:
        clean = re.sub(r"[^\d.-]", "", val_str)
        return float(clean) if clean else 0.0
    except Exception:
        return 0.0

def formatear_fecha_str(val):
    """Convierte fecha de celda a formato DD/MM/YYYY."""
    if val is None:
        return ""
    if isinstance(val, (datetime.datetime, datetime.date)):
        return f"{val.day:02d}/{val.month:02d}/{val.year}"
    val_s = str(val).strip()
    return val_s if val_s else ""

def listar_pestanas_valorizacion():
    """
    Lista todas las pestañas disponibles en el archivo de Google Drive.
    Retorna la lista ordenada con la última pestaña creada.
    """
    try:
        creds = obtener_credenciales()
        drive = build('drive', 'v3', credentials=creds)
        wb = descargar_workbook_drive(drive, data_only=True)

        pestanas = []
        for name in wb.sheetnames:
            if name.startswith('_'):
                continue
            ws = wb[name]
            cliente = str(ws['C5'].value or "").strip()
            
            ruc_raw = ws['C6'].value
            if isinstance(ruc_raw, float):
                ruc = f"{int(ruc_raw)}"
            else:
                ruc = str(ruc_raw or "").replace(".0", "").strip()

            liq_raw = ws['C9'].value
            if isinstance(liq_raw, float):
                liq = f"{int(liq_raw):03d}"
            else:
                liq = str(liq_raw or "").replace(".0", "").strip()

            fecha = formatear_fecha_str(ws['C10'].value)

            pestanas.append({
                "id": name,
                "title": name,
                "cliente": cliente,
                "ruc": ruc,
                "liquidacion": liq,
                "fecha": fecha
            })
        return pestanas
    except Exception as e:
        logger.error(f"Error listando pestañas de valorización: {e}", exc_info=True)
        return []

def obtener_detalle_pestana(tab_name):
    """Lee y estructura los datos y filas de una pestaña de valorización."""
    try:
        creds = obtener_credenciales()
        drive = build('drive', 'v3', credentials=creds)
        wb = descargar_workbook_drive(drive, data_only=True)

        if str(tab_name) not in wb.sheetnames:
            matched = [s for s in wb.sheetnames if str(tab_name).upper() in s.upper()]
            if matched:
                tab_name = matched[0]
            else:
                tab_name = wb.sheetnames[-1]

        ws = wb[tab_name]

        cliente = str(ws['C5'].value or "").strip()
        ruc_raw = ws['C6'].value
        ruc = f"{int(ruc_raw)}" if isinstance(ruc_raw, float) else str(ruc_raw or "").replace(".0", "").strip()
        planta = str(ws['C7'].value or "").strip()
        responsable = str(ws['C8'].value or "").strip()
        liq_raw = ws['C9'].value
        liquidacion = f"{int(liq_raw):03d}" if isinstance(liq_raw, float) else str(liq_raw or "").replace(".0", "").strip()
        fecha = formatear_fecha_str(ws['C10'].value)
        fecha_doc = formatear_fecha_str(ws['H3'].value) # Celda H3 de metadatos LIQ-F-001

        datos = {
            "title": tab_name,
            "id": tab_name,
            "cliente": cliente,
            "ruc": ruc,
            "planta": planta,
            "responsable": responsable,
            "liquidacion": liquidacion,
            "fecha": fecha,
            "fecha_doc": fecha_doc,
            "items": []
        }

        # Leer filas de servicios (fila 13 en adelante)
        for r in range(13, ws.max_row + 1):
            c1_val = str(ws.cell(r, 1).value or "").strip().upper()
            if "TOTAL GENERADO" in c1_val or "SUB TOTAL" in c1_val or "* METODO" in c1_val or "FORMA DE PAGO" in c1_val:
                break

            desc = str(ws.cell(r, 2).value or "").strip()
            if not desc and ws.cell(r, 1).value is None and ws.cell(r, 6).value is None:
                continue

            num_raw = ws.cell(r, 1).value
            if isinstance(num_raw, float):
                num_str = str(int(num_raw))
            else:
                num_str = str(num_raw or "").replace(".0", "").strip()

            fecha_item = formatear_fecha_str(ws.cell(r, 4).value)
            guia_item = str(ws.cell(r, 5).value or "-").strip()
            cant_item = limpiar_monto_numerico(ws.cell(r, 6).value) or 1.0
            costo_item = limpiar_monto_numerico(ws.cell(r, 7).value)
            total_item = limpiar_monto_numerico(ws.cell(r, 8).value) or (cant_item * costo_item)

            datos["items"].append({
                "num": num_str,
                "descripcion": desc,
                "fecha": fecha_item if fecha_item else "-",
                "guia": guia_item,
                "cantidad": cant_item,
                "costo": costo_item,
                "total": total_item
            })

        return datos
    except Exception as e:
        logger.error(f"Error obteniendo detalle de pestaña {tab_name}: {e}", exc_info=True)
        return None

def _set_cell_safe(ws, row, col, val):
    c = ws.cell(row, col)
    if type(c).__name__ != 'MergedCell':
        c.value = val

def _aplicar_datos_en_hoja(target_ws, datos, cliente):
    """
    Aplica cabeceras, items con fórmulas, totales dinámicos, firmas y reposiciona la imagen
    del sello oficial. Descombina rangos inferiores y replica exactamente el modelo de márgenes,
    anchos de columna y alturas de fila del formato oficial (modelo SER 21).
    """
    ruc = str(datos.get("ruc", "")).strip()
    planta = str(datos.get("planta", "")).strip()
    responsable = str(datos.get("responsable", "")).strip()
    liquidacion = str(datos.get("liquidacion", "")).strip()
    fecha_emision = str(datos.get("fecha", "")).strip() or datetime.datetime.now(PET).strftime("%d/%m/%Y")
    fecha_doc = str(datos.get("fecha_doc", "")).strip()
    items = datos.get("items", [])

    # 1. Ajuste exacto de anchos de columna (según modelo oficial SER 21)
    col_widths = {
        'A': 8.14,
        'B': 14.57,
        'C': 38.86,
        'D': 14.57,
        'E': 13.0,
        'F': 13.0,
        'G': 13.0,
        'H': 13.0,
        'I': 11.43
    }
    for col_letter, width in col_widths.items():
        target_ws.column_dimensions[col_letter].width = width

    # 2. Configuración de página y márgenes exactos de impresión (modelo SER 21)
    target_ws.page_margins.left = 0.24
    target_ws.page_margins.right = 0.12
    target_ws.page_margins.top = 0.3937007874015748
    target_ws.page_margins.bottom = 0.7480314960629921
    target_ws.page_setup.orientation = target_ws.ORIENTATION_PORTRAIT
    target_ws.page_setup.paperSize = target_ws.PAPERSIZE_A4

    # 3. Cabecera Ficha LIQ-F-001
    _set_cell_safe(target_ws, 5, 3, cliente)
    _set_cell_safe(target_ws, 6, 3, float(ruc) if ruc.isdigit() else ruc)
    _set_cell_safe(target_ws, 7, 3, planta)
    _set_cell_safe(target_ws, 8, 3, responsable)
    _set_cell_safe(target_ws, 9, 3, liquidacion)
    _set_cell_safe(target_ws, 10, 3, fecha_emision)
    _set_cell_safe(target_ws, 3, 8, fecha_doc if fecha_doc else None)

    # 4. Descombinar todas las celdas previas desde la fila 13 hacia abajo
    ranges_to_unmerge = [m for m in list(target_ws.merged_cells.ranges) if m.min_row >= 13]
    for m in ranges_to_unmerge:
        target_ws.unmerge_cells(str(m))

    # 5. Limpiar celdas previas desde la fila 13 hasta la fila 65
    for r in range(13, 66):
        for col in range(1, 9):
            cell = target_ws.cell(r, col)
            cell.value = None
            cell.border = Border()
            cell.fill = PatternFill(fill_type=None)

    # Estilos estándar LIQ-F-001
    thin_side = Side(border_style='thin', color='000000')
    cell_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)
    font_normal = Font(name='Calibri', size=10, bold=False)
    font_bold = Font(name='Calibri', size=10, bold=True)
    font_note = Font(name='Calibri', size=11, bold=False)
    currency_fmt = r'_ "S/."* #,##0.00_ ;_ "S/."* \-#,##0.00_ ;_ "S/."* "-"??_ ;_ @_ '
    yellow_fill = PatternFill(start_color="FFFFFF00", end_color="FFFFFF00", fill_type="solid")
    gray_fill = PatternFill(start_color="FFF2F2F2", end_color="FFF2F2F2", fill_type="solid")

    align_center_wrap = Alignment(horizontal='center', vertical='center', wrap_text=True)
    align_center = Alignment(horizontal='center', vertical='center', wrap_text=False)
    align_left = Alignment(horizontal='left', vertical='center', wrap_text=False)

    # 6. Escribir items con altura exacta (43.5 pt) y ajuste de texto (wrap_text=True)
    fila_inicio = 13
    n_items = len(items) if items else 1

    for i, it in enumerate(items):
        r = fila_inicio + i
        target_ws.row_dimensions[r].height = 43.5  # Altura exacta modelo SER 21

        num_v = str(it.get("num", "")).strip()
        desc_v = str(it.get("descripcion", "")).strip()
        fec_v = str(it.get("fecha", "-")).strip()
        guia_v = str(it.get("guia", "-")).strip()
        cant_v = limpiar_monto_numerico(it.get("cantidad", 1))
        costo_v = limpiar_monto_numerico(it.get("costo", 0))

        # Col 1: N°
        c1 = target_ws.cell(r, 1)
        c1.value = float(num_v) if num_v.isdigit() else (num_v if num_v else None)
        c1.font = font_bold if num_v else font_normal
        c1.alignment = align_center

        # Col 2: Descripción (B y C combinados, wrap_text=True para no cortar texto)
        c2 = target_ws.cell(r, 2)
        c2.value = desc_v
        c2.font = font_normal
        c2.alignment = align_center_wrap

        target_ws.cell(r, 3).alignment = align_center_wrap

        # Col 4: Fecha
        c4 = target_ws.cell(r, 4)
        c4.value = fec_v
        c4.font = font_normal
        c4.alignment = align_center_wrap

        # Col 5: Guía
        c5 = target_ws.cell(r, 5)
        c5.value = guia_v
        c5.font = font_bold
        c5.alignment = align_center_wrap

        # Col 6: Cantidad
        c6 = target_ws.cell(r, 6)
        c6.value = cant_v
        c6.font = font_normal
        c6.alignment = align_center
        c6.number_format = '0.000'

        # Col 7: Costo
        c7 = target_ws.cell(r, 7)
        c7.value = costo_v
        c7.font = font_normal
        c7.alignment = align_center
        c7.number_format = currency_fmt

        # Col 8: Total
        c8 = target_ws.cell(r, 8)
        c8.value = f"=F{r}*G{r}"
        c8.font = font_normal
        c8.alignment = align_center
        c8.number_format = currency_fmt

        # Bordes para cols 1..8
        for c in range(1, 9):
            target_ws.cell(r, c).border = cell_border

        # Combinar B{r}:C{r} para la descripción del residuo
        target_ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)

    # Combinar celdas de Col A para números de servicio con subfilas (ej: Recolección y Disposición)
    service_groups = []
    curr_group = []
    for i, it in enumerate(items):
        r = fila_inicio + i
        num_v = str(it.get("num", "")).strip()
        if num_v:
            if curr_group:
                service_groups.append(curr_group)
            curr_group = [r]
        else:
            if curr_group:
                curr_group.append(r)
            else:
                curr_group = [r]
    if curr_group:
        service_groups.append(curr_group)

    for grp in service_groups:
        if len(grp) > 1:
            target_ws.merge_cells(start_row=grp[0], start_column=1, end_row=grp[-1], end_column=1)

    # 7. Totales con alturas exactas modelo SER 21
    ultima_fila_items = fila_inicio + n_items - 1
    fila_total_mes = ultima_fila_items + 1
    fila_vacia = fila_total_mes + 1
    fila_subtotal = fila_vacia + 1
    fila_igv = fila_subtotal + 1
    fila_total = fila_igv + 1
    fila_detraccion = fila_total + 1

    # TOTAL GENERADO EN EL MESES (A-G combinados)
    target_ws.row_dimensions[fila_total_mes].height = 15.0
    c_tot_lbl = target_ws.cell(fila_total_mes, 1)
    c_tot_lbl.value = "TOTAL GENERADO EN EL MESES"
    c_tot_lbl.font = font_bold
    c_tot_lbl.alignment = align_center

    c_tot_val = target_ws.cell(fila_total_mes, 8)
    c_tot_val.value = f"=SUM(H{fila_inicio}:H{ultima_fila_items})"
    c_tot_val.font = font_bold
    c_tot_val.alignment = align_center
    c_tot_val.number_format = currency_fmt

    for c in range(1, 9):
        target_ws.cell(fila_total_mes, c).border = cell_border
        target_ws.cell(fila_total_mes, c).fill = gray_fill

    target_ws.merge_cells(start_row=fila_total_mes, start_column=1, end_row=fila_total_mes, end_column=7)

    # Fila vacía separadora
    target_ws.row_dimensions[fila_vacia].height = 14.25
    target_ws.merge_cells(start_row=fila_vacia, start_column=1, end_row=fila_vacia, end_column=8)

    # SUB TOTAL
    target_ws.row_dimensions[fila_subtotal].height = 14.25
    c_sub_lbl = target_ws.cell(fila_subtotal, 1)
    c_sub_lbl.value = "* Metodo de pago: transferencia."
    c_sub_lbl.font = font_note
    c_sub_lbl.alignment = align_left
    target_ws.merge_cells(start_row=fila_subtotal, start_column=1, end_row=fila_subtotal, end_column=6)

    c_sub_tag = target_ws.cell(fila_subtotal, 7)
    c_sub_tag.value = "SUB TOTAL"
    c_sub_tag.font = font_bold
    c_sub_tag.alignment = align_center
    c_sub_tag.border = cell_border

    c_sub_val = target_ws.cell(fila_subtotal, 8)
    c_sub_val.value = f"=H{fila_total_mes}"
    c_sub_val.font = font_bold
    c_sub_val.alignment = align_center
    c_sub_val.number_format = currency_fmt
    c_sub_val.border = cell_border

    # IGV
    target_ws.row_dimensions[fila_igv].height = 14.25
    c_igv_lbl = target_ws.cell(fila_igv, 1)
    c_igv_lbl.value = "* Forma de pago: Al contado."
    c_igv_lbl.font = font_note
    c_igv_lbl.alignment = align_left
    target_ws.merge_cells(start_row=fila_igv, start_column=1, end_row=fila_igv, end_column=6)

    c_igv_tag = target_ws.cell(fila_igv, 7)
    c_igv_tag.value = "IGV"
    c_igv_tag.font = font_bold
    c_igv_tag.alignment = align_center
    c_igv_tag.border = cell_border

    c_igv_val = target_ws.cell(fila_igv, 8)
    c_igv_val.value = f"=H{fila_total}-H{fila_subtotal}"
    c_igv_val.font = font_bold
    c_igv_val.alignment = align_center
    c_igv_val.number_format = currency_fmt
    c_igv_val.border = cell_border

    # TOTAL (Resaltado en amarillo)
    target_ws.row_dimensions[fila_total].height = 14.25
    c_tot_lbl2 = target_ws.cell(fila_total, 1)
    c_tot_lbl2.value = "* Numero de cuenta Corriente soles en Interbank es: 200-3005602805. "
    c_tot_lbl2.font = font_note
    c_tot_lbl2.alignment = align_left
    target_ws.merge_cells(start_row=fila_total, start_column=1, end_row=fila_total, end_column=6)

    c_total_tag = target_ws.cell(fila_total, 7)
    c_total_tag.value = "TOTAL"
    c_total_tag.font = font_bold
    c_total_tag.alignment = align_center
    c_total_tag.border = cell_border
    c_total_tag.fill = yellow_fill

    c_total_val = target_ws.cell(fila_total, 8)
    c_total_val.value = f"=H{fila_subtotal}*1.18"
    c_total_val.font = font_bold
    c_total_val.alignment = align_center
    c_total_val.number_format = currency_fmt
    c_total_val.border = cell_border
    c_total_val.fill = yellow_fill

    # DETRACCION
    target_ws.row_dimensions[fila_detraccion].height = 14.25
    c_det_lbl = target_ws.cell(fila_detraccion, 1)
    c_det_lbl.value = "* Numero de cuenta de detracción: 00050086984"
    c_det_lbl.font = font_note
    c_det_lbl.alignment = align_left

    # 8. Firmas al Pie
    fila_firmas = fila_detraccion + 8
    fila_firmas_nom = fila_firmas + 1

    for r_space in range(fila_detraccion + 1, fila_firmas):
        target_ws.row_dimensions[r_space].height = 14.25

    target_ws.row_dimensions[fila_firmas].height = 14.25
    c_sig_epmi = target_ws.cell(fila_firmas, 3)
    c_sig_epmi.value = "______________________________________"
    c_sig_epmi.alignment = align_center

    c_sig_cli = target_ws.cell(fila_firmas, 5)
    c_sig_cli.value = "_______________________________________"
    c_sig_cli.alignment = align_center
    target_ws.merge_cells(start_row=fila_firmas, start_column=5, end_row=fila_firmas, end_column=7)

    target_ws.row_dimensions[fila_firmas_nom].height = 14.25
    c_nom_epmi = target_ws.cell(fila_firmas_nom, 3)
    c_nom_epmi.value = "EPMI S.A.C."
    c_nom_epmi.font = font_normal
    c_nom_epmi.alignment = align_center

    c_nom_cli = target_ws.cell(fila_firmas_nom, 5)
    c_nom_cli.value = cliente
    c_nom_cli.font = font_normal
    c_nom_cli.alignment = align_center
    target_ws.merge_cells(start_row=fila_firmas_nom, start_column=5, end_row=fila_firmas_nom, end_column=7)

    for r_ext in range(fila_firmas_nom + 1, fila_firmas_nom + 6):
        target_ws.row_dimensions[r_ext].height = 14.25

    # 9. Reposición Dinámica del Sello (Imagen de Firma Oficial)
    try:
        if hasattr(target_ws, '_images') and target_ws._images:
            for img in target_ws._images:
                # El logo está en fila 0, la firma está abajo (fila >= 10)
                if hasattr(img, 'anchor') and hasattr(img.anchor, '_from'):
                    if img.anchor._from.row >= 10:
                        img.anchor._from.row = fila_firmas - 7
                        img.anchor._from.col = 2
    except Exception as e_img:
        logger.warning(f"No se pudo reposicionar imagen de firma: {e_img}")

def guardar_valorizacion(datos):
    """Crea, hereda o modifica una pestaña en el archivo de Google Drive."""
    try:
        creds = obtener_credenciales()
        drive = build('drive', 'v3', credentials=creds)
        wb = descargar_workbook_drive(drive, data_only=False)

        modo = datos.get("modo", "heredar")
        fuente_tab = str(datos.get("fuente_tab", "")).strip()
        nombre_pestana = str(datos.get("nombre_pestana", "")).strip()
        cliente = str(datos.get("cliente", "")).strip().upper()
        liquidacion = str(datos.get("liquidacion", "")).strip()

        if not nombre_pestana:
            sigla = cliente.replace("CORPORACION ", "").replace("S.A.C.", "").replace("S.A.", "").strip()[:10]
            nombre_pestana = f"{sigla}(SER {liquidacion.lstrip('0') or '1'})"

        target_ws = None
        if modo == "modificar" and nombre_pestana in wb.sheetnames:
            target_ws = wb[nombre_pestana]
        else:
            if fuente_tab in wb.sheetnames:
                source_ws = wb[fuente_tab]
            else:
                source_ws = wb[wb.sheetnames[-1]]

            target_ws = wb.copy_worksheet(source_ws)
            final_title = nombre_pestana
            c = 1
            while final_title in wb.sheetnames:
                final_title = f"{nombre_pestana}_{c}"
                c += 1
            target_ws.title = final_title
            nombre_pestana = final_title

        _aplicar_datos_en_hoja(target_ws, datos, cliente)
        subir_workbook_drive(drive, wb)

        return {
            "success": True,
            "message": f"Pestaña '{nombre_pestana}' guardada con éxito en Google Sheets.",
            "sheet_name": nombre_pestana,
            "gid": nombre_pestana,
            "cliente": cliente,
            "liquidacion": liquidacion
        }
    except Exception as e:
        logger.error(f"Error guardando valorización: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def exportar_valorizacion_pdf(sheet_name, nombre_archivo=None, datos_pantalla=None):
    """
    Exporta la pestaña a PDF. Si se envían 'datos_pantalla', aplica los cambios
    en memoria para que el PDF refleje los datos actuales de la pantalla
    incluso si no se guardaron previamente en la hoja original.
    """
    try:
        creds = obtener_credenciales()
        if (creds.expired or not creds.valid) and creds.refresh_token:
            creds.refresh(Request())

        drive = build('drive', 'v3', credentials=creds)

        # 1. Descargar el archivo Excel actual a memoria
        request = drive.files().get_media(fileId=SHEET_VALORIZACIONES_ID)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)

        # 2. Abrir en memoria para renderizado
        wb = openpyxl.load_workbook(fh, data_only=False)

        target_ws = None
        if sheet_name in wb.sheetnames:
            target_ws = wb[sheet_name]
        else:
            # Buscar coincidencia o clonar
            matched = [s for s in wb.sheetnames if str(sheet_name).upper() in s.upper()]
            if matched:
                sheet_name = matched[0]
                target_ws = wb[sheet_name]
            else:
                target_ws = wb.copy_worksheet(wb[wb.sheetnames[-1]])
                target_ws.title = str(sheet_name)

        # Si el usuario modificó datos en pantalla (o agregó items) sin guardar en el Sheet,
        # los aplicamos en memoria para que el PDF salga con toda su información:
        if datos_pantalla:
            c_nom = str(datos_pantalla.get("cliente") or target_ws['C5'].value or "").strip().upper()
            _aplicar_datos_en_hoja(target_ws, datos_pantalla, c_nom)

        # Guardar el workbook modificado en memoria
        render_fh = io.BytesIO()
        wb.save(render_fh)
        render_fh.seek(0)

        # 3. Subir como Google Sheet temporal
        media = MediaIoBaseUpload(
            render_fh,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        temp_body = {
            'name': f'TMP_CONVERT_{int(time.time())}',
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [DRIVE_FOLDER_VALORIZACIONES_PDF]
        }
        temp_file = drive.files().create(body=temp_body, media_body=media, fields='id').execute()
        temp_id = temp_file['id']

        # 4. Obtener GID de la hoja
        import gspread
        import requests
        gc = gspread.authorize(creds)
        temp_doc = gc.open_by_key(temp_id)

        try:
            ws = temp_doc.worksheet(str(sheet_name))
        except gspread.exceptions.WorksheetNotFound:
            ws = temp_doc.worksheets()[-1]

        target_gid = ws.id

        # 5. Descargar PDF renderizado
        url_export = (
            f"https://docs.google.com/spreadsheets/d/{temp_id}/export"
            f"?format=pdf&gid={target_gid}&size=A4&portrait=true&fitw=true&gridlines=false"
            f"&top_margin=0.39&bottom_margin=0.75&left_margin=0.24&right_margin=0.12"
        )
        headers = {'Authorization': f'Bearer {creds.token}'}
        resp = requests.get(url_export, headers=headers, timeout=40)

        # 6. Eliminar archivo temporal de Google Drive
        try:
            drive.files().delete(fileId=temp_id).execute()
        except Exception:
            pass

        if resp.status_code != 200:
            raise ValueError(f"Error descargando PDF: status {resp.status_code}")

        pdf_bytes = resp.content

        # Nombre oficial solicitado: VALORIZACION DE SERVICIOS - EPMI - {nombre_pestana}.pdf
        if not nombre_archivo:
            nombre_archivo = formatear_nombre_pdf(sheet_name)
        elif not nombre_archivo.lower().endswith(".pdf"):
            nombre_archivo += ".pdf"

        # 7. Subir PDF final a la carpeta de PDFs en Drive
        meta_pdf = {
            'name': nombre_archivo,
            'parents': [DRIVE_FOLDER_VALORIZACIONES_PDF],
            'mimeType': 'application/pdf'
        }
        media_pdf = MediaInMemoryUpload(pdf_bytes, mimetype='application/pdf', resumable=True)
        final_pdf = drive.files().create(
            body=meta_pdf,
            media_body=media_pdf,
            fields='id, name, webViewLink'
        ).execute()

        drive_link = final_pdf.get('webViewLink', '')
        file_id = final_pdf.get('id', '')
        logger.info(f"✅ PDF de Valorización guardado en Drive: {nombre_archivo} ({drive_link})")

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
