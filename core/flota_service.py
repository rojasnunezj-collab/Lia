# ====================================================================
# --- SERVICIO DE FLOTA: GESTIÓN DE CAMIONES Y CONDUCTORES ---
# ====================================================================
import re
import os
import time
from typing import List, Dict, Any, Optional, Tuple
from config.settings import logger
from core.certificados_service import obtener_servicios_google
from core.sheets_client import sanitizar_formula_sheets

ID_SHEET_CONTROL = "14As5bCpZi56V5Nq1DRs0xl6R1LuOXLvRRoV26nI50NU"

def _get_sheets():
    _, s = obtener_servicios_google()
    return s

def normalizar_placa(placa: Any) -> str:
    """Normaliza una placa vehicular al formato estándar ABC-123 o XY-1234."""
    if not placa:
        return ""
    s = str(placa).strip().upper().replace(" ", "").replace("-", "")
    if len(s) == 6 and re.match(r'^[A-Z0-9]{3}[A-Z0-9]{3}$', s):
        return f"{s[:3]}-{s[3:]}"
    elif len(s) == 5 and re.match(r'^[A-Z]{2}\d{4}$', s):
        return f"{s[:2]}-{s[2:]}"
    return str(placa).strip().upper()

def asegurar_pestanas_flota(sheets_service=None) -> None:
    """
    Verifica que existan las pestañas 'CAMIONES' y 'CONDUCTORES' en Google Sheets.
    Si no existen, las crea automáticamente con encabezados y registros de muestra.
    """
    if not sheets_service:
        sheets_service = _get_sheets()

    meta = sheets_service.spreadsheets().get(spreadsheetId=ID_SHEET_CONTROL).execute()
    titulos = {sh['properties']['title']: sh['properties']['sheetId'] for sh in meta.get('sheets', [])}

    requests = []
    crear_camiones = 'CAMIONES' not in titulos
    crear_conductores = 'CONDUCTORES' not in titulos

    if crear_camiones:
        requests.append({
            'addSheet': {
                'properties': {
                    'title': 'CAMIONES',
                    'gridProperties': {'rowCount': 200, 'columnCount': 12}
                }
            }
        })
    if crear_conductores:
        requests.append({
            'addSheet': {
                'properties': {
                    'title': 'CONDUCTORES',
                    'gridProperties': {'rowCount': 200, 'columnCount': 12}
                }
            }
        })

    if requests:
        res_batch = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=ID_SHEET_CONTROL,
            body={'requests': requests}
        ).execute()
        logger.info("Pestañas de flota creadas exitosamente.")

    # Cargar encabezados y registros de muestra si es nueva pestaña
    if crear_camiones:
        headers_camiones = [
            ['ID', 'Placa', 'Marca', 'Modelo', 'Tipo', 'Capacidad', 'Empresa', 'SOAT_Vence', 'Rev_Tecnica_Vence', 'Estado', 'Observaciones'],
            ['CAM-1', 'AZU-781', 'MITSUBISHI', 'CANTER', 'Furgón', '5 TM', 'EPMI S.A.C.', '31/12/2026', '30/11/2026', 'Activo', 'Operativo'],
            ['CAM-2', 'F8C-700', 'VOLVO', 'FMX', 'Baranda', '15 TM', 'EPMI S.A.C.', '15/10/2026', '20/12/2026', 'Activo', 'Operativo'],
            ['CAM-3', 'B5B-819', 'ISUZU', 'FORWARD', 'Furgón', '10 TM', 'INECOVE S.A.C.', '28/02/2027', '15/01/2027', 'Activo', 'Operativo'],
            ['CAM-4', 'F1C-832', 'HINO', '500', 'Plataforma', '12 TM', 'EPMI S.A.C.', '10/05/2027', '05/04/2027', 'Activo', 'Operativo'],
            ['CAM-5', 'Z3H-785', 'HYUNDAI', 'HD78', 'Cisterna', '8 TM', 'INECOVE S.A.C.', '20/11/2026', '12/10/2026', 'Activo', 'Residuos Peligrosos']
        ]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=ID_SHEET_CONTROL,
            range="'CAMIONES'!A1:K6",
            valueInputOption="USER_ENTERED",
            body={'values': headers_camiones}
        ).execute()

    if crear_conductores:
        headers_conductores = [
            ['ID', 'Nombre', 'DNI', 'Licencia', 'Categoria', 'Licencia_Vence', 'Telefono', 'Empresa', 'Estado', 'Observaciones'],
            ['CHO-1', 'Juan Carlos Pérez Gómez', '45892134', 'Q45892134', 'A-IIIc', '15/08/2027', '987654321', 'EPMI S.A.C.', 'Activo', 'Rutas Lima e Ica'],
            ['CHO-2', 'Carlos Alberto Mendoza Rivas', '42109876', 'Q42109876', 'A-IIIb', '20/11/2026', '991234567', 'EPMI S.A.C.', 'Activo', 'Conductor Titular'],
            ['CHO-3', 'Pedro Luis Quispe Huamán', '47654321', 'Q47654321', 'A-IIIc', '05/04/2028', '978112233', 'INECOVE S.A.C.', 'Activo', 'Rotativo']
        ]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=ID_SHEET_CONTROL,
            range="'CONDUCTORES'!A1:J4",
            valueInputOption="USER_ENTERED",
            body={'values': headers_conductores}
        ).execute()

# ====================================================================
# --- CRUD: CAMIONES ---
# ====================================================================
def listar_camiones(sheets_service=None) -> List[Dict[str, Any]]:
    """Obtiene la lista completa de camiones desde la pestaña 'CAMIONES'."""
    if not sheets_service:
        sheets_service = _get_sheets()
    asegurar_pestanas_flota(sheets_service)

    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL,
            range="'CAMIONES'!A2:K"
        ).execute()
        filas = res.get('values', [])
        lista = []
        for idx, row in enumerate(filas):
            if not row or not any(str(c).strip() for c in row):
                continue
            placa_raw = row[1] if len(row) > 1 else ""
            if not placa_raw or str(placa_raw).strip().upper() in ["PLACA", "S/D"]:
                continue

            num_fila = idx + 2
            cid = str(row[0]).strip() if len(row) > 0 and str(row[0]).strip() else f"CAM-{idx+1}"
            lista.append({
                "id": cid,
                "placa": normalizar_placa(placa_raw),
                "marca": str(row[2]).strip() if len(row) > 2 else "",
                "modelo": str(row[3]).strip() if len(row) > 3 else "",
                "tipo": str(row[4]).strip() if len(row) > 4 else "Furgón",
                "capacidad": str(row[5]).strip() if len(row) > 5 else "",
                "empresa": str(row[6]).strip() if len(row) > 6 else "EPMI S.A.C.",
                "soat_vence": str(row[7]).strip() if len(row) > 7 else "",
                "rev_tecnica_vence": str(row[8]).strip() if len(row) > 8 else "",
                "estado": str(row[9]).strip() if len(row) > 9 and str(row[9]).strip() else "Activo",
                "observaciones": str(row[10]).strip() if len(row) > 10 else "",
                "fila": num_fila
            })
        return lista
    except Exception as e:
        logger.error(f"Error listando camiones: {e}")
        return []

def obtener_camion(criterio: str, sheets_service=None) -> Optional[Dict[str, Any]]:
    """Busca un camión por Placa o por ID."""
    if not criterio:
        return None
    camiones = listar_camiones(sheets_service)
    crit_norm = normalizar_placa(criterio).replace("-", "").upper()
    crit_raw = str(criterio).strip().upper()

    for c in camiones:
        c_placa_norm = normalizar_placa(c['placa']).replace("-", "").upper()
        if c['id'].upper() == crit_raw or c_placa_norm == crit_norm or c['placa'].upper() == crit_raw:
            return c
    return None

def guardar_camion(datos: Dict[str, Any], sheets_service=None) -> Dict[str, Any]:
    """Crea o actualiza un camión en la pestaña 'CAMIONES'."""
    if not sheets_service:
        sheets_service = _get_sheets()
    asegurar_pestanas_flota(sheets_service)

    placa_limpia = normalizar_placa(datos.get('placa', ''))
    if not placa_limpia:
        raise ValueError("La placa del camión es obligatoria.")

    camiones = listar_camiones(sheets_service)
    camion_existente = None
    target_id = str(datos.get('id', '')).strip().upper()

    for c in camiones:
        if target_id and c['id'].upper() == target_id:
            camion_existente = c
            break
        if normalizar_placa(c['placa']) == placa_limpia:
            camion_existente = c
            break

    marca = str(datos.get('marca', '')).strip().upper()
    modelo = str(datos.get('modelo', '')).strip().upper()
    tipo = str(datos.get('tipo', 'Furgón')).strip()
    capacidad = str(datos.get('capacidad', '')).strip()
    empresa = str(datos.get('empresa', 'EPMI S.A.C.')).strip().upper()
    soat = str(datos.get('soat_vence', '')).strip()
    rev = str(datos.get('rev_tecnica_vence', '')).strip()
    estado = str(datos.get('estado', 'Activo')).strip()
    obs = str(datos.get('observaciones', '')).strip()

    if camion_existente:
        fila = camion_existente['fila']
        cid = camion_existente['id']
        row_vals = [sanitizar_formula_sheets(x) for x in [cid, placa_limpia, marca, modelo, tipo, capacidad, empresa, soat, rev, estado, obs]]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=ID_SHEET_CONTROL,
            range=f"'CAMIONES'!A{fila}:K{fila}",
            valueInputOption="USER_ENTERED",
            body={'values': [row_vals]}
        ).execute()
        logger.info(f"Camión {placa_limpia} (ID {cid}) actualizado en fila {fila}.")
    else:
        # Generar nuevo ID
        max_num = 0
        for c in camiones:
            m = re.search(r'\d+', c['id'])
            if m:
                max_num = max(max_num, int(m.group(0)))
        cid = f"CAM-{max_num + 1}"
        row_vals = [sanitizar_formula_sheets(x) for x in [cid, placa_limpia, marca, modelo, tipo, capacidad, empresa, soat, rev, estado, obs]]
        res = sheets_service.spreadsheets().values().append(
            spreadsheetId=ID_SHEET_CONTROL,
            range="'CAMIONES'!A:K",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={'values': [row_vals]}
        ).execute()
        logger.info(f"Nuevo camión {placa_limpia} registrado con ID {cid}.")

    return {
        "id": cid,
        "placa": placa_limpia,
        "marca": marca,
        "modelo": modelo,
        "tipo": tipo,
        "capacidad": capacidad,
        "empresa": empresa,
        "soat_vence": soat,
        "rev_tecnica_vence": rev,
        "estado": estado,
        "observaciones": obs
    }

def eliminar_camion(criterio: str, sheets_service=None) -> bool:
    """Elimina físicamente la fila del camión de la pestaña 'CAMIONES'."""
    if not sheets_service:
        sheets_service = _get_sheets()
    camion = obtener_camion(criterio, sheets_service)
    if not camion:
        return False

    meta = sheets_service.spreadsheets().get(spreadsheetId=ID_SHEET_CONTROL).execute()
    sheet_id = None
    for sh in meta.get('sheets', []):
        if sh['properties']['title'] == 'CAMIONES':
            sheet_id = sh['properties']['sheetId']
            break

    if sheet_id is None:
        return False

    fila_idx = camion['fila'] - 1  # 0-indexed para deleteDimension
    req = {
        'deleteDimension': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'ROWS',
                'startIndex': fila_idx,
                'endIndex': fila_idx + 1
            }
        }
    }
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=ID_SHEET_CONTROL,
        body={'requests': [req]}
    ).execute()
    logger.info(f"Camión {camion['placa']} eliminado de fila {camion['fila']}.")
    return True

# ====================================================================
# --- CRUD: CONDUCTORES ---
# ====================================================================
def listar_conductores(sheets_service=None) -> List[Dict[str, Any]]:
    """Obtiene la lista completa de conductores desde la pestaña 'CONDUCTORES'."""
    if not sheets_service:
        sheets_service = _get_sheets()
    asegurar_pestanas_flota(sheets_service)

    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=ID_SHEET_CONTROL,
            range="'CONDUCTORES'!A2:J"
        ).execute()
        filas = res.get('values', [])
        lista = []
        for idx, row in enumerate(filas):
            if not row or not any(str(c).strip() for c in row):
                continue
            nombre = row[1] if len(row) > 1 else ""
            if not nombre or str(nombre).strip().upper() in ["NOMBRE", "CONDUCTOR", "S/D"]:
                continue

            num_fila = idx + 2
            cid = str(row[0]).strip() if len(row) > 0 and str(row[0]).strip() else f"CHO-{idx+1}"
            lista.append({
                "id": cid,
                "nombre": str(nombre).strip().title(),
                "dni": str(row[2]).strip() if len(row) > 2 else "",
                "licencia": str(row[3]).strip().upper() if len(row) > 3 else "",
                "categoria": str(row[4]).strip().upper() if len(row) > 4 else "A-IIIc",
                "licencia_vence": str(row[5]).strip() if len(row) > 5 else "",
                "telefono": str(row[6]).strip() if len(row) > 6 else "",
                "empresa": str(row[7]).strip() if len(row) > 7 else "EPMI S.A.C.",
                "estado": str(row[8]).strip() if len(row) > 8 and str(row[8]).strip() else "Activo",
                "observaciones": str(row[9]).strip() if len(row) > 9 else "",
                "fila": num_fila
            })
        return lista
    except Exception as e:
        logger.error(f"Error listando conductores: {e}")
        return []

def obtener_conductor(criterio: str, sheets_service=None) -> Optional[Dict[str, Any]]:
    """Busca un conductor por DNI, N° Licencia, Nombre o ID."""
    if not criterio:
        return None
    conductores = listar_conductores(sheets_service)
    crit_clean = str(criterio).strip().upper()

    for c in conductores:
        if (c['id'].upper() == crit_clean or
            c['dni'].upper() == crit_clean or
            c['licencia'].upper() == crit_clean or
            crit_clean in c['nombre'].upper()):
            return c
    return None

def guardar_conductor(datos: Dict[str, Any], sheets_service=None) -> Dict[str, Any]:
    """Crea o actualiza un conductor en la pestaña 'CONDUCTORES'."""
    if not sheets_service:
        sheets_service = _get_sheets()
    asegurar_pestanas_flota(sheets_service)

    nombre = str(datos.get('nombre', '')).strip().title()
    if not nombre:
        raise ValueError("El nombre del conductor es obligatorio.")

    dni = str(datos.get('dni', '')).strip()
    licencia = str(datos.get('licencia', '')).strip().upper()
    categoria = str(datos.get('categoria', 'A-IIIc')).strip().upper()
    licencia_vence = str(datos.get('licencia_vence', '')).strip()
    telefono = str(datos.get('telefono', '')).strip()
    empresa = str(datos.get('empresa', 'EPMI S.A.C.')).strip().upper()
    estado = str(datos.get('estado', 'Activo')).strip()
    obs = str(datos.get('observaciones', '')).strip()

    conductores = listar_conductores(sheets_service)
    conductor_existente = None
    target_id = str(datos.get('id', '')).strip().upper()

    for c in conductores:
        if target_id and c['id'].upper() == target_id:
            conductor_existente = c
            break
        if dni and c['dni'] == dni:
            conductor_existente = c
            break
        if licencia and c['licencia'] == licencia:
            conductor_existente = c
            break

    if conductor_existente:
        fila = conductor_existente['fila']
        cid = conductor_existente['id']
        row_vals = [sanitizar_formula_sheets(x) for x in [cid, nombre, dni, licencia, categoria, licencia_vence, telefono, empresa, estado, obs]]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=ID_SHEET_CONTROL,
            range=f"'CONDUCTORES'!A{fila}:J{fila}",
            valueInputOption="USER_ENTERED",
            body={'values': [row_vals]}
        ).execute()
        logger.info(f"Conductor {nombre} (ID {cid}) actualizado en fila {fila}.")
    else:
        max_num = 0
        for c in conductores:
            m = re.search(r'\d+', c['id'])
            if m:
                max_num = max(max_num, int(m.group(0)))
        cid = f"CHO-{max_num + 1}"
        row_vals = [sanitizar_formula_sheets(x) for x in [cid, nombre, dni, licencia, categoria, licencia_vence, telefono, empresa, estado, obs]]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=ID_SHEET_CONTROL,
            range="'CONDUCTORES'!A:J",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={'values': [row_vals]}
        ).execute()
        logger.info(f"Nuevo conductor {nombre} registrado con ID {cid}.")

    return {
        "id": cid,
        "nombre": nombre,
        "dni": dni,
        "licencia": licencia,
        "categoria": categoria,
        "licencia_vence": licencia_vence,
        "telefono": telefono,
        "empresa": empresa,
        "estado": estado,
        "observaciones": obs
    }

def eliminar_conductor(criterio: str, sheets_service=None) -> bool:
    """Elimina físicamente la fila del conductor de la pestaña 'CONDUCTORES'."""
    if not sheets_service:
        sheets_service = _get_sheets()
    conductor = obtener_conductor(criterio, sheets_service)
    if not conductor:
        return False

    meta = sheets_service.spreadsheets().get(spreadsheetId=ID_SHEET_CONTROL).execute()
    sheet_id = None
    for sh in meta.get('sheets', []):
        if sh['properties']['title'] == 'CONDUCTORES':
            sheet_id = sh['properties']['sheetId']
            break

    if sheet_id is None:
        return False

    fila_idx = conductor['fila'] - 1
    req = {
        'deleteDimension': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'ROWS',
                'startIndex': fila_idx,
                'endIndex': fila_idx + 1
            }
        }
    }
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=ID_SHEET_CONTROL,
        body={'requests': [req]}
    ).execute()
    logger.info(f"Conductor {conductor['nombre']} eliminado de fila {conductor['fila']}.")
    return True

# ====================================================================
# --- URL DE LA MINI APP WEBAPP ---
# ====================================================================
def obtener_url_webapp_flota(tab: str = "camiones", user_id: Optional[Any] = None) -> str:
    """Construye la URL segura HTTPS para la Mini App de Flota (Camiones & Choferes)."""
    import urllib.parse
    from config.settings import RENDER_EXTERNAL_URL, WEBAPP_FLOTA_URL

    base_url = WEBAPP_FLOTA_URL
    if not base_url and RENDER_EXTERNAL_URL:
        base_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/flota"
    if not base_url:
        base_url = "https://lia-w2yb.onrender.com/flota"

    params = {'tab': tab, 'v': str(int(time.time()))}
    if user_id:
        params['user_id'] = str(user_id)

    return f"{base_url}?{urllib.parse.urlencode(params)}"
