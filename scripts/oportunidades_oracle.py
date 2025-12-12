# ============================================================================
# 🔄 BLOQUE MERGE: ACTUALIZACIÓN INCREMENTAL (ÚLTIMO DÍA)
# ============================================================================

import os
import json
import time
import math
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.types import CLOB, Integer, String, Float, Numeric
from colorama import Fore, Style, init

init(autoreset=True)

# --- CONFIGURACIÓN DINÁMICA ---
# Calculamos la fecha de hace 1 día automáticamente
DIAS_ATRAS = 1
fecha_corte = datetime.now(timezone.utc) - timedelta(days=DIAS_ATRAS)
FECHA_FILTRO = fecha_corte.isoformat()

TABLE_ID = "OPORTUNIDADES_REAL"

# CREDENCIALES DESDE VARIABLES DE ENTORNO (GitHub Secrets)
API_TOKEN = os.environ.get('CLIENTIFY_API_TOKEN')
ORACLE_USER = os.environ.get('ORACLE_USER')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')
ORACLE_DSN = os.environ.get('ORACLE_DSN')

# Validación de credenciales
if not all([API_TOKEN, ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN]):
    raise ValueError("❌ Faltan variables de entorno. Verifica los Secrets en GitHub.")

HEADERS = { "Authorization": f"Token {API_TOKEN}", "Content-Type": "application/json" }
URL_OPORTUNIDADES = "https://api.clientify.net/v1/deals/"
engine_oracle = create_engine(f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}")

# --- MAPA DE COLUMNAS ---
MAPA_COLUMNAS_TIPOS = {
    "id": Integer(), "probability": Integer(), "status": Integer(), "who_can_view": Integer(),
    "amount": Numeric(15, 2), "created": String(64), "modified": String(64),
    "expected_closed_date": String(64), "actual_closed_date": String(64),
    "currency": String(50), "contact": String(255), "contact_name": String(255),
    "contact_email": String(255), "contact_phone": String(255), "contact_source": String(255),
    "contact_medium": String(255), "owner": String(255), "owner_name": String(255),
    "owner_picture": String(500), "company": String(255), "name": String(500),
    "source": String(255), "deal_source": String(255), "lost_reason": String(500),
    "remarks": String(4000), "url": String(500), "pipeline": String(255),
    "pipeline_desc": String(255), "pipeline_stage": String(255),
    "pipeline_stage_desc": String(255), "status_desc": String(255),
    "probability_desc": String(50), "amount_user": String(255),
    "custom_fields": CLOB(), "tags": CLOB(), "products": CLOB(), "events": CLOB(),
    "tasks": CLOB(), "integrations": CLOB(), "involved_companies": CLOB(),
    "involved_contacts": CLOB(), "stages_duration": CLOB(), "wall_entries": CLOB()
}

# ============================================================================
# 🧠 FUNCIONES
# ============================================================================

def request_blindado(url):
    while True:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200: return r.json()
            elif r.status_code == 429:
                time.sleep(5)
                continue
            elif r.status_code >= 500:
                time.sleep(5)
                continue
            else: return None
        except: time.sleep(5)

def obtener_estimacion():
    print(f"{Fore.CYAN}ℹ️  Buscando cambios desde: {FECHA_FILTRO}...")
    url = f"{URL_OPORTUNIDADES}?modified[gte]={FECHA_FILTRO}&page=1"
    data = request_blindado(url)
    if data:
        total = data.get("count", 0)
        results = data.get("results", [])
        page_size = len(results) if len(results) > 0 else 50
        return total, page_size
    return 0, 50

def obtener_datos_secuencial(total_paginas):
    items_acumulados = []
    print(f"\n{Fore.WHITE}📥 Descargando {total_paginas} páginas de cambios...")
    pbar = tqdm(range(1, total_paginas + 1), desc="Páginas", unit="pag", colour='cyan')
    for page in pbar:
        url = f"{URL_OPORTUNIDADES}?modified[gte]={FECHA_FILTRO}&page={page}"
        data = request_blindado(url)
        if data:
            items_acumulados.extend(data.get("results", []))
        time.sleep(0.2)
    return items_acumulados

def obtener_detalles(lista_items):
    detalles_fin = []
    print(f"\n{Fore.WHITE}🔍 Actualizando detalles profundos...")
    pbar = tqdm(lista_items, desc="Detalles", unit="deal", colour='green')
    for item in pbar:
        url = f"{URL_OPORTUNIDADES}{item['id']}/"
        detalle = request_blindado(url)
        if detalle: detalles_fin.append(detalle)
        time.sleep(0.1)
    return detalles_fin

def procesar_datos(lista_datos):
    print(f"\n{Fore.CYAN}⚙️  Procesando datos para MERGE...")
    df = pd.DataFrame(lista_datos)
    if df.empty: return df

    cols_db = list(MAPA_COLUMNAS_TIPOS.keys())
    for col in cols_db:
        if col not in df.columns: df[col] = None
    df = df[cols_db]

    def _limpiar(val):
        if val is None: return None
        if isinstance(val, (list, dict, np.ndarray)):
            try: return json.dumps(val, ensure_ascii=False)
            except: return "[]"
        if pd.isna(val): return None
        return str(val)

    for col, tipo in MAPA_COLUMNAS_TIPOS.items():
        if isinstance(tipo, (String, CLOB)):
            df[col] = df[col].apply(_limpiar)
        elif isinstance(tipo, (Integer, Numeric, Float)):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df.columns = [c.upper() for c in df.columns]

    # --- DEDUPLICACIÓN (IMPORTANTE PARA MERGE) ---
    # Para el MERGE, el ID debe ser único en el paquete que subimos.
    # Si un ID aparece dos veces el último Día, nos quedamos con el más reciente.
    df = df.drop_duplicates(subset=['ID'], keep='last')

    return df

def ejecutar_merge_oracle(df, engine, table_name):
    if df.empty: return

    temp_table = f"{table_name}_TEMP"
    print(f"\n{Fore.YELLOW}🔄 EJECUTANDO MERGE (UPSERT) EN ORACLE...")
    print(f"   » Registros a procesar: {len(df)}")

    dtype = {k.upper(): v for k, v in MAPA_COLUMNAS_TIPOS.items()}

    try:
        with engine.connect() as conn:
            # 1. Crear/Limpiar Tabla Temporal
            try: conn.execute(text(f'DROP TABLE "{temp_table}"')); conn.commit()
            except: pass

            # 2. Subir datos a Temporal
            print(f"{Fore.CYAN}   » Subiendo a tabla temporal...")
            df.to_sql(temp_table, con=engine, if_exists='replace', index=False, dtype=dtype)

            # 3. Construir Query MERGE Dinámico
            cols = df.columns.tolist()
            # Clause SET (Para Updates): Actualiza todo menos el ID
            set_clause = ", ".join([f'T."{c}"=S."{c}"' for c in cols if c != 'ID'])
            # Clause INSERT (Para Nuevos): Inserta todo
            ins_cols = ", ".join([f'"{c}"' for c in cols])
            ins_vals = ", ".join([f'S."{c}"' for c in cols])

            sql_merge = f"""
            MERGE INTO "{table_name}" T
            USING "{temp_table}" S
            ON (T."ID" = S."ID")
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({ins_cols}) VALUES ({ins_vals})
            """

            # 4. Ejecutar MERGE
            print(f"{Fore.MAGENTA}   » Cruzando datos (Update/Insert)...")
            conn.execute(text(sql_merge))
            conn.commit()

            # 5. Limpieza
            conn.execute(text(f'DROP TABLE "{temp_table}"'))
            conn.commit()

        print(f"{Fore.GREEN}✅ ¡SINCRONIZACIÓN EXITOSA! Datos actualizados.")

    except Exception as e:
        print(f"{Fore.RED}❌ Error en el Merge: {e}")
        raise

# ============================================================================
# 🚀 EJECUCIÓN
# ============================================================================

def main():
    inicio = time.time()
    print(f"{Fore.MAGENTA}{Style.BRIGHT}🚀 INICIO PROCESO INCREMENTAL (MERGE 3 DÍAS)")

    # 1. Estimación
    total, page_size = obtener_estimacion()
    if total == 0:
        print(f"{Fore.GREEN}✅ No hay cambios Todo al día.")
        return

    total_paginas = math.ceil(total / page_size)
    print(f"   » Cambios detectados: {total}")

    # 2. Descarga
    items = obtener_datos_secuencial(total_paginas)
    if not items: return

    # 3. Detalles
    detalles = obtener_detalles(items)

    # 4. Procesar y Merge
    try:
        df_final = procesar_datos(detalles)
        ejecutar_merge_oracle(df_final, engine_oracle, TABLE_ID)
    except Exception as e:
        print(f"Error crítico: {e}")
        raise

    mins, secs = divmod(time.time() - inicio, 60)
    print(f"\n{Fore.WHITE}⏱️ TIEMPO TOTAL: {int(mins)}m {int(secs)}s")

if __name__ == "__main__":
    main()
