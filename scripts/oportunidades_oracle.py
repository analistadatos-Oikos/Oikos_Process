# ============================================================================
# 🔄 BLOQUE MERGE: OPTIMIZADO CON PARALELIZACIÓN (8 WORKERS) - OUTPUT LIMPIO
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

init(autoreset=True)

# --- CONFIGURACIÓN DINÁMICA ---
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

# Session global para reutilizar conexiones HTTP
session = requests.Session()
session.headers.update({"Authorization": f"Token {API_TOKEN}", "Content-Type": "application/json"})

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
# 🧠 FUNCIONES OPTIMIZADAS
# ============================================================================

def request_blindado(url):
    """Request con reintentos y timeout reducido"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(2)
            elif r.status_code >= 500:
                time.sleep(2)
            else:
                return None
        except:
            if attempt == max_retries - 1:
                return None
            time.sleep(1)
    return None

def obtener_estimacion():
    """Calcula cuántos registros hay que procesar"""
    url = f"{URL_OPORTUNIDADES}?modified[gte]={FECHA_FILTRO}&page=1"
    data = request_blindado(url)
    if data:
        total = data.get("count", 0)
        results = data.get("results", [])
        page_size = len(results) if len(results) > 0 else 50
        return total, page_size
    return 0, 50

def obtener_datos_secuencial(total_paginas):
    """Descarga todas las páginas de listado (IDs)"""
    items_acumulados = []
    
    # Barra con file=sys.stdout y bar_format personalizado
    pbar = tqdm(
        total=total_paginas,
        desc="📥 Descargando páginas",
        unit="pag",
        bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
        file=sys.stdout,
        dynamic_ncols=True
    )
    
    for page in range(1, total_paginas + 1):
        url = f"{URL_OPORTUNIDADES}?modified[gte]={FECHA_FILTRO}&page={page}"
        data = request_blindado(url)
        if data:
            items_acumulados.extend(data.get("results", []))
        time.sleep(0.05)
        pbar.update(1)
    
    pbar.close()
    return items_acumulados

def obtener_detalle_paralelo(item_id):
    """Obtiene el detalle completo de una oportunidad (para paralelizar)"""
    url = f"{URL_OPORTUNIDADES}{item_id}/"
    return request_blindado(url)

def obtener_detalles(lista_items):
    """Obtiene detalles en paralelo con 8 workers"""
    detalles_fin = []
    total_items = len(lista_items)
    
    # Barra con configuración para evitar múltiples líneas
    pbar = tqdm(
        total=total_items,
        desc="🔍 Obteniendo detalles",
        unit="deal",
        bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
        file=sys.stdout,
        dynamic_ncols=True
    )
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Enviar todas las tareas
        futures = {executor.submit(obtener_detalle_paralelo, item['id']): item 
                  for item in lista_items}
        
        # Procesar conforme van terminando
        for future in as_completed(futures):
            detalle = future.result()
            if detalle:
                detalles_fin.append(detalle)
            pbar.update(1)
    
    pbar.close()
    return detalles_fin

def procesar_datos(lista_datos):
    """Procesa y limpia los datos para Oracle"""
    df = pd.DataFrame(lista_datos)
    if df.empty:
        return df

    # Asegurar que existan todas las columnas
    cols_db = list(MAPA_COLUMNAS_TIPOS.keys())
    for col in cols_db:
        if col not in df.columns:
            df[col] = None
    df = df[cols_db]

    # Función de limpieza
    def _limpiar(val):
        if val is None or pd.isna(val):
            return None
        if isinstance(val, (list, dict, np.ndarray)):
            try:
                return json.dumps(val, ensure_ascii=False)
            except:
                return "[]"
        return str(val)

    # Aplicar tipos de datos
    for col, tipo in MAPA_COLUMNAS_TIPOS.items():
        if isinstance(tipo, (String, CLOB)):
            df[col] = df[col].apply(_limpiar)
        elif isinstance(tipo, (Integer, Numeric, Float)):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Columnas en mayúsculas
    df.columns = [c.upper() for c in df.columns]

    # Deduplicar por ID (quedarse con el más reciente)
    df = df.drop_duplicates(subset=['ID'], keep='last')

    return df

def ejecutar_merge_oracle(df, engine, table_name):
    """Ejecuta MERGE (UPSERT) en Oracle"""
    if df.empty:
        return

    temp_table = f"{table_name}_TEMP"
    dtype = {k.upper(): v for k, v in MAPA_COLUMNAS_TIPOS.items()}

    try:
        with engine.connect() as conn:
            # 1. Limpiar tabla temporal si existe
            try:
                conn.execute(text(f'DROP TABLE "{temp_table}"'))
                conn.commit()
            except:
                pass

            # 2. Subir datos a tabla temporal (sin print, solo write directo)
            sys.stdout.write(f"   📤 Subiendo {len(df)} registros a Oracle...\n")
            sys.stdout.flush()
            
            df.to_sql(temp_table, con=engine, if_exists='replace', index=False, 
                     dtype=dtype, method='multi', chunksize=1000)

            # 3. Construir query MERGE
            cols = df.columns.tolist()
            set_clause = ", ".join([f'T."{c}"=S."{c}"' for c in cols if c != 'ID'])
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
            sys.stdout.write(f"   🔄 Ejecutando MERGE...\n")
            sys.stdout.flush()
            
            conn.execute(text(sql_merge))
            conn.commit()

            # 5. Limpieza
            conn.execute(text(f'DROP TABLE "{temp_table}"'))
            conn.commit()

    except Exception as e:
        sys.stdout.write(f"\n{Fore.RED}❌ Error en Merge: {e}\n")
        sys.stdout.flush()
        raise

# ============================================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ============================================================================

def main():
    inicio = time.time()
    
    # Header sin usar print múltiple
    sys.stdout.write(f"\n{Fore.CYAN}{'='*80}\n")
    sys.stdout.write(f"{Fore.MAGENTA}🚀 SINCRONIZACIÓN INCREMENTAL - OPORTUNIDADES (OPTIMIZADO)\n")
    sys.stdout.write(f"{Fore.CYAN}{'='*80}\n")
    sys.stdout.write(f"{Fore.WHITE}📅 Fecha corte: {FECHA_FILTRO}\n")
    sys.stdout.write(f"{Fore.WHITE}🎯 Tabla destino: {TABLE_ID}\n")
    sys.stdout.write(f"{Fore.CYAN}{'='*80}\n\n")
    sys.stdout.flush()

    # 1. Estimación
    sys.stdout.write(f"{Fore.YELLOW}⏳ Calculando cambios...\n")
    sys.stdout.flush()
    
    total, page_size = obtener_estimacion()
    
    if total == 0:
        sys.stdout.write(f"{Fore.GREEN}✅ No hay cambios. Todo está actualizado.\n\n")
        sys.stdout.flush()
        return

    total_paginas = math.ceil(total / page_size)
    sys.stdout.write(f"{Fore.WHITE}   ✓ Cambios detectados: {total}\n")
    sys.stdout.write(f"{Fore.WHITE}   ✓ Páginas a procesar: {total_paginas}\n\n")
    sys.stdout.flush()

    # 2. Descarga de IDs (listado)
    items = obtener_datos_secuencial(total_paginas)
    if not items:
        sys.stdout.write(f"{Fore.RED}❌ No se obtuvieron datos\n\n")
        sys.stdout.flush()
        return
    
    sys.stdout.write(f"{Fore.GREEN}   ✓ {len(items)} oportunidades encontradas\n\n")
    sys.stdout.flush()

    # 3. Obtener detalles completos (PARALELO con 8 workers)
    detalles = obtener_detalles(items)
    sys.stdout.write(f"{Fore.GREEN}   ✓ {len(detalles)} detalles obtenidos\n\n")
    sys.stdout.flush()

    # 4. Procesar y hacer MERGE
    try:
        sys.stdout.write(f"{Fore.YELLOW}⚙️  Procesando datos...\n\n")
        sys.stdout.flush()
        
        df_final = procesar_datos(detalles)
        
        sys.stdout.write(f"{Fore.YELLOW}🔄 Sincronizando con Oracle...\n\n")
        sys.stdout.flush()
        
        ejecutar_merge_oracle(df_final, engine_oracle, TABLE_ID)
        
        sys.stdout.write(f"\n{Fore.GREEN}{'='*80}\n")
        sys.stdout.write(f"{Fore.GREEN}✅ SINCRONIZACIÓN EXITOSA\n")
        sys.stdout.write(f"{Fore.GREEN}{'='*80}\n\n")
        sys.stdout.flush()
        
    except Exception as e:
        sys.stdout.write(f"\n{Fore.RED}{'='*80}\n")
        sys.stdout.write(f"{Fore.RED}❌ ERROR CRÍTICO: {e}\n")
        sys.stdout.write(f"{Fore.RED}{'='*80}\n\n")
        sys.stdout.flush()
        raise

    # Tiempo total
    mins, secs = divmod(time.time() - inicio, 60)
    sys.stdout.write(f"{Fore.CYAN}⏱️  Tiempo total: {int(mins)}m {int(secs)}s\n\n")
    sys.stdout.flush()

if __name__ == "__main__":
    main()
