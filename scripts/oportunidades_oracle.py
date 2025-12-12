# ============================================================================
# 🔄 SINCRONIZACIÓN OPORTUNIDADES - VERSIÓN ULTRA-RÁPIDA
# ============================================================================
# - 20 workers paralelos
# - Sin barras de progreso molestas
# - Output limpio
# - Tiempo estimado: 3-4 minutos para 1500+ registros
# ============================================================================

import os
import json
import time
import math
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.types import CLOB, Integer, String, Float, Numeric
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

init(autoreset=True)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

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

# Session global con configuraciones de velocidad
session = requests.Session()
session.headers.update({
    "Authorization": f"Token {API_TOKEN}", 
    "Content-Type": "application/json"
})

# Optimización de conexión HTTP - Connection Pooling
adapter = requests.adapters.HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100,
    max_retries=3,
    pool_block=False
)
session.mount('http://', adapter)
session.mount('https://', adapter)

URL_OPORTUNIDADES = "https://api.clientify.net/v1/deals/"
engine_oracle = create_engine(f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}")

# ============================================================================
# MAPA DE COLUMNAS Y TIPOS
# ============================================================================

MAPA_COLUMNAS_TIPOS = {
    "id": Integer(),
    "probability": Integer(),
    "status": Integer(),
    "who_can_view": Integer(),
    "amount": Numeric(15, 2),
    "created": String(64),
    "modified": String(64),
    "expected_closed_date": String(64),
    "actual_closed_date": String(64),
    "currency": String(50),
    "contact": String(255),
    "contact_name": String(255),
    "contact_email": String(255),
    "contact_phone": String(255),
    "contact_source": String(255),
    "contact_medium": String(255),
    "owner": String(255),
    "owner_name": String(255),
    "owner_picture": String(500),
    "company": String(255),
    "name": String(500),
    "source": String(255),
    "deal_source": String(255),
    "lost_reason": String(500),
    "remarks": String(4000),
    "url": String(500),
    "pipeline": String(255),
    "pipeline_desc": String(255),
    "pipeline_stage": String(255),
    "pipeline_stage_desc": String(255),
    "status_desc": String(255),
    "probability_desc": String(50),
    "amount_user": String(255),
    "custom_fields": CLOB(),
    "tags": CLOB(),
    "products": CLOB(),
    "events": CLOB(),
    "tasks": CLOB(),
    "integrations": CLOB(),
    "involved_companies": CLOB(),
    "involved_contacts": CLOB(),
    "stages_duration": CLOB(),
    "wall_entries": CLOB()
}

# ============================================================================
# FUNCIONES
# ============================================================================

def request_blindado(url):
    """
    Hace un request con reintentos automáticos y timeout agresivo
    """
    max_retries = 2
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=8)
            
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:  # Rate limit
                time.sleep(1)
                continue
            elif r.status_code >= 500:  # Error del servidor
                time.sleep(1)
                continue
            else:
                return None
                
        except Exception as e:
            if attempt == max_retries - 1:
                return None
            time.sleep(0.5)
    
    return None


def obtener_estimacion():
    """
    Calcula cuántos registros hay que procesar
    """
    url = f"{URL_OPORTUNIDADES}?modified[gte]={FECHA_FILTRO}&page=1"
    data = request_blindado(url)
    
    if data:
        total = data.get("count", 0)
        results = data.get("results", [])
        page_size = len(results) if len(results) > 0 else 50
        return total, page_size
    
    return 0, 50


def obtener_datos_secuencial(total_paginas):
    """
    Descarga todas las páginas de listado (solo IDs básicos)
    """
    items_acumulados = []
    
    for page in range(1, total_paginas + 1):
        url = f"{URL_OPORTUNIDADES}?modified[gte]={FECHA_FILTRO}&page={page}"
        data = request_blindado(url)
        
        if data:
            items_acumulados.extend(data.get("results", []))
        
        # Mostrar progreso cada 10 páginas
        if page % 10 == 0 or page == total_paginas:
            sys.stdout.write(f"   → {page}/{total_paginas} páginas\r")
            sys.stdout.flush()
    
    sys.stdout.write("\n")
    return items_acumulados


def obtener_detalle_paralelo(item_id):
    """
    Obtiene el detalle completo de una oportunidad específica
    (Esta función se ejecuta en paralelo)
    """
    url = f"{URL_OPORTUNIDADES}{item_id}/"
    return request_blindado(url)


def obtener_detalles(lista_items):
    """
    Obtiene todos los detalles en paralelo usando ThreadPoolExecutor
    """
    detalles_fin = []
    total_items = len(lista_items)
    contador = [0]  # Lista mutable para usar en closure
    
    def callback(future):
        """Callback que se ejecuta cuando cada future termina"""
        detalle = future.result()
        if detalle:
            detalles_fin.append(detalle)
        
        contador[0] += 1
        
        # Mostrar progreso cada 50 items
        if contador[0] % 50 == 0 or contador[0] == total_items:
            sys.stdout.write(f"   → {contador[0]}/{total_items} detalles\r")
            sys.stdout.flush()
    
    # Ejecutar en paralelo con 20 workers
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        
        for item in lista_items:
            future = executor.submit(obtener_detalle_paralelo, item['id'])
            future.add_done_callback(callback)
            futures.append(future)
        
        # Esperar a que todos terminen
        for future in futures:
            future.result()
    
    sys.stdout.write("\n")
    return detalles_fin


def procesar_datos(lista_datos):
    """
    Procesa y limpia los datos para que sean compatibles con Oracle
    """
    df = pd.DataFrame(lista_datos)
    
    if df.empty:
        return df

    # Asegurar que existan todas las columnas necesarias
    cols_db = list(MAPA_COLUMNAS_TIPOS.keys())
    for col in cols_db:
        if col not in df.columns:
            df[col] = None
    
    df = df[cols_db]

    # Función para limpiar valores
    def _limpiar(val):
        if val is None:
            return None
        
        # Convertir numpy arrays a listas
        if isinstance(val, np.ndarray):
            val = val.tolist()
        
        # Verificar NaN solo en tipos escalares
        if isinstance(val, (int, float, str, bool)):
            if pd.isna(val):
                return None
        
        # Convertir listas y dicts a JSON
        if isinstance(val, (list, dict)):
            try:
                return json.dumps(val, ensure_ascii=False)
            except:
                return "[]"
        
        return str(val)

    # Aplicar tipos de datos según el mapa
    for col, tipo in MAPA_COLUMNAS_TIPOS.items():
        if isinstance(tipo, (String, CLOB)):
            df[col] = df[col].apply(_limpiar)
        elif isinstance(tipo, (Integer, Numeric, Float)):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convertir nombres de columnas a mayúsculas (Oracle estándar)
    df.columns = [c.upper() for c in df.columns]

    # Eliminar duplicados (quedarse con el más reciente por ID)
    df = df.drop_duplicates(subset=['ID'], keep='last')

    return df


def ejecutar_merge_oracle(df, engine, table_name):
    """
    Ejecuta un MERGE (UPSERT) en Oracle usando tabla temporal
    """
    if df.empty:
        return

    temp_table = f"{table_name}_TEMP"
    dtype = {k.upper(): v for k, v in MAPA_COLUMNAS_TIPOS.items()}

    try:
        with engine.connect() as conn:
            # 1. Eliminar tabla temporal si existe
            try:
                conn.execute(text(f'DROP TABLE "{temp_table}"'))
                conn.commit()
            except:
                pass

            # 2. Subir datos a tabla temporal
            df.to_sql(
                temp_table,
                con=engine,
                if_exists='replace',
                index=False,
                dtype=dtype,
                method='multi',
                chunksize=1000
            )

            # 3. Construir query MERGE dinámico
            cols = df.columns.tolist()
            
            # Columnas para UPDATE (todas menos ID)
            set_clause = ", ".join([f'T."{c}"=S."{c}"' for c in cols if c != 'ID'])
            
            # Columnas para INSERT
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
            conn.execute(text(sql_merge))
            conn.commit()

            # 5. Limpiar tabla temporal
            conn.execute(text(f'DROP TABLE "{temp_table}"'))
            conn.commit()

    except Exception as e:
        sys.stdout.write(f"\n{Fore.RED}❌ Error en Merge: {e}\n")
        sys.stdout.flush()
        raise


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """
    Función principal que coordina todo el proceso
    """
    inicio = time.time()
    
    # ========== HEADER ==========
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.MAGENTA}🚀 SINCRONIZACIÓN OPORTUNIDADES - MODO ULTRA-RÁPIDO")
    print(f"{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}📅 Fecha corte: {FECHA_FILTRO}")
    print(f"{Fore.WHITE}🎯 Tabla destino: {TABLE_ID}")
    print(f"{Fore.WHITE}⚡ Workers paralelos: 20")
    print(f"{Fore.CYAN}{'='*80}\n")

    # ========== 1. ESTIMACIÓN ==========
    print(f"{Fore.YELLOW}⏳ Calculando cambios desde hace {DIAS_ATRAS} día(s)...")
    total, page_size = obtener_estimacion()
    
    if total == 0:
        print(f"{Fore.GREEN}✅ No hay cambios. Todo está actualizado.\n")
        return

    total_paginas = math.ceil(total / page_size)
    print(f"{Fore.WHITE}   ✓ Cambios detectados: {total} registros")
    print(f"{Fore.WHITE}   ✓ Páginas a descargar: {total_paginas}\n")

    # ========== 2. DESCARGA DE PÁGINAS ==========
    print(f"{Fore.YELLOW}📥 Descargando páginas (listado básico)...")
    items = obtener_datos_secuencial(total_paginas)
    
    if not items:
        print(f"{Fore.RED}❌ No se obtuvieron datos\n")
        return
    
    print(f"{Fore.GREEN}   ✓ {len(items)} oportunidades encontradas\n")

    # ========== 3. OBTENER DETALLES (PARALELO) ==========
    print(f"{Fore.YELLOW}🔍 Obteniendo detalles completos (20 workers paralelos)...")
    detalles = obtener_detalles(items)
    print(f"{Fore.GREEN}   ✓ {len(detalles)} detalles obtenidos correctamente\n")

    # ========== 4. PROCESAR Y SINCRONIZAR ==========
    try:
        print(f"{Fore.YELLOW}⚙️  Procesando y limpiando datos...")
        df_final = procesar_datos(detalles)
        print(f"{Fore.GREEN}   ✓ {len(df_final)} registros procesados\n")
        
        print(f"{Fore.YELLOW}🔄 Sincronizando con Oracle Database...")
        print(f"   → Creando tabla temporal...")
        print(f"   → Subiendo {len(df_final)} registros...")
        print(f"   → Ejecutando MERGE (UPDATE + INSERT)...")
        
        ejecutar_merge_oracle(df_final, engine_oracle, TABLE_ID)
        
        print(f"{Fore.GREEN}   ✓ MERGE completado exitosamente\n")
        
        # ========== ÉXITO ==========
        print(f"{Fore.GREEN}{'='*80}")
        print(f"{Fore.GREEN}✅ SINCRONIZACIÓN EXITOSA")
        print(f"{Fore.GREEN}{'='*80}\n")
        
    except Exception as e:
        # ========== ERROR ==========
        print(f"\n{Fore.RED}{'='*80}")
        print(f"{Fore.RED}❌ ERROR CRÍTICO")
        print(f"{Fore.RED}{'='*80}")
        print(f"{Fore.RED}{str(e)}\n")
        raise

    # ========== TIEMPO TOTAL ==========
    mins, secs = divmod(time.time() - inicio, 60)
    print(f"{Fore.CYAN}⏱️  Tiempo total de ejecución: {int(mins)}m {int(secs)}s")
    print(f"{Fore.CYAN}{'='*80}\n")


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    main()
