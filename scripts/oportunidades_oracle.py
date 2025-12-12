# ============================================================================
# 🔄 SINCRONIZACIÓN OPORTUNIDADES - VERSIÓN OPTIMIZADA Y ESTABLE
# ============================================================================
# - 12 workers paralelos (balance velocidad/estabilidad)
# - Manejo robusto de errores y timeouts
# - Output limpio sin barras molestas
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
import threading

init(autoreset=True)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

DIAS_ATRAS = 1
fecha_corte = datetime.now(timezone.utc) - timedelta(days=DIAS_ATRAS)
FECHA_FILTRO = fecha_corte.isoformat()

TABLE_ID = "OPORTUNIDADES_REAL"

# CREDENCIALES DESDE VARIABLES DE ENTORNO
API_TOKEN = os.environ.get('CLIENTIFY_API_TOKEN')
ORACLE_USER = os.environ.get('ORACLE_USER')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')
ORACLE_DSN = os.environ.get('ORACLE_DSN')

if not all([API_TOKEN, ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN]):
    raise ValueError("❌ Faltan variables de entorno")

# Session global con configuraciones optimizadas
session = requests.Session()
session.headers.update({
    "Authorization": f"Token {API_TOKEN}", 
    "Content-Type": "application/json"
})

# Configuración moderada de connection pooling
adapter = requests.adapters.HTTPAdapter(
    pool_connections=20,  # Reducido de 100 a 20
    pool_maxsize=20,      # Reducido de 100 a 20
    max_retries=3,
    pool_block=False
)
session.mount('http://', adapter)
session.mount('https://', adapter)

URL_OPORTUNIDADES = "https://api.clientify.net/v1/deals/"
engine_oracle = create_engine(f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}")

# ============================================================================
# MAPA DE COLUMNAS
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
    """Request con manejo robusto de errores"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=12)  # Aumentado a 12 segundos
            
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(2)
                continue
            elif r.status_code >= 500:
                time.sleep(2)
                continue
            else:
                return None
                
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None
        except Exception:
            return None
    
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
    """Descarga todas las páginas de listado"""
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
        
        # Pequeño delay para evitar sobrecarga
        time.sleep(0.1)
    
    sys.stdout.write("\n")
    return items_acumulados


def obtener_detalle_paralelo(item_id):
    """Obtiene el detalle de una oportunidad"""
    url = f"{URL_OPORTUNIDADES}{item_id}/"
    return request_blindado(url)


def obtener_detalles(lista_items):
    """
    Obtiene detalles en paralelo con 12 workers
    Incluye manejo robusto de errores y timeouts
    """
    detalles_fin = []
    errores = []
    total_items = len(lista_items)
    
    # Lock para thread-safe
    lock = threading.Lock()
    contador = [0]
    
    def procesar_item(item):
        """Procesa un item individual"""
        try:
            detalle = obtener_detalle_paralelo(item['id'])
            
            with lock:
                contador[0] += 1
                current = contador[0]
                
                if detalle:
                    detalles_fin.append(detalle)
                else:
                    errores.append(item['id'])
                
                # Mostrar progreso cada 50 items
                if current % 50 == 0 or current == total_items:
                    sys.stdout.write(f"   → {current}/{total_items} detalles ({len(errores)} errores)\r")
                    sys.stdout.flush()
            
            return detalle
            
        except Exception as e:
            with lock:
                errores.append(item['id'])
            return None
    
    # Ejecutar en paralelo con 12 workers
    with ThreadPoolExecutor(max_workers=12) as executor:
        # Submit todas las tareas
        future_to_item = {
            executor.submit(procesar_item, item): item 
            for item in lista_items
        }
        
        # Esperar resultados con timeout global de 30 minutos
        try:
            for future in as_completed(future_to_item, timeout=1800):
                try:
                    future.result(timeout=15)  # Timeout individual de 15 segundos
                except Exception:
                    pass
        except Exception as e:
            print(f"\n⚠️  Timeout global o error: {e}")
    
    sys.stdout.write("\n")
    
    if errores:
        print(f"{Fore.YELLOW}   ⚠️  {len(errores)} detalles fallaron (continuando con los exitosos)")
    
    return detalles_fin


def procesar_datos(lista_datos):
    """Procesa y limpia los datos"""
    df = pd.DataFrame(lista_datos)
    
    if df.empty:
        return df

    cols_db = list(MAPA_COLUMNAS_TIPOS.keys())
    for col in cols_db:
        if col not in df.columns:
            df[col] = None
    
    df = df[cols_db]

    def _limpiar(val):
        if val is None:
            return None
        
        if isinstance(val, np.ndarray):
            val = val.tolist()
        
        if isinstance(val, (int, float, str, bool)):
            if pd.isna(val):
                return None
        
        if isinstance(val, (list, dict)):
            try:
                return json.dumps(val, ensure_ascii=False)
            except:
                return "[]"
        
        return str(val)

    for col, tipo in MAPA_COLUMNAS_TIPOS.items():
        if isinstance(tipo, (String, CLOB)):
            df[col] = df[col].apply(_limpiar)
        elif isinstance(tipo, (Integer, Numeric, Float)):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df.columns = [c.upper() for c in df.columns]
    df = df.drop_duplicates(subset=['ID'], keep='last')

    return df


def ejecutar_merge_oracle(df, engine, table_name):
    """Ejecuta MERGE en Oracle"""
    if df.empty:
        return

    temp_table = f"{table_name}_TEMP"
    dtype = {k.upper(): v for k, v in MAPA_COLUMNAS_TIPOS.items()}

    try:
        with engine.connect() as conn:
            try:
                conn.execute(text(f'DROP TABLE "{temp_table}"'))
                conn.commit()
            except:
                pass

            df.to_sql(
                temp_table,
                con=engine,
                if_exists='replace',
                index=False,
                dtype=dtype,
                method='multi',
                chunksize=1000
            )

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

            conn.execute(text(sql_merge))
            conn.commit()

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
    """Función principal"""
    inicio = time.time()
    
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.MAGENTA}🚀 SINCRONIZACIÓN OPORTUNIDADES - VERSIÓN ESTABLE")
    print(f"{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}📅 Fecha corte: {FECHA_FILTRO}")
    print(f"{Fore.WHITE}🎯 Tabla: {TABLE_ID}")
    print(f"{Fore.WHITE}⚡ Workers: 12 (optimizado para estabilidad)")
    print(f"{Fore.CYAN}{'='*80}\n")

    # 1. Estimación
    print(f"{Fore.YELLOW}⏳ Calculando cambios...")
    total, page_size = obtener_estimacion()
    
    if total == 0:
        print(f"{Fore.GREEN}✅ No hay cambios\n")
        return

    total_paginas = math.ceil(total / page_size)
    print(f"{Fore.WHITE}   ✓ Detectados: {total} registros")
    print(f"{Fore.WHITE}   ✓ Páginas: {total_paginas}\n")

    # 2. Descarga páginas
    print(f"{Fore.YELLOW}📥 Descargando páginas...")
    items = obtener_datos_secuencial(total_paginas)
    
    if not items:
        print(f"{Fore.RED}❌ No se obtuvieron datos\n")
        return
    
    print(f"{Fore.GREEN}   ✓ {len(items)} oportunidades encontradas\n")

    # 3. Obtener detalles
    print(f"{Fore.YELLOW}🔍 Obteniendo detalles (12 workers paralelos)...")
    detalles = obtener_detalles(items)
    print(f"{Fore.GREEN}   ✓ {len(detalles)} detalles obtenidos\n")

    # 4. Procesar y sincronizar
    try:
        print(f"{Fore.YELLOW}⚙️  Procesando datos...")
        df_final = procesar_datos(detalles)
        print(f"{Fore.GREEN}   ✓ {len(df_final)} registros procesados\n")
        
        print(f"{Fore.YELLOW}🔄 Sincronizando con Oracle...")
        ejecutar_merge_oracle(df_final, engine_oracle, TABLE_ID)
        print(f"{Fore.GREEN}   ✓ MERGE completado\n")
        
        print(f"{Fore.GREEN}{'='*80}")
        print(f"{Fore.GREEN}✅ SINCRONIZACIÓN EXITOSA")
        print(f"{Fore.GREEN}{'='*80}\n")
        
    except Exception as e:
        print(f"\n{Fore.RED}{'='*80}")
        print(f"{Fore.RED}❌ ERROR: {e}")
        print(f"{Fore.RED}{'='*80}\n")
        raise

    mins, secs = divmod(time.time() - inicio, 60)
    print(f"{Fore.CYAN}⏱️  Tiempo total: {int(mins)}m {int(secs)}s")
    print(f"{Fore.CYAN}{'='*80}\n")


if __name__ == "__main__":
    main()
