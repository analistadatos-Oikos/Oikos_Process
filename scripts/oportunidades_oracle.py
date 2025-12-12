# ============================================================================
# 🔄 SINCRONIZACIÓN OPORTUNIDADES - VERSIÓN CONSERVADORA (5 WORKERS)
# ============================================================================
# - 5 workers paralelos (máxima estabilidad)
# - Timeouts agresivos para evitar bloqueos
# - Skip automático de requests problemáticos
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
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
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

# CREDENCIALES
API_TOKEN = os.environ.get('CLIENTIFY_API_TOKEN')
ORACLE_USER = os.environ.get('ORACLE_USER')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')
ORACLE_DSN = os.environ.get('ORACLE_DSN')

if not all([API_TOKEN, ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN]):
    raise ValueError("❌ Faltan variables de entorno")

# Session global
session = requests.Session()
session.headers.update({
    "Authorization": f"Token {API_TOKEN}", 
    "Content-Type": "application/json"
})

# Connection pooling conservador
adapter = requests.adapters.HTTPAdapter(
    pool_connections=10,
    pool_maxsize=10,
    max_retries=2,
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

def request_blindado(url, timeout=10):
    """Request con timeout agresivo y pocos reintentos"""
    max_retries = 2  # Solo 2 intentos
    
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=timeout)
            
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(1)
            elif r.status_code >= 500:
                time.sleep(1)
            else:
                return None
                
        except requests.exceptions.Timeout:
            # Si hay timeout, salir rápido
            return None
        except requests.exceptions.RequestException:
            # Cualquier error de red, salir rápido
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
    """Descarga páginas secuencialmente"""
    items_acumulados = []
    
    for page in range(1, total_paginas + 1):
        url = f"{URL_OPORTUNIDADES}?modified[gte]={FECHA_FILTRO}&page={page}"
        data = request_blindado(url, timeout=10)
        
        if data:
            items_acumulados.extend(data.get("results", []))
        
        if page % 10 == 0 or page == total_paginas:
            sys.stdout.write(f"   → {page}/{total_paginas} páginas\r")
            sys.stdout.flush()
        
        time.sleep(0.15)  # Delay para evitar sobrecarga
    
    sys.stdout.write("\n")
    return items_acumulados


def obtener_detalle_paralelo(item_id):
    """Obtiene detalle con timeout corto"""
    url = f"{URL_OPORTUNIDADES}{item_id}/"
    return request_blindado(url, timeout=8)  # Timeout corto de 8 segundos


def obtener_detalles(lista_items):
    """
    Obtiene detalles con SOLO 5 WORKERS
    Timeout agresivo para evitar bloqueos
    """
    detalles_fin = []
    errores = []
    skipped = []
    total_items = len(lista_items)
    
    lock = threading.Lock()
    contador = [0]
    
    print(f"   🔄 Iniciando descarga con 5 workers...")
    print(f"   ⏱️  Timeout por request: 8 segundos")
    print(f"   🔄 Si un request se congela, se skipea automáticamente\n")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Enviar todas las tareas
        future_to_id = {}
        for item in lista_items:
            future = executor.submit(obtener_detalle_paralelo, item['id'])
            future_to_id[future] = item['id']
        
        # Procesar resultados con timeout individual
        for future in as_completed(future_to_id, timeout=3600):  # 1 hora total
            item_id = future_to_id[future]
            
            try:
                # Timeout de 10 segundos para obtener resultado
                detalle = future.result(timeout=10)
                
                with lock:
                    contador[0] += 1
                    current = contador[0]
                    
                    if detalle:
                        detalles_fin.append(detalle)
                    else:
                        errores.append(item_id)
                    
                    # Mostrar progreso cada 25 items
                    if current % 25 == 0 or current == total_items:
                        sys.stdout.write(
                            f"   → {current}/{total_items} | "
                            f"✓ {len(detalles_fin)} OK | "
                            f"✗ {len(errores)} errores | "
                            f"⏭ {len(skipped)} skipped\r"
                        )
                        sys.stdout.flush()
                
            except TimeoutError:
                # Si este request individual se congela, skipear
                with lock:
                    skipped.append(item_id)
                    contador[0] += 1
                    current = contador[0]
                    
                    if current % 25 == 0 or current == total_items:
                        sys.stdout.write(
                            f"   → {current}/{total_items} | "
                            f"✓ {len(detalles_fin)} OK | "
                            f"✗ {len(errores)} errores | "
                            f"⏭ {len(skipped)} skipped\r"
                        )
                        sys.stdout.flush()
                
            except Exception as e:
                with lock:
                    errores.append(item_id)
                    contador[0] += 1
    
    sys.stdout.write("\n")
    
    if errores or skipped:
        total_fallidos = len(errores) + len(skipped)
        print(f"{Fore.YELLOW}   ⚠️  {total_fallidos} requests fallaron (continuando con {len(detalles_fin)} exitosos)")
    
    return detalles_fin


def procesar_datos(lista_datos):
    """Procesa datos"""
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
    """Ejecuta MERGE"""
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
        sys.stdout.write(f"\n{Fore.RED}❌ Error: {e}\n")
        raise


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal"""
    inicio = time.time()
    
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.MAGENTA}🚀 SINCRONIZACIÓN OPORTUNIDADES - VERSIÓN CONSERVADORA")
    print(f"{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}📅 Fecha corte: {FECHA_FILTRO}")
    print(f"{Fore.WHITE}🎯 Tabla: {TABLE_ID}")
    print(f"{Fore.WHITE}⚡ Workers: 5 (máxima estabilidad)")
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

    # 3. Obtener detalles (5 WORKERS)
    print(f"{Fore.YELLOW}🔍 Obteniendo detalles (5 workers - modo estable)...")
    detalles = obtener_detalles(items)
    print(f"{Fore.GREEN}   ✓ {len(detalles)} detalles obtenidos\n")

    # 4. Procesar
    if len(detalles) == 0:
        print(f"{Fore.RED}❌ No se obtuvieron detalles. Abortando.\n")
        return
    
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
