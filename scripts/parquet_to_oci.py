# ============================================================================
# SCRIPT COMPLETO: Oracle -> Parquet -> OCI Object Storage
# SIN CHUNKS - LECTURA COMPLETA GARANTIZADA
# ============================================================================

import os
import pandas as pd
import json
import tempfile
import time
from sqlalchemy import create_engine
from oci.object_storage import ObjectStorageClient
from tqdm import tqdm

print("=" * 80)
print("🚀 INICIO DEL PROCESO ETL: OPORTUNIDADES + ACTIVIDADES")
print("=" * 80)

# --- CREDENCIALES ORACLE (DB Fuente) ---
ORACLE_USER = os.environ.get('ORACLE_USER')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')
ORACLE_DSN = os.environ.get('ORACLE_DSN')

# --- CREDENCIALES OCI OBJECT STORAGE (Bucket Destino) ---
BUCKET_NAME = os.environ.get('OCI_BUCKET_NAME')
REGION = os.environ.get('OCI_REGION')
NAMESPACE = os.environ.get('OCI_NAMESPACE')

# --- CREDENCIALES DE AUTENTICACIÓN OCI SDK ---
USER_OCID = os.environ.get('OCI_USER_OCID')
TENANCY_OCID = os.environ.get('OCI_TENANCY_OCID')
KEY_FINGERPRINT = os.environ.get('OCI_KEY_FINGERPRINT')
PRIVATE_KEY_CONTENT = os.environ.get('OCI_PRIVATE_KEY')

# Validación de credenciales
required_vars = [
    ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN,
    BUCKET_NAME, REGION, NAMESPACE,
    USER_OCID, TENANCY_OCID, KEY_FINGERPRINT, PRIVATE_KEY_CONTENT
]

if not all(required_vars):
    raise ValueError("❌ Faltan variables de entorno. Verifica los Secrets en GitHub.")

# --- CONFIGURACIÓN DE TABLAS A PROCESAR ---
TABLAS_CONFIG = [
    {
        "tabla": "OPORTUNIDADES_REAL",
        "archivo": "Archivos_ParquetOportunidades_Real.parquet",
        "nombre": "OPORTUNIDADES"
    },
    {
        "tabla": "ACTIVIDADES_TOTALES",
        "archivo": "Archivos_ParquetActividades_Total.parquet",
        "nombre": "ACTIVIDADES"
    }
]

# --- CONFIGURACIÓN OCI ---
KEY_FILE_PATH = "/tmp/oci_key_new.pem"
OBJECT_STORAGE_CLIENT = None

try:
    with open(KEY_FILE_PATH, 'w') as f:
        f.write(PRIVATE_KEY_CONTENT.strip())

    OCI_CONFIG = {
        "user": USER_OCID,
        "fingerprint": KEY_FINGERPRINT,
        "key_file": KEY_FILE_PATH,
        "tenancy": TENANCY_OCID,
        "region": REGION
    }

    OBJECT_STORAGE_CLIENT = ObjectStorageClient(OCI_CONFIG)
    print("✅ Configuración OCI SDK exitosa.\n")

except Exception as e:
    print(f"❌ Error al configurar OCI SDK: {e}")
    import traceback
    traceback.print_exc()
    raise

# --- FUNCIONES DE LIMPIEZA ---
def clean_clob_pilo(value):
    """Limpia valores CLOB y convierte JSON strings si aplica"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (list, dict)):
        return value
    text_val = str(value).strip()
    if text_val.startswith('[') or text_val.startswith('{'):
        try:
            return json.loads(text_val)
        except:
            pass
    return text_val

def limpiar_versiones_antiguas(client, namespace, bucket_name, object_name):
    """Elimina TODAS las versiones anteriores de un objeto"""
    try:
        versions = client.list_object_versions(
            namespace_name=namespace,
            bucket_name=bucket_name,
            prefix=object_name
        ).data.items

        if not versions:
            return

        for version in versions:
            try:
                client.delete_object(
                    namespace_name=namespace,
                    bucket_name=bucket_name,
                    object_name=version.name,
                    version_id=version.version_id
                )
            except:
                pass

    except:
        pass

def upload_to_oci_force_overwrite(client, namespace, bucket_name, object_name, file_path, pbar):
    """Sube archivo a OCI con sobrescritura REAL"""
    if not client:
        return False

    try:
        limpiar_versiones_antiguas(client, namespace, bucket_name, object_name)

        with open(file_path, 'rb') as f:
            client.put_object(
                namespace_name=namespace,
                bucket_name=bucket_name,
                object_name=object_name,
                put_object_body=f
            )

        return True

    except Exception as e:
        print(f"\n❌ Error al subir: {e}")
        return False

# --- FUNCIÓN PARA PROCESAR UNA TABLA ---
def procesar_tabla(config, engine, pbar):
    """Procesa una tabla: extrae, limpia y sube a OCI"""
    tabla = config["tabla"]
    archivo = config["archivo"]
    nombre = config["nombre"]

    ruta_temporal = None

    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            ruta_temporal = tmp.name

        # 1. LECTURA COMPLETA (SIN CHUNKS) - LEE TODO DE UNA VEZ
        pbar.set_description(f"📚 {nombre}: Leyendo TODOS los datos")
        inicio = time.time()

        # LECTURA DIRECTA SIN CHUNKS - GARANTIZA TODOS LOS REGISTROS
        df = pd.read_sql(f'SELECT * FROM "{tabla}"', engine)

        duracion = time.time() - inicio

        if df.empty:
            pbar.set_description(f"⚠️ {nombre}: Vacío")
            return False

        registros_leidos = len(df)
        pbar.set_description(f"✅ {nombre}: {registros_leidos:,} registros en {duracion:.1f}s")
        pbar.update(30)

        # 2. Limpieza MÍNIMA (sin eliminar registros)
        pbar.set_description(f"🧹 {nombre}: Limpiando columnas")

        # Solo limpiar columnas específicas de texto
        cols_texto = ['LOST_REASON', 'SOURCE', 'DEAL_SOURCE', 'REMARKS', 'STATUS_DESC']
        for col in cols_texto:
            if col in df.columns:
                df[col] = df[col].astype(str).replace({
                    'None': None, 'nan': None, '<NA>': None
                })

        # Procesar columnas objeto solo si tienen JSON
        cols_obj = df.select_dtypes(include=['object']).columns
        for col in cols_obj:
            df[col] = df[col].apply(clean_clob_pilo)

            sample = df[col].dropna()
            if len(sample) > 0:
                todas_listas = all(isinstance(x, list) for x in sample.head(50))
                if todas_listas:
                    df[col] = df[col].apply(lambda x: x if isinstance(x, list) else None)

        # VERIFICAR QUE NO SE PERDIERON REGISTROS
        if len(df) != registros_leidos:
            print(f"\n⚠️ ALERTA: Se perdieron registros en {nombre}!")
            print(f"   Antes: {registros_leidos:,} | Después: {len(df):,}")

        pbar.update(30)

        # 3. Guardar Parquet
        pbar.set_description(f"💾 {nombre}: Creando Parquet")
        df.to_parquet(ruta_temporal, index=False, engine='pyarrow')
        tamaño_mb = os.path.getsize(ruta_temporal) / (1024 * 1024)

        print(f"\n   📊 {nombre}: {len(df):,} registros → {tamaño_mb:.2f} MB")

        pbar.update(20)

        # 4. Subir a OCI
        pbar.set_description(f"☁️ {nombre}: Subiendo a OCI ({tamaño_mb:.1f} MB)")
        resultado = upload_to_oci_force_overwrite(
            client=OBJECT_STORAGE_CLIENT,
            namespace=NAMESPACE,
            bucket_name=BUCKET_NAME,
            object_name=archivo,
            file_path=ruta_temporal,
            pbar=pbar
        )

        if resultado:
            pbar.set_description(f"✅ {nombre}: Completado ({len(df):,} reg, {tamaño_mb:.1f} MB)")
        else:
            pbar.set_description(f"❌ {nombre}: Error al subir")

        pbar.update(20)

        return resultado

    except Exception as e:
        print(f"\n❌ Error en {nombre}: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if ruta_temporal and os.path.exists(ruta_temporal):
            os.remove(ruta_temporal)

# --- FUNCIÓN PRINCIPAL ---
def main():
    if not OBJECT_STORAGE_CLIENT:
        print("❌ No se puede continuar sin OCI.")
        return

    try:
        engine = create_engine(
            f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}"
        )

        print("✅ Conexión a Oracle establecida.\n")

        resultados = {}
        total_pasos = len(TABLAS_CONFIG) * 100

        with tqdm(total=total_pasos, desc="🚀 Procesando", unit="%", ncols=100) as pbar:
            for config in TABLAS_CONFIG:
                exito = procesar_tabla(config, engine, pbar)
                resultados[config["nombre"]] = exito

        # Resumen
        print("\n" + "=" * 80)
        print("📊 RESUMEN FINAL")
        print("=" * 80)
        for nombre, exito in resultados.items():
            estado = "✅ ÉXITO" if exito else "❌ FALLÓ"
            print(f"{estado} - {nombre}")

        print("\n🎉 Proceso completado.")

    except Exception as e:
        print(f"\n❌ Error general: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        if os.path.exists(KEY_FILE_PATH):
            os.remove(KEY_FILE_PATH)

if __name__ == "__main__":
    main()
