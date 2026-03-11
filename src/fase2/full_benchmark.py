import time
import os
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Constantes ---
DATA_FILE = 'data/ventas_big.csv'
JAVA_HOME_PATH = '/usr/lib/jvm/java-17-openjdk-amd64'

def benchmark_pandas():
    """Ejecuta el benchmark usando Pandas."""
    print("\n--- 🐼 Iniciando Benchmark con Pandas ---")
    start_time = time.time()
    try:
        df = pd.read_csv(DATA_FILE)
        result_df = (
            df[df['monto'] > 100]
            .groupby('tienda_id')
            .agg(monto_total=('monto', 'sum'), monto_promedio=('monto', 'mean'))
        )
        print("Resultado de Pandas (primeras 5 filas):")
        print(result_df.head())
    except Exception as e:
        print(f"❌ Error durante la ejecución de Pandas: {e}")
        return None
    end_time = time.time()
    return end_time - start_time

def benchmark_polars():
    """Ejecuta el benchmark usando Polars (Lazy)."""
    print("\n--- 🐻‍❄️ Iniciando Benchmark con Polars ---")
    start_time = time.time()
    try:
        lazy_df = pl.scan_csv(DATA_FILE)
        result_df = (
            lazy_df
            .filter(pl.col('monto') > 100)
            .group_by('tienda_id')
            .agg(
                pl.sum('monto').alias('monto_total'),
                pl.mean('monto').alias('monto_promedio')
            )
            .collect()
        )
        print("Resultado de Polars (primeras 5 filas):")
        print(result_df.head())
    except Exception as e:
        print(f"❌ Error durante la ejecución de Polars: {e}")
        return None
    end_time = time.time()
    return end_time - start_time

def benchmark_pyspark_query_only():
    """
    Ejecuta el benchmark con PySpark, midiendo SOLO el tiempo de la consulta,
    excluyendo el tiempo de inicialización de la sesión.
    """
    print("\n--- 🔥 Iniciando Benchmark con PySpark (Solo Consulta) ---")
    os.environ['JAVA_HOME'] = JAVA_HOME_PATH
    spark = None
    try:
        # --- INICIO DE SESIÓN (FUERA DEL CRONÓMETRO) ---
        print("Iniciando SparkSession (este tiempo no se cuenta)...")
        spark = (
            SparkSession.builder
            .appName("FullBenchmarkQueryOnly")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .getOrCreate()
        )
        print("SparkSession iniciada.")
        
        # --- INICIO DE LA MEDICIÓN DE LA CONSULTA ---
        start_time = time.time()
        
        df = spark.read.csv(DATA_FILE, header=True, inferSchema=True)
        result_df = (
            df
            .filter(F.col('monto') > 100)
            .groupBy('tienda_id')
            .agg(
                F.sum('monto').alias('monto_total'),
                F.avg('monto').alias('monto_promedio')
            )
        )
        
        print("Resultado de PySpark (primeras 5 filas):")
        result_df.show(5) # Acción que dispara la ejecución
        
        end_time = time.time()
        # --- FIN DE LA MEDICIÓN ---
        
        return end_time - start_time

    except Exception as e:
        print(f"❌ Error durante la ejecución de PySpark: {e}")
        return None
    finally:
        if spark:
            print("Deteniendo SparkSession...")
            spark.stop()

if __name__ == "__main__":
    if not os.path.exists(DATA_FILE):
        print(f"❌ Error: El archivo de datos '{DATA_FILE}' no existe.")
        print("   Por favor, ejecute primero 'src/generate_data_benchmark.py'.")
    else:
        # Ejecutar todos los benchmarks
        t_pandas = benchmark_pandas()
        t_polars = benchmark_polars()
        t_pyspark_query = benchmark_pyspark_query_only()
        
        print("\n\n--- 📊 RESULTADOS FINALES DEL BENCHMARK ---")
        print(f"Operación: Filtrar(monto>100), Agrupar(tienda_id), Sumar y Promediar monto.")
        print(f"Dataset: {DATA_FILE} (5 millones de filas)")
        print("-" * 50)
        
        if t_pandas is not None:
            print(f"🐼 Pandas:                  {t_pandas:.4f} segundos")
        if t_polars is not None:
            print(f"🐻‍❄️ Polars:                  {t_polars:.4f} segundos")
        if t_pyspark_query is not None:
            print(f"🔥 PySpark (solo consulta):   {t_pyspark_query:.4f} segundos")
        
        print("-" * 50)

        # Comparativas
        if t_pandas and t_polars:
            if t_polars < t_pandas:
                print(f"🚀 Polars fue {round(t_pandas/t_polars, 2)}x más rápido que Pandas.")
        
        if t_pyspark_query and t_polars:
            if t_polars < t_pyspark_query:
                print(f"🚀 Polars fue {round(t_pyspark_query/t_polars, 2)}x más rápido que el procesamiento de PySpark.")
            else:
                 print(f"🚀 El procesamiento de PySpark fue {round(t_polars/t_pyspark_query, 2)}x más rápido que Polars.")
