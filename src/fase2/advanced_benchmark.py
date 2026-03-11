import time
import os
import threading
import multiprocessing
import psutil
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Constantes ---
DATA_FILE = 'data/ventas_big.csv'
JAVA_HOME_PATH = '/usr/lib/jvm/java-17-openjdk-amd64'

# --- Función de Monitoreo ---
def monitor_process(proc, queue):
    """
    Observa un proceso y registra su uso de CPU y memoria.
    """
    p = psutil.Process(proc.pid)
    cpu_usage = []
    mem_usage_rss = []
    
    while proc.is_alive():
        try:
            cpu_usage.append(p.cpu_percent(interval=0.1))
            mem_usage_rss.append(p.memory_info().rss / (1024 * 1024)) # a MB
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break
            
    # Calcular métricas finales
    metrics = {
        'avg_cpu': sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0,
        'peak_mem_mb': max(mem_usage_rss) if mem_usage_rss else 0,
    }
    
    # Intentar obtener el I/O del disco, que puede fallar si el proceso termina muy rápido
    try:
        metrics['disk_io_read_mb'] = p.io_counters().read_bytes / (1024 * 1024)
    except psutil.NoSuchProcess:
        metrics['disk_io_read_mb'] = 0 # El proceso terminó antes de que pudiéramos leer el I/O

    queue.put(metrics)

# --- Funciones de Benchmark (diseñadas para correr en procesos separados) ---
def run_pandas(queue):
    df = pd.read_csv(DATA_FILE)
    result = (
        df[df['monto'] > 100]
        .groupby('tienda_id')
        .agg(monto_total=('monto', 'sum'), monto_promedio=('monto', 'mean'))
        .sort_index()
    )
    queue.put(result)

def run_polars(queue):
    lf = pl.scan_csv(DATA_FILE)
    result = (
        lf.filter(pl.col('monto') > 100)
        .group_by('tienda_id')
        .agg(
            pl.sum('monto').alias('monto_total'),
            pl.mean('monto').alias('monto_promedio')
        )
        .sort('tienda_id')
        .collect()
    )
    queue.put(result.to_pandas()) # Convertir para comparación

def run_pyspark(queue):
    os.environ['JAVA_HOME'] = JAVA_HOME_PATH
    spark = SparkSession.builder.appName("AdvancedBenchmark").master("local[*]").getOrCreate()
    
    df = spark.read.csv(DATA_FILE, header=True, inferSchema=True)
    result = (
        df.filter(F.col('monto') > 100)
        .groupBy('tienda_id')
        .agg(
            F.sum('monto').alias('monto_total'),
            F.avg('monto').alias('monto_promedio')
        )
        .orderBy('tienda_id')
    )
    queue.put(result.toPandas()) # Convertir para comparación
    spark.stop()

# --- Orquestador Principal ---
def run_benchmark(name, target_func):
    print(f"\n--- 🚀 Ejecutando benchmark para: {name} ---")
    
    result_queue = multiprocessing.Queue()
    metrics_queue = multiprocessing.Queue()
    
    # Crear y empezar el proceso de benchmark
    proc = multiprocessing.Process(target=target_func, args=(result_queue,))
    start_time = time.time()
    proc.start()
    
    # Crear y empezar el hilo de monitoreo
    monitor = threading.Thread(target=monitor_process, args=(proc, metrics_queue))
    monitor.start()
    
    # Esperar a que ambos terminen
    proc.join()
    execution_time = time.time() - start_time
    monitor.join()
    
    result_df = result_queue.get()
    metrics = metrics_queue.get()
    
    return execution_time, metrics, result_df

if __name__ == "__main__":
    if not os.path.exists(DATA_FILE):
        print(f"❌ Error: El archivo de datos '{DATA_FILE}' no existe.")
    else:
        # --- Ejecución ---
        t_pandas, m_pandas, df_pandas = run_benchmark("Pandas", run_pandas)
        t_polars, m_polars, df_polars = run_benchmark("Polars", run_polars)
        t_pyspark, m_pyspark, df_pyspark = run_benchmark("PySpark", run_pyspark)

        # --- Reporte ---
        print("\n\n--- 📊 RESULTADOS DEL BENCHMARK AVANZADO ---")
        print(f"{'Métrica':<25} | {'Pandas':>15} | {'Polars':>15} | {'PySpark':>15}")
        print("-" * 75)
        print(f"{'Tiempo (segundos)':<25} | {t_pandas:>15.2f} | {t_polars:>15.2f} | {t_pyspark:>15.2f}")
        print(f"{'Memoria Máx. (MB)':<25} | {m_pandas['peak_mem_mb']:>15.2f} | {m_polars['peak_mem_mb']:>15.2f} | {m_pyspark['peak_mem_mb']:>15.2f}")
        print(f"{'CPU Prom. (%)':<25} | {m_pandas['avg_cpu']:>15.2f} | {m_polars['avg_cpu']:>15.2f} | {m_pyspark['avg_cpu']:>15.2f}")
        print(f"{'Lectura Disco (MB)':<25} | {m_pandas['disk_io_read_mb']:>15.2f} | {m_polars['disk_io_read_mb']:>15.2f} | {m_pyspark['disk_io_read_mb']:>15.2f}")
        
        # --- Verificación de Correctitud ---
        print("\n--- ✅ Verificando la correctitud de los resultados ---")

        # Normalizar el DataFrame de Pandas para la comparación
        # groupby() convierte 'tienda_id' en el índice, lo movemos de nuevo a una columna.
        df_pandas_norm = df_pandas.reset_index()
        
        # Los DataFrames de Polars y PySpark ya vienen con 'tienda_id' como columna.
        # Solo nos aseguramos de que el index sea estándar para la comparación.
        df_polars_norm = df_polars.reset_index(drop=True)
        df_pyspark_norm = df_pyspark.reset_index(drop=True)
        
        all_ok = True

        # Comparación 1: Pandas vs. Polars
        try:
            print("\n[Comparando Pandas vs. Polars]")
            pd.testing.assert_frame_equal(
                df_pandas_norm,
                df_polars_norm,
                check_dtype=False,
                atol=1e-5 # Tolerancia para diferencias de punto flotante (e.g., en promedios)
            )
            print("✅ Pandas y Polars: OK")
        except AssertionError as e:
            all_ok = False
            print("❌ ¡Error! Los resultados de Pandas y Polars no coinciden.")
            print("--- Detalles del Error ---")
            print(e)
            print("\n--- Head() de DataFrame Pandas ---")
            print(df_pandas_norm.head())
            print("\n--- Info() de DataFrame Pandas ---")
            df_pandas_norm.info()
            print("\n--- Head() de DataFrame Polars ---")
            print(df_polars_norm.head())
            print("\n--- Info() de DataFrame Polars ---")
            df_polars_norm.info()


        # Comparación 2: Pandas vs. PySpark
        try:
            print("\n[Comparando Pandas vs. PySpark]")
            pd.testing.assert_frame_equal(
                df_pandas_norm,
                df_pyspark_norm,
                check_dtype=False,
                atol=1e-5 # Tolerancia para diferencias de punto flotante
            )
            print("✅ Pandas y PySpark: OK")
        except AssertionError as e:
            all_ok = False
            print("❌ ¡Error! Los resultados de Pandas y PySpark no coinciden.")
            print("--- Detalles del Error ---")
            print(e)
            print("\n--- Head() de DataFrame Pandas ---")
            print(df_pandas_norm.head())
            print("\n--- Info() de DataFrame Pandas ---")
            df_pandas_norm.info()
            print("\n--- Head() de DataFrame PySpark ---")
            print(df_pyspark_norm.head())
            print("\n--- Info() de DataFrame PySpark ---")
            df_pyspark_norm.info()

        if all_ok:
            print("\n\n🎉 ¡Todos los resultados son numéricamente idénticos!")
