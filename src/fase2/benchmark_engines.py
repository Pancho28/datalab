import time
import os
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Constantes ---
DATA_FILE = 'data/ventas_big.csv'
REPORT_FILE = 'debug/last_benchmark_results.txt'
JAVA_HOME_PATH = '/usr/lib/jvm/java-17-openjdk-amd64'

def benchmark_polars():
    """Ejecuta el benchmark usando Polars."""
    print("\n--- 🚀 Iniciando Benchmark con Polars ---")
    start_time = time.time()
    
    # Usamos scan_csv para procesamiento Lazy (no se carga todo en memoria de inmediato)
    # El motor de Polars optimiza el plan de ejecución completo.
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
            .collect() # .collect() dispara la ejecución de todo el plan
        )
        
        print("Resultado de Polars (primeras 5 filas):")
        print(result_df.head())
        
    except Exception as e:
        print(f"❌ Error durante la ejecución de Polars: {e}")
        return None
        
    end_time = time.time()
    return end_time - start_time

def benchmark_pyspark():
    """Ejecuta el benchmark usando PySpark."""
    print("\n--- 🔥 Iniciando Benchmark con PySpark ---")
    
    # Regla Innegociable: Aseguramos la ruta de Java.
    os.environ['JAVA_HOME'] = JAVA_HOME_PATH
    
    spark = None  # Inicializar para el bloque finally
    start_time = time.time()
    
    try:
        # master('local[*]') usa todos los cores de la CPU.
        # Es la configuración óptima para una sola máquina.
        spark = (
            SparkSession.builder
            .appName("BenchmarkPolarsVsSpark")
            .master("local[*]")
            .config("spark.driver.memory", "4g") # Asignar memoria al driver
            .getOrCreate()
        )
        
        # Spark es Lazy por naturaleza. La lectura (read.csv) es una transformación.
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
        
        # .show() es una acción que dispara la computación distribuida.
        print("Resultado de PySpark (primeras 5 filas):")
        result_df.show(5)
        
    except Exception as e:
        print(f"❌ Error durante la ejecución de PySpark: {e}")
        return None
    finally:
        if spark:
            spark.stop() # Muy importante cerrar la sesión
            
    end_time = time.time()
    return end_time - start_time

if __name__ == "__main__":
    if not os.path.exists(DATA_FILE):
        print(f"❌ Error: El archivo de datos '{DATA_FILE}' no existe.")
        print("   Por favor, ejecute primero 'src/generate_data_benchmark.py'.")
    else:
        time_polars = benchmark_polars()
        time_pyspark = benchmark_pyspark()
        
        print("\n\n--- 📊 RESUMEN DEL BENCHMARK ---")
        
        if time_polars is not None and time_pyspark is not None:
            report_lines = [
                f"Benchmark completado el {time.strftime('%Y-%m-%d %H:%M:%S')}",
                "Operación: Filtrar(monto>100), Agrupar(tienda_id), Sumar y Promediar monto.",
                f"Dataset: {DATA_FILE}",
                "",
                f"🐻‍❄️ Polars: {time_polars:.4f} segundos",
                f"🔥 PySpark: {time_pyspark:.4f} segundos",
            ]
            
            if time_polars < time_pyspark:
                factor = round(time_pyspark / time_polars, 2)
                report_lines.append(f"\n🏆 Polars fue {factor}x más rápido.")
            else:
                factor = round(time_polars / time_pyspark, 2)
                report_lines.append(f"\n🏆 PySpark fue {factor}x más rápido.")
            
            report_content = "\n".join(report_lines)
            
            print(report_content)
            
            with open(REPORT_FILE, 'w') as f:
                f.write(report_content)
            
            print(f"\n✅ Reporte guardado en: {REPORT_FILE}")
        else:
            print("\nEl benchmark no pudo completarse debido a errores.")
