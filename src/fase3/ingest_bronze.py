import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

# --- Constantes ---
# Es una buena práctica definir las rutas como variables para facilitar su mantenimiento.
# Usamos rutas relativas desde la ubicación del script (src/fase3/).
DATA_FILE_PATH = '../../data/ventas_big.csv'
BRONZE_OUTPUT_PATH = '../../warehouse/bronze/ventas'
JAVA_HOME_PATH = '/usr/lib/jvm/java-17-openjdk-amd64'

def get_dir_size(path='.'):
    """
    Calcula el tamaño total de un directorio en megabytes.
    """
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total / (1024 * 1024) # Convertir a MB

def main():
    """
    Función principal para la ingesta de datos a la capa Bronze.
    - Lee datos de un CSV.
    - Realiza un casting de tipos explícito.
    - Guarda los datos en formato Parquet en la capa Bronze.
    - Compara el tamaño de los archivos de origen y destino.
    """
    print("🚀 Iniciando el proceso de ingesta a la capa Bronze...")

    # --- Configuración de Spark ---
    # Es fundamental establecer JAVA_HOME para que PySpark encuentre el JDK correcto.
    os.environ['JAVA_HOME'] = JAVA_HOME_PATH
    
    spark = SparkSession.builder \
        .appName("IngestaBronze") \
        .master("local[*]") \
        .getOrCreate()

    print("✅ Sesión de Spark iniciada correctamente.")

    # --- Lectura de Datos ---
    # Leemos el archivo CSV. inferSchema puede ser lento y propenso a errores
    # en producción, por lo que haremos casting explícito.
    print(f"📖 Leyendo datos desde: {DATA_FILE_PATH}")
    df_raw = spark.read.csv(DATA_FILE_PATH, header=True, inferSchema=False)

    # --- Transformación y Casting de Tipos ---
    # En la capa Bronze, nos aseguramos de que los tipos de datos sean los correctos.
    # Esto evita problemas en las capas posteriores.
    print("🔧 Realizando casting de tipos de datos...")
    df_bronze = df_raw.withColumn("id_transaccion", col("id_transaccion").cast(IntegerType())) \
                     .withColumn("tienda_id", col("tienda_id").cast(IntegerType())) \
                     .withColumn("monto", col("monto").cast(DoubleType())) \
                     .withColumn("fecha", col("fecha").cast(TimestampType()))

    # --- Escritura en la Capa Bronze ---
    # Guardamos los datos en formato Parquet, que es columnar y optimizado para análisis.
    # 'overwrite' asegura que si volvemos a ejecutar el script, no falle.
    print(f"💾 Escribiendo datos en formato Parquet en: {BRONZE_OUTPUT_PATH}")
    df_bronze.write.mode("overwrite").parquet(BRONZE_OUTPUT_PATH)

    print("\n🎉 ¡Proceso de ingesta a Bronze completado exitosamente!")
    print(f"Verifica los archivos generados en la carpeta: {os.path.abspath(BRONZE_OUTPUT_PATH)}")
    
    # --- Verificación ---
    print("\n🔍 Verificando los datos escritos (primeras 5 filas):")
    df_check = spark.read.parquet(BRONZE_OUTPUT_PATH)
    df_check.printSchema()
    df_check.show(5)

    # --- Comparación de Tamaño ---
    print("\n⚖️  Comparando tamaño de archivos...")
    csv_size_mb = os.path.getsize(DATA_FILE_PATH) / (1024 * 1024)
    parquet_size_mb = get_dir_size(BRONZE_OUTPUT_PATH)
    
    print(f"Tamaño del CSV original : {csv_size_mb:.2f} MB")
    print(f"Tamaño del Parquet final : {parquet_size_mb:.2f} MB")
    
    if parquet_size_mb > 0:
        ahorro_porcentaje = (1 - parquet_size_mb / csv_size_mb) * 100
        print(f"✨ Ahorro de espacio     : {ahorro_porcentaje:.2f}%")

    # --- Finalización ---
    spark.stop()


if __name__ == "__main__":
    main()
