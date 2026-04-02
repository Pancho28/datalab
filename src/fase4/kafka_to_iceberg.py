import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from dotenv import load_dotenv

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
load_dotenv()

KAFKA_BROKER_URL = os.getenv("KAFKA_PRIVATE_URL")
KAFKA_TOPIC = "ventas_stream"

MINIO_URL = os.getenv("MINIO_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "datalab_catalog")
ICEBERG_S3_BUCKET = os.getenv("ICEBERG_S3_BUCKET", "iceberg-datalab")
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "warehouse")


# --- Lógica Principal ---
print("--- Iniciando Consumidor de Streaming Spark (Kafka a Iceberg) ---")

spark_jars_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "software.amazon.awssdk:bundle:2.17.257"
]

spark = (
    SparkSession.builder.appName("KafkaToIceberg")
    .config("spark.jars.packages", ",".join(spark_jars_packages))
    .config(f"spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    
    # --- CAMBIO IMPORTANTE: Usar HadoopCatalog en lugar de Glue ---
    # HadoopCatalog es más simple y robusto para un entorno con MinIO,
    # ya que guarda los metadatos directamente en el sistema de archivos S3.
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", f"s3a://{ICEBERG_S3_BUCKET}/{ICEBERG_WAREHOUSE_PATH}")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
print("Sesión de Spark creada y configurada.")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.default.ventas_iceberg (
        id_transaccion STRING,
        tienda_id STRING,
        monto DOUBLE,
        fecha TIMESTAMP
    )
    USING iceberg
""")

# 1. Definición de los paquetes de Maven necesarios para la sesión de Spark
#    Spark descargará estos JARs automáticamente.
spark_jars_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", # Conector Spark <-> Kafka
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2", # Conector Spark <-> Iceberg
    "org.apache.hadoop:hadoop-aws:3.3.4", # Permite a Spark hablar con S3
    "software.amazon.awssdk:bundle:2.17.257" # SDK de AWS para la conectividad S3
]

# 2. Construcción de la Sesión de Spark con toda la configuración necesaria
spark = (
    SparkSession.builder.appName("KafkaToIceberg")
    # --- Configuración de Paquetes ---
    .config("spark.jars.packages", ",".join(spark_jars_packages))
    
    # --- Configuración del Catálogo de Iceberg ---
    # Habilita las extensiones de Iceberg en Spark
    .config(f"spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Nombra y define nuestro catálogo
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    # Usaremos un catálogo tipo Hadoop, que es simple y funciona bien con MinIO
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.AwsGlueCatalog")
    # Especifica dónde guardará Iceberg los datos y metadatos (en nuestro bucket de MinIO)
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", f"s3a://{ICEBERG_S3_BUCKET}/{ICEBERG_WAREHOUSE_PATH}")
    
    # --- Configuración de la Conexión a MinIO (S3) ---
    # Le decimos a Spark cómo encontrar nuestro servidor MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL)
    # Le pasamos las credenciales de MinIO
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    # Esencial para que la conexión con MinIO funcione
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    # Especifica el sistema de archivos a usar
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    .getOrCreate()
)
print("Sesión de Spark creada y configurada.")

# 3. Creación de la tabla Iceberg (si no existe)
#    Esta es una operación idempotente.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.default.ventas_iceberg (
        id_transaccion STRING,
        tienda_id STRING,
        monto DOUBLE,
        fecha TIMESTAMP
    )
    USING iceberg
""")
print("Tabla Iceberg 'ventas_iceberg' asegurada.")

# 4. Lectura del Stream de Kafka
kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest") # Empezar desde el principio en el primer arranque
    .load()
)

# 5. Procesamiento del Stream
#    Los datos de Kafka vienen como binarios. Necesitamos castearlos y parsear el JSON.

# Define el schema del JSON que esperamos recibir
schema = StructType([
    StructField("id_transaccion", StringType()),
    StructField("tienda_id", StringType()),
    StructField("monto", DoubleType()),
    StructField("fecha", TimestampType())
])

# Transforma el stream
processed_stream_df = (
    kafka_stream_df.select(col("value").cast("string")) # Castear el valor binario a string
    .select(from_json(col("value"), schema).alias("data")) # Parsear el string JSON a una struct
    .select("data.*") # Aplanar la struct para tener las columnas
)

# 6. Escritura del Stream en la tabla Iceberg
print(f"Iniciando el stream para escribir en la tabla Iceberg: {ICEBERG_CATALOG_NAME}.default.ventas_iceberg")
# Ubicación para los checkpoints del streaming, crucial para la tolerancia a fallos
checkpoint_location = f"s3a://{ICEBERG_S3_BUCKET}/checkpoints/ventas_iceberg_checkpoint"

query = (
    processed_stream_df.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="30 seconds") # Procesar datos cada 30 segundos
    .option("path", f"{ICEBERG_CATALOG_NAME}.default.ventas_iceberg")
    .option("checkpointLocation", checkpoint_location)
    .start()
)

print("Stream iniciado. Esperando terminación (Ctrl+C para parar)...")
query.awaitTermination()