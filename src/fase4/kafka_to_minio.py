import os
import sys
import json
import polars as pl
import boto3
from confluent_kafka import Consumer, KafkaException
from io import BytesIO
import uuid
from dotenv import load_dotenv

"""
Consumidor ligero de Kafka que guarda datos en MinIO como Parquet.

Este script demuestra un enfoque sin Spark/Iceberg. Consume mensajes en lotes,
los convierte a un DataFrame de Polars, escribe el resultado a un búfer de
Parquet en memoria y sube el objeto a un bucket de S3 (MinIO).

ADVERTENCIA: Este enfoque puede generar el 'Small File Problem' y no ofrece
garantías transaccionales como Iceberg.
"""

# --- Configuración ---
load_dotenv()
KAFKA_BROKER_URL = os.getenv("KAFKA_PRIVATE_URL")
KAFKA_TOPIC = "ventas_stream"
CONSUMER_GROUP_ID = "lightweight-parquet-writers-v2"

MINIO_URL = os.getenv("MINIO_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
S3_BUCKET = os.getenv("ICEBERG_S3_BUCKET") # Reutilizamos el mismo bucket
S3_PREFIX = "lightweight_data/" # Guardaremos en una carpeta separada

BATCH_SIZE = 5000  # Número de mensajes por lote
BATCH_TIMEOUT = 30 # Segundos máximos de espera por lote

# --- Lógica Principal ---
conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'group.id': CONSUMER_GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False # Nosotros controlaremos cuándo guardar el progreso
}

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])

# Cliente de S3 (MinIO)
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

print("--- Iniciando Consumidor Ligero ---")
print(f"Escuchando el topic '{KAFKA_TOPIC}' en el grupo '{CONSUMER_GROUP_ID}'")

try:
    while True:
        messages = consumer.consume(num_messages=BATCH_SIZE, timeout=BATCH_TIMEOUT)
        
        if not messages:
            print("No se recibieron mensajes en este lote, esperando...")
            continue

        if any(msg.error() for msg in messages):
            # Filtramos errores de Kafka (ej. fin de partición)
            kafka_errors = [msg.error() for msg in messages if msg.error()]
            print(f"Se encontraron errores de Kafka, ignorando mensajes: {kafka_errors}")
            messages = [msg for msg in messages if not msg.error()]
            if not messages:
                continue

        print(f"Procesando un lote de {len(messages)} mensajes.")
        
        # 1. Decodificar mensajes y cargar en una lista
        records = [json.loads(msg.value().decode('utf-8')) for msg in messages]

        # 2. Convertir a DataFrame de Polars
        df = pl.DataFrame(records)

        # 3. Escribir a un búfer de Parquet en memoria
        buffer = BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0) # Rebobinar el búfer al principio para la lectura

        # 4. Subir a MinIO
        # Generamos un nombre de archivo único para cada lote
        object_name = f"{S3_PREFIX}{uuid.uuid4()}.parquet"
        s3_client.upload_fileobj(buffer, S3_BUCKET, object_name)

        print(f"  -> Lote guardado exitosamente en s3://{S3_BUCKET}/{object_name}")

        # 5. Confirmar a Kafka que hemos procesado los mensajes
        consumer.commit(asynchronous=False)
        print("  -> Offset confirmado en Kafka.")


except KeyboardInterrupt:
    print("\nDeteniendo consumidor...")
except Exception as e:
    print(f"\n❌ ERROR INESPERADO: {e}")
finally:
    consumer.close()
    print("--- Consumidor Finalizado ---")
