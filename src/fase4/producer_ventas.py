import os
import sys
import json
import polars as pl
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaConnectionError
from dotenv import load_dotenv

"""
Productor de Kafka para simular un flujo de datos de ventas.

Este script lee datos de un archivo CSV, convierte cada fila en un evento JSON
y lo envía a un topic de Kafka especificado.

Permite limitar el número de registros a enviar mediante un argumento opcional.

Uso:
    # Enviar todos los registros
    python src/fase4/producer.py <nombre_del_topic>

    # Enviar un número limitado de registros (ej: los primeros 100)
    python src/fase4/producer.py <nombre_del_topic> 100

Ejemplo:
    python src/fase4/producer.py ventas_stream 50
"""

# --- Configuración y Carga de Entorno ---
load_dotenv()
KAFKA_BROKER_URL = os.getenv("KAFKA_PRIVATE_URL")
# Se modifica la ruta para apuntar al archivo CSV masivo.
SOURCE_DATA_PATH = "data/ventas_masivas.csv"

# --- Validación de Argumentos ---
if len(sys.argv) < 2:
    print("❌ ERROR: Faltan argumentos.")
    print(f"Uso: python {sys.argv[0]} <nombre_del_topic> [numero_de_registros]")
    sys.exit(1)

TOPIC_NAME = sys.argv[1]
# El límite es opcional. Si se proporciona, se convierte a entero. Si no, es None.
try:
    RECORD_LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else None
except ValueError:
    print(f"❌ ERROR: El número de registros debe ser un entero. Recibido: '{sys.argv[2]}'")
    sys.exit(1)

# --- Lógica Principal ---
producer = None
try:
    print("--- Iniciando Productor de Kafka ---")
    
    # 1. Validar configuración
    if not KAFKA_BROKER_URL:
        raise ValueError("La variable KAFKA_PRIVATE_URL no está configurada en el archivo .env")
    if not os.path.exists(SOURCE_DATA_PATH):
        raise FileNotFoundError(f"El archivo de datos no se encontró en la ruta: '{SOURCE_DATA_PATH}'")

    # 2. Leer los datos eficientemente con Polars scan_csv (Lazy Evaluation)
    print(f"Leyendo datos desde '{SOURCE_DATA_PATH}' usando scan_csv...")
    # Usamos scan_csv para una lectura 'lazy', ideal para archivos grandes.
    # No se carga todo el archivo en memoria hasta que es estrictamente necesario.
    lazy_df = pl.scan_csv(SOURCE_DATA_PATH)
    
    # Aplicar el límite si se especificó, manteniendo el procesamiento 'lazy'
    if RECORD_LIMIT is not None:
        print(f"Aplicando límite de {RECORD_LIMIT} registros.")
        lazy_df_to_send = lazy_df.head(RECORD_LIMIT)
    else:
        print("No se especificó límite, se enviarán todos los registros.")
        lazy_df_to_send = lazy_df
        
    # 'collect()' ejecuta las operaciones y carga el resultado en memoria.
    print("Materializando los datos para el envío...")
    df_to_send = lazy_df_to_send.collect()

    total_records = len(df_to_send)
    if total_records == 0:
        print("No hay registros para enviar. Terminando.")
        sys.exit(0)

    # 3. Conectar al broker de Kafka
    print(f"Conectando al broker en '{KAFKA_BROKER_URL}'...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Serializador para convertir los valores del mensaje a JSON y luego a bytes
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        client_id='ventas-producer'
    )
    print("Conexión exitosa.")

    # 4. Enviar los registros
    print(f"\nEnviando {total_records} registros al topic '{TOPIC_NAME}'...")
    # Convertimos el DataFrame a una lista de diccionarios para iterar
    records = df_to_send.to_dicts()

    for i, record in enumerate(records):
        # La llave del mensaje (key) es opcional pero muy recomendada.
        # Ayuda a Kafka a colocar todos los mensajes de una misma entidad (ej: una misma tienda)
        # en la misma partición, garantizando el orden para esa entidad.
        key = str(record.get("tienda_id", "")).encode('utf-8')

        producer.send(TOPIC_NAME, key=key, value=record)
        
        # Imprimir progreso para no tener una pantalla vacía
        print(f"  -> Enviado registro {i + 1}/{total_records}", end="\r")

    # 5. Asegurar el envío de todos los mensajes
    producer.flush()
    print("\n\n✅ ¡ÉXITO! Todos los mensajes han sido enviados al búfer de Kafka.")


except (NoBrokersAvailable, KafkaConnectionError) as e:
    print(f"\n❌ ERROR DE KAFKA: No se pudo conectar o comunicar con el broker en '{KAFKA_BROKER_URL}'.")
    print(f"Detalle: {e}")
except Exception as e:
    print(f"\n❌ ERROR INESPERADO: {e}")
finally:
    if producer:
        producer.close()
    print("\n--- Productor Finalizado ---")