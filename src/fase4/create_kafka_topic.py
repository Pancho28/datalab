import os
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from dotenv import load_dotenv

"""
Script para crear un topic de Kafka de forma manual y explícita.

Este script se conecta al broker de Kafka especificado en el archivo .env
y crea un nuevo topic con una configuración predeterminada (3 particiones,
factor de replicación 1).

Uso:
    python src/fase4/create_kafka_topic.py <nombre_del_topic>

Ejemplo:
    python src/fase4/create_kafka_topic.py ventas_stream
"""

# --- Carga de configuración ---
load_dotenv()
KAFKA_BROKER_URL = os.getenv("KAFKA_PRIVATE_URL")

# --- Validación de Argumentos de Línea de Comandos ---
# sys.argv es una lista que contiene los argumentos.
# sys.argv[0] es el nombre del script.
# sys.argv[1] es el primer argumento.
if len(sys.argv) < 2:
    print("❌ ERROR: Argumento faltante.")
    print(f"Uso: python {sys.argv[0]} <nombre_del_topic>")
    sys.exit(1) # Salir del script con un código de error

# El nombre del topic se toma del primer argumento de la línea de comandos
NEW_TOPIC_NAME = sys.argv[1]

print("--- Iniciando Script de Creación de Topic ---")
print(f"Broker de Kafka: {KAFKA_BROKER_URL}")
print(f"Topic a crear: {NEW_TOPIC_NAME}")
print("---------------------------------------------")

admin_client = None
try:
    if not KAFKA_BROKER_URL:
        raise ValueError("La variable KAFKA_PRIVATE_URL no está configurada en el archivo .env")

    # 1. Conectarse a Kafka usando el cliente de administración
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER_URL,
        client_id='topic-creator-script'
    )

    # 2. Definir la configuración del nuevo topic
    topic_config = NewTopic(
        name=NEW_TOPIC_NAME,
        num_partitions=3,
        replication_factor=1
    )

    # 3. Intentar crear el topic
    print(f"Intentando crear el topic '{NEW_TOPIC_NAME}'...")
    admin_client.create_topics(new_topics=[topic_config], validate_only=False)
    print(f"\n✅ ¡ÉXITO! Topic '{NEW_TOPIC_NAME}' creado correctamente.")

except TopicAlreadyExistsError:
    print(f"\nℹ️ INFO: El topic '{NEW_TOPIC_NAME}' ya existe. No se requiere ninguna acción.")
except NoBrokersAvailable:
    print(f"\n❌ ERROR: No se pudo conectar a ningún broker en '{KAFKA_BROKER_URL}'. Verifica la URL y que Kafka esté en ejecución.")
except Exception as e:
    print(f"\n❌ ERROR INESPERADO: Ocurrió un error al intentar crear el topic.")
    print(f"Detalle: {e}")
finally:
    if admin_client:
        admin_client.close()
    print("\n--- Script Finalizado ---")