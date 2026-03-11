#!/bin/bash

# Este script ejecuta el pipeline completo de la Fase 3 de forma robusta.
# 1. Limpia los directorios de salida para garantizar un estado inicial limpio.
# 2. Activa el entorno y configura las variables necesarias.
# 3. Ejecuta los modelos y las pruebas de calidad de datos con 'dbt build'.
# 4. Genera el sitio web de la documentación.
# --- MODO DE DEPURACIÓN ---
# Activa el modo de depuración para ver cada comando que se ejecuta.
# set -x

# Termina el script inmediatamente si cualquier comando falla
set -e

# --- 1. LIMPIEZA DEL WAREHOUSE ---
echo "🧹 Limpiando directorios del warehouse..."
# Usamos `rm -rf` para borrar el CONTENIDO de las carpetas silver y gold.
# El `*` es importante para no borrar las carpetas en sí.
rm -rf /home/coder/project/warehouse/silver/*
rm -rf /home/coder/project/warehouse/gold/*
echo "✅ Directorios limpios."

# --- 2. CONFIGURACIÓN DEL ENTORNO ---
echo "🐍 Activando entorno virtual..."
source /home/coder/project/venv/bin/activate
echo "🐍 Configurando Java..."
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo "✅ Entorno listo."

# --- 3. EJECUCIÓN Y PRUEBA DEL PIPELINE ---
echo "🚀 Ejecutando modelos y pruebas con 'dbt build'..."
# Nos movemos a la carpeta del proyecto dbt para que los comandos funcionen
cd /home/coder/project/src/fase3/datalab_dbt/
# 'dbt build' ejecuta los modelos y las pruebas en orden.
# Si algo falla (un modelo o una prueba), el script se detendrá gracias a 'set -e'.
dbt build --profiles-dir .

# --- 4. GENERACIÓN DE DOCUMENTACIÓN ---
echo "📚 Generando la documentación del proyecto..."
dbt docs generate --profiles-dir .
echo "🎉 ¡Pipeline y documentación generados exitosamente!"
echo "ℹ️ Para explorar la documentación, ejecuta 'dbt docs serve' desde la carpeta 'src/fase3/datalab_dbt/'."
