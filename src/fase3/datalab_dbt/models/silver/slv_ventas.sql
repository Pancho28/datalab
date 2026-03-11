-- slv_ventas.sql
-- Este modelo transforma los datos crudos de ventas en una tabla limpia y estandarizada.

-- Configuración del modelo:
-- Le decimos a dbt que materialice el resultado de este SELECT como una tabla Parquet.
{{
  config(
    materialized='table'
  )
}}

SELECT
    -- Renombramos columnas para mayor claridad y consistencia de negocio.
    id_transaccion      AS venta_id,
    tienda_id,
    monto,

    -- Hacemos casting y renombramos la columna de fecha.
    CAST(fecha AS TIMESTAMP) AS fecha_venta,

    -- Enriquecemos los datos extrayendo partes de la fecha.
    -- Esto es muy útil para análisis y agregaciones posteriores.
    YEAR(CAST(fecha AS TIMESTAMP)) AS anio_venta,
    MONTH(CAST(fecha AS TIMESTAMP)) AS mes_venta,
    DAY(CAST(fecha AS TIMESTAMP)) AS dia_venta,
    DAYOFWEEK(CAST(fecha AS TIMESTAMP)) AS dia_semana_venta

FROM
    -- Usamos la función source() para referenciar nuestra fuente de datos bronze.
    -- Esto permite a dbt entender el linaje completo de los datos.
    parquet.`/home/coder/project/warehouse/bronze/ventas`

WHERE
    -- Añadimos una simple regla de calidad de datos.
    -- Solo incluimos transacciones con un monto positivo.
    monto > 0
