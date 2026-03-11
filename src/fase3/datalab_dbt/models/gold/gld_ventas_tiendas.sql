-- gld_ventas_por_tienda.sql
-- Este modelo agrega los datos de ventas para obtener métricas a nivel de tienda.

{{
  config(
    materialized='table'
  )
}}

SELECT
    tienda_id,
    SUM(monto) AS monto_total_ventas,
    COUNT(venta_id) AS numero_de_transacciones,
    AVG(monto) AS monto_promedio_por_transaccion

FROM
    -- ¡IMPORTANTE! Ahora leemos desde nuestra tabla Silver.
    -- La función ref() es la forma correcta de referenciar OTROS MODELOS en dbt.
    -- dbt automáticamente resuelve la ruta y construye el grafo de dependencias (DAG).
    {{ ref('slv_ventas') }}

GROUP BY
    tienda_id

ORDER BY
    tienda_id