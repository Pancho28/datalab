import pandas as pd
import polars as pl
import time

def test_pandas():
    start = time.time()
    df = pd.read_csv('data/ventas_masivas.csv')
    res = df.groupby('tienda_id').agg({'monto': 'sum'})
    end = time.time()
    
    print(f"\n[Pandas] Total de registros a agrupar: {len(df):,}")
    print(res)
    return end - start

def test_polars():
    start = time.time()
    # Usamos lazy mode para máxima potencia
    lazy_df = pl.scan_csv('data/ventas_masivas.csv')

    # Creamos dos planes lazy: uno para la agregación y otro para el conteo
    agg_plan = lazy_df.group_by('tienda_id').agg(pl.col('monto').sum())
    count_plan = lazy_df.select(pl.len())
    
    # Ejecutamos ambos planes de forma optimizada con collect_all
    res, count_df = pl.collect_all([agg_plan, count_plan])
    end = time.time()

    print(f"\n[Polars] Total de registros a agrupar: {count_df.item():,}")
    print(res)
    return end - start

if __name__ == "__main__":
    t_pandas = test_pandas()
    t_polars = test_polars()
    
    print(f"\n--- 📊 RESULTADOS ---")
    print(f"Pandas: {t_pandas:.4f} segundos")
    print(f"Polars: {t_polars:.4f} segundos")
    print(f"🚀 Polars es {round(t_pandas/t_polars, 2)}x más rápido")