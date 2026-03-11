import pandas as pd
import numpy as np
import time

def generate_benchmark_data(n_rows=5_000_000, file_path='data/ventas_big.csv'):
    """Genera un archivo CSV grande para el benchmark."""
    print(f"🚀 Generando {n_rows:,} filas de datos para el benchmark...")
    start_time = time.time()
    
    data = {
        'id_transaccion': np.arange(n_rows),
        'tienda_id': np.random.randint(1, 101, n_rows),
        'monto': np.random.uniform(10.5, 1000.0, n_rows).round(2),
        'fecha': pd.to_datetime(np.random.randint(1609459200, 1672531199, n_rows), unit='s') # Fechas entre 2021 y 2022
    }
    
    df = pd.DataFrame(data)
    
    print(f"Guardando el archivo en {file_path}...")
    df.to_csv(file_path, index=False)
    
    end_time = time.time()
    print(f"✅ Archivo generado exitosamente en {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    generate_benchmark_data()
