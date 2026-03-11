import pandas as pd
import numpy as np
import time
import sys

def generate_massive_data(n_rows=5_000_000):
    print(f"🚀 Generando {n_rows} filas...")
    data = {
        'id': np.arange(n_rows),
        'tienda_id': np.random.randint(1, 100, n_rows),
        'producto_id': np.random.randint(1, 1000, n_rows),
        'monto': np.random.uniform(10.5, 500.0, n_rows),
        'fecha': pd.date_range(start='2020-01-01', periods=n_rows, freq='s')
    }
    df = pd.DataFrame(data)
    df.to_csv('data/ventas_masivas.csv', index=False)
    print("✅ Archivo data/ventas_masivas.csv creado.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            n_millions = int(sys.argv[1])
            n_rows_to_generate = n_millions * 1_000_000
        except ValueError:
            print("❌ Error: El argumento debe ser un número entero.")
            sys.exit(1)
    else:
        # Valor por defecto si no se proporciona argumento
        n_rows_to_generate = 5_000_000 
    
    generate_massive_data(n_rows_to_generate)