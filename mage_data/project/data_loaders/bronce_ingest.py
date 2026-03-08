import pandas as pd
import numpy as np

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

def optimize_types(df):
    """
    Reduce el consumo de RAM ajustando la precisión de los datos.
    """
    # 1. Optimizar Flotantes (de 64 bits a 32 bits)
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float32')
        
    # 2. Optimizar Enteros (de 64 bits a 32 bits)
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype('int32')
    
    # 3. Optimizar Categorías (para strings que se repiten mucho)
    # VendorID, RatecodeID y store_and_fwd_flag son excelentes candidatos
    for col in ['VendorID', 'RatecodeID', 'store_and_fwd_flag', 'payment_type']:
        if col in df.columns:
            df[col] = df[col].astype('category')
            
    return df

@data_loader
def load_taxi_data(partition_data, *args, **kwargs):
    year = 2025
    month = 11
    service = 'green'

    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet'

    print(f'--- Iniciando: {service} {year}-{month} ---')

    try:
        # Cargamos el archivo
        df = pd.read_parquet(url, engine="pyarrow")
        
        # APLICAMOS OPTIMIZACIÓN INMEDIATAMENTE
        df = optimize_types(df)
        
        # Manejo de nulos de forma más eficiente que .replace
        # (Mantenemos los tipos numéricos usando los nulos nativos de numpy/pandas)
        # df = df.fillna(np.nan) # Opcional, dependiendo de tu base de datos
        
    except Exception as e:
        print(f'Error o archivo inexistente: {service} {year}-{month} -> {e}')
        return None

    # Añadimos columnas de control (service_type como categoría ahorra RAM)
    df['service_type'] = service
    df['service_type'] = df['service_type'].astype('category')
    
    df['source_month'] = f'{year}-{month}'
    df['ingest_ts'] = pd.Timestamp.utcnow()

    # Log de ahorro
    mem_usage = df.memory_usage(deep=True).sum() / 1024**2
    print(f'Descargado: {df.shape} | Uso de RAM aprox: {mem_usage:.2f} MB')

    return df