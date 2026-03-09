import pandas as pd
import numpy as np
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import gc

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

def optimize_types(df):
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float32')
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype('int32')
    return df

@transformer
def load_and_export(partition_data, *args, **kwargs):
    # 1. DATOS DE LA PARTICIÓN

    year = partition_data['year']
    month = partition_data['month']
    service = partition_data['service']

    """
    year = "2025"
    month = "06"
    service = "green"
    """
    
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet'

    print(url)
    
    # 2. CONFIGURACIÓN DB
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    schema_name = 'raw'
    table_name = 'taxi_trips'

    print(f'Procesando batch: {service} {year}-{month}')

    try:
        # 3. DESCARGA
        df = pd.read_parquet(url, engine="pyarrow")
        df = optimize_types(df)
        
        # Estandarizar columnas lpep -> tpep
        rename_map = {
            'lpep_pickup_datetime': 'tpep_pickup_datetime',
            'lpep_dropoff_datetime': 'tpep_dropoff_datetime'
        }
        df.rename(columns=rename_map, inplace=True)

        # Agregar metadatos
        df['service_type'] = service
        df['source_month'] = f'{year}-{month}'
        df['ingest_ts'] = pd.Timestamp.utcnow()

        # 4. EXPORTACIÓN INMEDIATA
        loader = Postgres.with_config(ConfigFileLoader(config_path, config_profile))
        with loader as connection:
            # Limpiar datos viejos del mismo lote para evitar duplicados

            delete_query = f"DELETE FROM {schema_name}.{table_name} WHERE source_month = '{year}-{month}' AND service_type = '{service}'"
            try:
                connection.execute(delete_query)
            except:
                pass # Si la tabla no existe aún
            

            # Insertar
            connection.export(df, schema_name, table_name, index=False, if_exists='append')
            print(f'ÉXITO: {service} {year}-{month} subido ({len(df)} filas).')

        # 5. LIMPIEZA AGRESIVA DE MEMORIA
        del df
        gc.collect()

    except Exception as e:
        print(f'ERROR en {service} {year}-{month}: {str(e)}')
    
    return partition_data # Retornamos algo simple para que Mage marque el bloque como completado