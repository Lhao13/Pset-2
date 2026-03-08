from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import gc

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    
    if df is None or df.empty:
        print("No hay datos — skipping export")
        return
    # 1. ESTANDARIZACIÓN DE COLUMNAS (Green -> Yellow)
    # Renombramos las columnas lpep a tpep si existen en el DataFrame
    rename_map = {
        'lpep_pickup_datetime': 'tpep_pickup_datetime',
        'lpep_dropoff_datetime': 'tpep_dropoff_datetime'
    }
    df.rename(columns=rename_map, inplace=True)

    schema_name = 'bronze'
    table_name = 'taxi_trips'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Obtenemos los valores del batch actual para la limpieza
    # Asumimos que estas columnas vienen de tu bloque anterior
    source_month = df['source_month'].iloc[0]
    service_type = df['service_type'].iloc[0]


    loader = Postgres.with_config(ConfigFileLoader(config_path, config_profile))

    with loader as connection:
        # 2. PREVENIR DUPLICADOS (Idempotencia)
        # Borramos los datos existentes para este mes y servicio específico antes de insertar
        print(f"Limpiando datos previos para: {service_type} - {source_month}")
        
        delete_query = f"""
            DELETE FROM {schema_name}.{table_name} 
            WHERE source_month = '{source_month}' 
            AND service_type = '{service_type}'
        """
        
        # Ejecutamos el delete. Si la tabla no existe, fallará, por eso usamos un try/except simple
        try:
            connection.execute(delete_query)
        except Exception as e:
            print(f"Nota: No se pudo ejecutar el DELETE (posiblemente la tabla no existe aún): {e}")

        # 3. EXPORTACIÓN
        print(f"Insertando batch: {len(df)} filas...")
        connection.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='append'  # Siempre append porque ya limpiamos arriba
        )

    print("Exportación finalizada con éxito.")

    # Al final del bloque:
    del df # Borra la referencia al objeto
    gc.collect() # Fuerza al sistema a limpiar la RAM