from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import requests

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:

    schema_name = 'raw'  # Specify the name of the schema to export data to
    table_name = 'zones'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    #llamada al trigger    
    headers = {
    'Content-Type': 'application/json',
    }

    json_data = {
        'pipeline_run': {
            'variables': {
                'key1': 'value1',
                'key2': 'value2',
            },
        },
    }

    response = requests.post(
        'http://localhost:6789/api/pipeline_schedules/3/pipeline_runs/e6d4006167784638a4eaa2d9255161ba',
        headers=headers,
        json=json_data,
    )
