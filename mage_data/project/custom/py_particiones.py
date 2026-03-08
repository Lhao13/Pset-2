from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    profile = 'default'

    loader = Postgres.with_config(ConfigFileLoader(config_path, profile))

    sql = """
    DO $$
    DECLARE
        start_date date := '2022-01-01';
        end_date date := '2026-01-01';
        partition_start date;
        partition_end date;
        partition_name text;
    BEGIN
        partition_start := start_date;

        WHILE partition_start < end_date LOOP
            
            partition_end := (partition_start + INTERVAL '1 month')::date;

            partition_name := format(
                'analytics_gold.fct_trips_%s',
                to_char(partition_start, 'YYYY_MM')
            );

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %s PARTITION OF analytics_gold.fct_trips
                 FOR VALUES FROM (%L) TO (%L);',
                partition_name,
                partition_start,
                partition_end
            );

            partition_start := partition_end;

        END LOOP;
    END $$;
    """

    loader.open()
    loader.execute_queries([sql])
    loader.close()

    return {"status": "partitions created"}

