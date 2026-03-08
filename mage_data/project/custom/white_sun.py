if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from mage_ai.data_preparation.decorators import custom
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader
from os import path


@custom
def create_partitions(*args, **kwargs):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    profile = 'default'

    loader = Postgres.with_config(ConfigFileLoader(config_path, profile))

    start_year = 2022
    end_year = 2025

    queries = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):

            start = f"{year}-{month:02d}-01"

            if month == 12:
                end = f"{year+1}-01-01"
            else:
                end = f"{year}-{month+1:02d}-01"

            partition = f"analytics_gold.fct_trips_{year}_{month:02d}"

            query = f"""
            CREATE TABLE IF NOT EXISTS {partition}
            PARTITION OF analytics_gold.fct_trips
            FOR VALUES FROM ('{start}') TO ('{end}');
            """

            queries.append(query)

    loader.open()
    loader.execute_queries(queries)
    loader.close()

    return {"partitions_created": len(queries)}



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
