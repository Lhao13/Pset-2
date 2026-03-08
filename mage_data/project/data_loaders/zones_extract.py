import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    url_csv = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    
    print("Inicio carga de datos")

    zonas = pd.read_csv(
        url_csv,
    )

    print(f"Datos descargados con forma: {zonas.shape}")

    return zonas


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
