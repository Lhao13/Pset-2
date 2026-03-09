if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import requests

@custom
def transform_custom(*args, **kwargs):

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
        'http://localhost:6789/api/pipeline_schedules/5/pipeline_runs/5b9dccec81ac4762bf1f0e2b1c372df2',
        headers=headers,
        json=json_data,
    )


    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
