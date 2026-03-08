if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

@custom
def generate_partitions(*args, **kwargs):
    years = ['2025']
    services = ['yellow', 'green']

    partitions = []

    for year in years:
        for month in range(7, 13):
            for service in services:
                partitions.append({
                    'year': year,
                    'month': f'{month:02d}',
                    'service': service
                })

    print(f'Total partitions generated: {len(partitions)}')

    # IMPORTANTE: Para que Mage lo reconozca como dinámico, 
    # devolvemos una lista que contiene la lista de particiones.
    return [partitions]