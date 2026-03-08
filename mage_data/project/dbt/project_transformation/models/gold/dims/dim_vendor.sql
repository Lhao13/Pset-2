{{ config(materialized='table') }}

select *
from (
    values
        (1, 'Creative Mobile Technologies, LLC'),
        (2, 'VeriFone Inc.')
) as vendors(vendor_key, vendor_name)