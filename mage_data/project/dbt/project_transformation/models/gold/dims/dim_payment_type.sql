{{ config(
    materialized='incremental',
    schema='gold'
) }}

select *
from (
    values
        (1, 'Credit card', 'card'),
        (2, 'Cash', 'cash'),
        (3, 'No charge', 'other'),
        (4, 'Dispute', 'other'),
        (5, 'Unknown', 'other'),
        (6, 'Voided trip', 'other')
) as payment_types(payment_type_key, payment_type, payment_category)