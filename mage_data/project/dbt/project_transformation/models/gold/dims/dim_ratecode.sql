{{ config(materialized='table') }}

select *
from (
    values
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride')
) as ratecodes(ratecode_key, ratecode_name)