{{ config(materialized='incremental') }}

select distinct
    service_type as service_type_key,

    service_type
from {{ ref('int_taxi_trips_zones_joined') }}