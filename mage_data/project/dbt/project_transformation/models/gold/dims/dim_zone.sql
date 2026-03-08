{{ config(
    materialized='incremental'
) }}

with pickup_zones as (

    select distinct
        pickup_location as zone_key,
        pickup_location_name as zone_name
    from {{ ref('int_taxi_trips_zones_joined') }}

),

dropoff_zones as (

    select distinct
        dropff_location as zone_key,
        dropoff_location_name as zone_name
    from {{ ref('int_taxi_trips_zones_joined') }}

),

zones_union as (

    select * from pickup_zones
    union
    select * from dropoff_zones

)

select distinct
    zone_key,
    zone_name
from zones_union