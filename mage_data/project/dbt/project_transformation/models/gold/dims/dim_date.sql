{{ config(materialized='table') }}

with dates as (

    select distinct
        date_trunc('day', pickup_ts)::date as full_date
    from {{ ref('int_taxi_trips_zones_joined') }}

)

select
    to_char(full_date, 'YYYYMMDD')::int as date_key,
    full_date,
    extract(year from full_date) as year,
    extract(month from full_date) as month,
    extract(day from full_date) as day,
    extract(dow from full_date) as day_of_week,
    extract(week from full_date) as week_of_year,
    extract(quarter from full_date) as quarter

from dates