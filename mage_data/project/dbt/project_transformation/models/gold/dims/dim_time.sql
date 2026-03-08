{{ config(materialized='table') }}

with times as (

    select distinct
        extract(hour from pickup_ts)   as hour,
        extract(minute from pickup_ts) as minute
    from {{ ref('int_taxi_trips_zones_joined') }}

)

select

    (hour * 100 + minute) as time_key,

    hour,
    minute,

    case
        when hour between 5 and 11 then 'morning'
        when hour between 12 and 16 then 'afternoon'
        when hour between 17 and 21 then 'evening'
        else 'night'
    end as time_period

from times