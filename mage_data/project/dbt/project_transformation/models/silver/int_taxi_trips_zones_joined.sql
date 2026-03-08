with 
trips as (

    select *
    from {{ ref('stg_taxi_trips') }}

),

zones as (

    select *
    from {{ ref('stg_zones') }}

),

clean_trips as (

    select
        coalesce(vendor_id, 0) as vendor_id,

        pickup_ts,
        dropff_ts,

        case
            when passengers_count is null then 0
            when passengers_count < 0 then 0
            when passengers_count > 10 then 10
            else passengers_count
        end as passengers_count,

        trip_distance,

        pickup_location,
        dropff_location,

        ratecode_id,

        coalesce(payment_type, 0) as payment_type,

        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,

        service_type, 
        source_month, 
        ingest_ts, 
        trip_duration_minutes

    from trips

    where
        -- Null obligatorios
        pickup_ts is not null
        and dropff_ts is not null
        and trip_distance is not null
        and pickup_location is not null
        and dropff_location is not null
        and total_amount is not null
        -- solo por revision
        and service_type is not null
        and source_month is not null
        and ingest_ts is not null
        and trip_duration_minutes is not null

        -- Reglas lógicas
        and pickup_ts <= dropff_ts
        and trip_distance >= 0
        and total_amount >= 0

        -- Solo datos antes de 2022
        and pickup_ts >= '2022-01-01'
        and dropff_ts >= '2022-01-01'

        -- Validar que el mes de ingest coincida con el mes real del viaje
        -- solo validamos ingest_ts porque un viaje podria terminar el siguiente mes
        and date_trunc('month', pickup_ts)
            = date_trunc('month', to_date(source_month, 'YYYY-MM'))

)

select
    ct.*,
    zones_pickup.zone_name as pickup_location_name,
    zones_dropoff.zone_name as dropoff_location_name

from clean_trips ct

inner join zones as zones_pickup
    on ct.pickup_location = zones_pickup.location_id

inner join zones as zones_dropoff
    on ct.dropff_location = zones_dropoff.location_id