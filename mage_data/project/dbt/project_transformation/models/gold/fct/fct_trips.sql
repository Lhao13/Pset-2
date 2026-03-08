{{ config(materialized='incremental') }}

WITH trips AS (

SELECT
    *
FROM {{ ref('int_taxi_trips_zones_joined') }}
WHERE dropff_ts IS NOT NULL   -- elimina filas sin dropoff

)

SELECT

    -- Partition column (OBLIGATORIA)
    date_trunc('day', t.pickup_ts)::date AS pickup_date,

    -- Date keys
    dp.date_key  AS pickup_date_key,
    dd.date_key  AS dropoff_date_key,

    -- Time keys
    pt.time_key  AS pickup_time_key,
    dt.time_key  AS dropoff_time_key,

    -- Location
    p.zone_key AS pickup_location_key,
    d.zone_key AS dropoff_location_key,

    -- Other dimensions
    pay.payment_type_key,
    v.vendor_key,
    r.ratecode_key,
    serty.service_type_key,

    -- Measures
    t.passengers_count,
    t.trip_distance,
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    t.trip_duration_minutes

FROM trips t

-- Date dimension
LEFT JOIN {{ ref('dim_date') }} dp
    ON date_trunc('day', t.pickup_ts)::date = dp.full_date

LEFT JOIN {{ ref('dim_date') }} dd
    ON date_trunc('day', t.dropff_ts)::date = dd.full_date

-- Time dimension
LEFT JOIN {{ ref('dim_time') }} pt
    ON extract(hour from t.pickup_ts) = pt.hour
   AND extract(minute from t.pickup_ts) = pt.minute

LEFT JOIN {{ ref('dim_time') }} dt
    ON extract(hour from t.dropff_ts) = dt.hour
   AND extract(minute from t.dropff_ts) = dt.minute

-- Zone dimension
LEFT JOIN {{ ref('dim_zone') }} p
    ON t.pickup_location = p.zone_key

LEFT JOIN {{ ref('dim_zone') }} d
    ON t.dropff_location = d.zone_key

-- Other dimensions
LEFT JOIN {{ ref('dim_payment_type') }} pay
    ON t.payment_type = pay.payment_type_key

LEFT JOIN {{ ref('dim_vendor') }} v
    ON t.vendor_id = v.vendor_key

LEFT JOIN {{ ref('dim_ratecode') }} r
    ON t.ratecode_id = r.ratecode_key

LEFT JOIN {{ ref('dim_service_type') }} serty
    ON t.service_type = serty.service_type_key