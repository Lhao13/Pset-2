DROP TABLE IF EXISTS analytics_gold.fct_trips CASCADE;

--particiones por date

CREATE TABLE analytics_gold.fct_trips (
    pickup_date date NOT NULL,

    pickup_date_key int,
    dropoff_date_key int,

    pickup_time_key int,
    dropoff_time_key int,

    pickup_location_key int,
    dropoff_location_key int,

    payment_type_key int,
    vendor_key int,
    ratecode_key int,
    service_type_key text,

    passengers_count int,
    trip_distance numeric,
    fare_amount numeric,
    tip_amount numeric,
    tolls_amount numeric,
    total_amount numeric,
    trip_duration_minutes int
)
PARTITION BY RANGE (pickup_date);
