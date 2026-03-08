DROP TABLE IF EXISTS analytics_gold.dim_service_type CASCADE;

--particiones por list

CREATE TABLE analytics_gold.dim_service_type (
    service_type_key text NOT NULL,
    service_type text
)
PARTITION BY LIST (service_type);

CREATE TABLE analytics_gold.dim_service_type_yellow
PARTITION OF analytics_gold.dim_service_type
FOR VALUES IN ('yellow');

CREATE TABLE analytics_gold.dim_service_type_green
PARTITION OF analytics_gold.dim_service_type
FOR VALUES IN ('green');