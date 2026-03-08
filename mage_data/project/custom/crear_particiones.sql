
DROP TABLE IF EXISTS analytics_gold.dim_zone CASCADE;

--particiones por hash

CREATE TABLE analytics_gold.dim_zone (
    zone_key int NOT NULL,
    zone_name text
)
PARTITION BY HASH (zone_key);

CREATE TABLE analytics_gold.dim_zone_p0
PARTITION OF analytics_gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE analytics_gold.dim_zone_p1
PARTITION OF analytics_gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE analytics_gold.dim_zone_p2
PARTITION OF analytics_gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE analytics_gold.dim_zone_p3
PARTITION OF analytics_gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 3);