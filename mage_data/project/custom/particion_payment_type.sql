DROP TABLE IF EXISTS analytics_gold.dim_payment_type CASCADE;

--particiones por list (solo con card y cash)

CREATE TABLE analytics_gold.dim_payment_type (
    payment_type_key int NOT NULL,
    payment_type text,
    payment_category text
)
PARTITION BY LIST (payment_category);


CREATE TABLE analytics_gold.dim_payment_type_card
PARTITION OF analytics_gold.dim_payment_type
FOR VALUES IN ('card');

CREATE TABLE analytics_gold.dim_payment_type_cash
PARTITION OF analytics_gold.dim_payment_type
FOR VALUES IN ('cash');

CREATE TABLE analytics_gold.dim_payment_type_other
PARTITION OF analytics_gold.dim_payment_type
FOR VALUES IN ('other');