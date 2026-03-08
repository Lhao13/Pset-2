DO $$
DECLARE
    start_date date := '2022-01-01';
    end_date date := '2026-01-01';
    partition_start date;
    partition_end date;
    partition_name text;
BEGIN
    partition_start := start_date;

    WHILE partition_start < end_date LOOP
        
        partition_end := (partition_start + INTERVAL '1 month')::date;

        partition_name := format(
            'analytics_gold.fct_trips_%s',
            to_char(partition_start, 'YYYY_MM')
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %s PARTITION OF analytics_gold.fct_trips
             FOR VALUES FROM (%L) TO (%L);',
            partition_name,
            partition_start,
            partition_end
        );

        partition_start := partition_end;

    END LOOP;
END $$;
