SELECT
    s.sequence_schema AS schema,
    s.sequence_name AS name,
    s.data_type AS data_type,
    s.numeric_precision AS precision,
    s.numeric_precision_radix AS precision_radix,
    s.numeric_scale AS scale,
    s.start_value AS start_value,
    s.minimum_value AS minimum_value,
    s.maximum_value AS maximum_value,
    s.increment AS increment,
    s.cycle_option AS cycle_option
FROM information_schema.sequences s
-- INTERNAL 
WHERE sequence_schema NOT IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- INTERNAL 
AND sequence_schema NOT LIKE 'pg_temp_%' 
-- INTERNAL 
AND sequence_schema NOT LIKE 'pg_toast_temp_%'
ORDER BY 1, 2;
