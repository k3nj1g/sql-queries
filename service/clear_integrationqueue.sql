SELECT *
FROM integrationqueue_archive i 
ORDER BY ts DESC
LIMIT 1;

SELECT *
FROM "integrationqueue"
-- WHERE resource @@ 'payload.resourceType="Patient"'::jsquery
ORDER BY ts
LIMIT 1;

INSERT INTO integrationqueue_archive
(SELECT * FROM integrationqueue_y23w34 i WHERE resource @@ 'payload.resourceType="Patient"'::jsquery);

DROP TABLE integrationqueue_y23w34;

SELECT *
FROM integrationqueue_archive
ORDER BY ts desc 
LIMIT 1;

-- DO
-- $do$
-- BEGIN
--   FOR i IN 0..10 LOOP
--     RAISE NOTICE 'Calculate loop %', i;  
--     DELETE FROM integrationqueue
--     WHERE ts < '2023-01-01'
--       AND ts < (('2022-12-01'::date + '0 days'::INTERVAL)::date + (i::text || ' days')::INTERVAL);
--   END LOOP;
-- END;
-- $do$;

SELECT count(*)
FROM integrationqueue;

SELECT *
FROM pg_indexes 
WHERE tablename='appointment_history';

-- CREATE INDEX integrationqueue_archive_resource__gin_jsquery ON public.integrationqueue_archive USING gin (resource jsonb_path_value_ops);
-- CREATE INDEX integrationqueue_archive_ts ON public.integrationqueue_archive USING btree (ts);

SELECT (('2022-10-01'::date + '44 days'::INTERVAL)::date + (10::text || ' days')::INTERVAL);
  
VACUUM ANALYZE integrationqueue;


