SELECT *
FROM pg_indexes 
WHERE tablename='task_history';

CREATE INDEX CONCURRENTLY task_history_cts ON task_history (cts);

SELECT cts
FROM task_history
ORDER BY cts
LIMIT 10;

DO
$do$
BEGIN
  FOR i IN 0..31 LOOP
    RAISE NOTICE 'Calculate loop %', i;  
    DELETE FROM task_history
    WHERE cts < (('2021-08-01'::date + '0 days'::INTERVAL)::date + (i::text || ' days')::INTERVAL)
      AND cts < '2022-12-01';
  END LOOP;
END;
$do$;

VACUUM ANALYZE task_history;