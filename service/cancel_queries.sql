SELECT application_name, pid, age(clock_timestamp(), query_start), query_start , wait_event, query 
FROM pg_stat_activity 
WHERE query != '<IDLE>' 
  AND query NOT ILIKE '%pg_stat_activity%' 
  and "state" != 'idle' 
  and query_start < (now() - interval '0.1 minute')
  AND NOT query LIKE 'START_REPLICATION%'
ORDER BY query_start nulls last;

SELECT pid, age(clock_timestamp(), query_start), query_start , wait_event, query, application_name, state
FROM pg_stat_activity
WHERE query NOT ILIKE '%pg_stat_activity%'
  and not "state" = 'idle' 
--  and "state" = 'idle in transaction' 
--  and query_start < (now() - interval '0.1 minute') 
--  AND NOT query LIKE 'START_REPLICATION%'
  ORDER BY 2 DESC  

SELECT pg_cancel_backend(21118);
SELECT pg_terminate_backend(4838);

SELECT
  query_start AS "time",
  count(*)
FROM pg_stat_activity
WHERE
  backend_type = 'client backend' AND
  wait_event = 'Lock'
GROUP BY 1
ORDER BY 1

SELECT pg_cancel_backend(pid)
FROM pg_stat_activity
WHERE query_start < (now() - interval '0.5 minute')
  AND pid <> pg_backend_pid()
  AND (query LIKE 'SELECT sr.* FROM servicerequest sr WHERE ((knife_extract_text(sr.resource, $$[["category",{"coding":[{"system":"urn:CodeSystem:servicerequest-category"}]},"coding",{"system":"urn:CodeSystem:servicerequest-category"},"code"]]$$))[1]%'
       OR query LIKE 'WITH archived AS (INSERT INTO "%'
--       OR query LIKE 'insert into _cache_bigint (id, value)%'
--       OR query LIKE 'SELECT "documentreference".* FROM "documentreference" WHERE "documentreference".resource @> $1 ORDER BY "documentreference".cts DESC LIMIT $2 OFFSET $3 '
--       OR query LIKE 'WITH task_ids AS (SELECT sd.id FROM rmis.semd_document AS sd WHERE%'
--       OR query LIKE 'SELECT COUNT(t.*) AS total FROM task AS t WHERE (((knife_extract_text%'      
--       OR query LIKE 'SELECT t.* FROM task AS t WHERE ((jsonb_path_query_array(t.resource, ''$.input[*] ? (@.type.coding.code == "signature")''))%'
--       OR query LIKE 'WITH archived AS (INSERT INTO "specimen_history"%'
--       OR query LIKE 'SELECT t.* FROM task AS t WHERE (((knife_extract_text(t%'
--       OR query LIKE 'SELECT "id" FROM "appointment"%'
--       OR query LIKE 'SELECT TO_JSONB(o.*) AS observation, TO_JSONB(c.*) AS concept%'
--       OR query LIKE 'WITH archived AS (INSERT INTO "specimen_history"%'
);

--SELECT pg_cancel_backend(pid)
--FROM pg_stat_activity
--WHERE query_start <(now() -interval '10 minute');
--
--SELECT pg_cancel_backend(pid)
--FROM pg_stat_activity
--WHERE query_start <(now() -interval '0.1 minute')
--AND   pid <> pg_backend_pid()
--AND   query LIKE 'WITH inserted AS (INSERT INTO%';

select command, blocks_done, blocks_total, concat(round(((blocks_done::float / blocks_total::float) * 100)::numeric, 2), '%') as progress
from pg_stat_progress_create_index;

SELECT pid,
       pg_blocking_pids(pid),
       query
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND wait_event_type = 'Lock'
ORDER BY pid
              
SELECT pid, age(clock_timestamp(), query_start), query_start , wait_event, query, state
FROM pg_stat_activity
--WHERE "state" = 'idle in transaction'
WHERE pid in (8096)

SELECT *
FROM pg_indexes
WHERE tablename = 'servicerequest';

SELECT 
  (total_time / 1000 / 60) as total, 
  (total_time/calls) as avg, 
  calls,
  query 
FROM pg_stat_statements 
ORDER BY 1 DESC 
LIMIT 100;

SELECT pid, age(clock_timestamp(), query_start), query_start , wait_event, query 
FROM pg_stat_activity 
WHERE query != '<IDLE>' 
  AND query NOT ILIKE '%pg_stat_activity%' 
  and "state" != 'idle' 
  --and query_start < (now() - interval '0.1 minute') 
  --AND NOT query LIKE 'START_REPLICATION%'
ORDER BY query_start nulls last;
