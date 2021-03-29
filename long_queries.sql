--SELECT pg_cancel_backend(PID);
SELECT sum(numbackends) as openned_connections FROM pg_stat_database;

SELECT count(pid) as count_of_long_queries 
FROM pg_stat_activity 
WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' and "state" = 'active'
and query_start < (now() - interval '1 minute');

SELECT pid, age(clock_timestamp(), query_start), query_start , query 
FROM pg_stat_activity 
WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' and "state" = 'active'
and query_start < (now() - interval '1 minute')
ORDER BY query_start  nulls last;

SELECT pg_cancel_backend(pid) 
FROM pg_stat_activity 
WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' and "state" = 'active'
	and query_start < (now() - interval '1 minute');

