SELECT * FROM pg_stat_statements ORDER BY total_time DESC;

SELECT
	substring(query, 1, 50) AS short_query
	, round(total_time::NUMERIC, 2) AS total_time
	, calls
	, round(mean_time::NUMERIC, 2) AS mean
	, round((100 * total_time / sum(total_time::NUMERIC) OVER ())::NUMERIC, 2) AS percentage_cpu
FROM
	pg_stat_statements
ORDER BY
	total_time DESC
LIMIT 20;