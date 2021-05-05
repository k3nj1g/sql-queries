WITH last_updated AS (
	SELECT date_trunc('second', ts), count(*), jsonb_agg(resource) resource 
	FROM concept c 
	WHERE resource @@ 'system="urn:CodeSystem:smnn-eng"'::jsquery
	GROUP BY 1
	ORDER BY 1 DESC 
	LIMIT 1)
SELECT jsonb_array_elements(resource) 
FROM last_updated 