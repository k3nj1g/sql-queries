SELECT d.id, jsonb_path_query(d.resource, '$.docEditingHistory.type()')
FROM documentreference d 
WHERE resource @@ 'docEditingHistory = *'::jsquery

SELECT *
FROM documentreference d 
WHERE d.id IN ('5acce4b8-54eb-4e15-8991-5592dbc15272')

WITH dr_with_history AS (
	SELECT id, jsonb_path_query(resource, '$.docEditingHistory.type()') jsonb_type
	FROM documentreference 
	WHERE resource @@ 'docEditingHistory = *'::jsquery
)
SELECT count(*)
FROM dr_with_history
WHERE jsonb_type = '"array"'::jsonb


SELECT count(*)
FROM documentreference 
WHERE resource @@ 'docEditingHistory = *'::jsquery

SELECT *
FROM pg_indexes
WHERE tablename = 'documentreference'

SELECT count(*)
	, date_part('year', knife_extract_min_timestamptz(resource, '[["date"]]'::jsonb)) AS y
FROM documentreference
GROUP BY y

UPDATE documentreference 
SET resource = jsonb_set(resource, '{docEditingHistory}', jsonb_build_array(resource -> 'docEditingHistory')) 
WHERE id = 'a5b8f8f5-884b-45a8-9ec7-9a0d94624426'

WITH dr_with_history AS (
	SELECT id, jsonb_path_query(resource, '$.docEditingHistory.type()') jsonb_type
	FROM documentreference 
	WHERE resource @@ 'docEditingHistory = *'::jsquery
),
broken_dr AS (
	SELECT id
	FROM dr_with_history
	WHERE jsonb_type = '"array"'::jsonb)
UPDATE documentreference dr 
SET resource = jsonb_set(dr.resource, '{docEditingHistory}', jsonb_build_array(dr.resource -> 'docEditingHistory')) 
FROM broken_dr
WHERE dr.id = broken_dr.id