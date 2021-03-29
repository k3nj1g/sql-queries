WITH totals AS (
	SELECT
		a.resource #>> '{mainOrganization,id}' AS org_id,
		count(a.id) FILTER (
			WHERE a.resource ->> 'from' = 'doctor') AS doctor,
		count(a.id) FILTER (
			WHERE (a.resource ->> 'from' = 'reg'
				AND a.resource #>> '{mainOrganization,id}' = a.resource #>> '{authorOrganization,id}')) AS reg,
		count(a.id) FILTER (
			WHERE a.resource ->> 'from' = 'kc') AS kc,
		count(a.id) FILTER (
			WHERE a.resource ->> 'from' = 'web') AS web,
		count(a.id) FILTER (
			WHERE a.resource #>> '{mainOrganization,id}' <> a.resource #>> '{authorOrganization,id}') AS other
	FROM appointment a
	WHERE 
		knife_extract_min_timestamptz(a.resource, '[["start"]]') >= knife_date_bound('2020-01-01',	'min')
		AND knife_extract_min_timestamptz(a.resource, '[["start"]]') <= knife_date_bound('2020-12-01', 'max')
	GROUP BY a.resource #>> '{mainOrganization,id}'
)
SELECT
	o.resource #>> '{alias,0}' AS org
    , COALESCE(doctor, 0) AS doctor
	, COALESCE(reg, 0) AS reg
	, COALESCE(kc, 0) AS doctor
	, COALESCE(web, 0) AS doctor
	, COALESCE(other, 0) AS other
	, COALESCE((doctor + reg + kc + web + other), 0) AS total
FROM organization o
LEFT JOIN totals ON org_id = o.id
WHERE 
	(o.resource #>> '{partOf}' IS NULL
	AND COALESCE(o.resource ->> 'active', 'true') = 'true')
	
SELECT
	org,
	doctor,
	reg,
	kc,
	web,
	other,
--	totals,
	(doctor + reg + kc + web + other) AS total
FROM
	(
	SELECT
		o.resource #>> '{alias,0}' AS org,
		count(a.id) FILTER (
		WHERE (a.resource @@ 'from = doctor'::jsquery)) AS doctor,
		count(a.id) FILTER (
		WHERE (a.resource @@ 'from = reg'::jsquery)
		AND (a.resource#>>'{mainOrganization, id}' = a.resource#>>'{authorOrganization, id}')) AS reg,
		count(a.id) FILTER (
		WHERE (a.resource @@'from = kc'::jsquery)) AS kc,
		count(a.id) FILTER (
		WHERE (a.resource @@'from = web'::jsquery)) AS web,
		count(a.id) FILTER (
		WHERE a.resource#>>'{mainOrganization, id}' <> a.resource#>>'{authorOrganization, id}') AS other
	FROM
		organization o
	LEFT JOIN appointment a ON a.resource#>>'{mainOrganization, id}' = o.id
		AND knife_extract_min_timestamptz(a.resource, '[["start"]]') >= knife_date_bound('2020-01-01', 'min')
		AND knife_extract_min_timestamptz(a.resource, '[["start"]]') <= knife_date_bound('2020-12-01', 'max')
	WHERE o.resource #>> '{partOf}' IS NULL
		AND (COALESCE (o.resource->>'active', 'true')) = 'true'
	GROUP BY
		o.id) totals		

SELECT *, pg_size_pretty(pg_relation_size(indexname::text))
FROM pg_indexes
WHERE tablename = 'organization'
