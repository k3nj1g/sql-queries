EXPLAIN ANALYZE
SELECT mo, web, reg, doctor, kc, other, (web + reg + kc + other + doctor) AS total
FROM (
	SELECT
	org.resource ->> 'name' AS mo,
	count(app.*) FILTER (
		WHERE app.resource -> 'authorOrganization' IS NULL
			AND app.resource @> '{"from":"web"}'::jsonb) AS web,
	count(app.*) FILTER (
		WHERE app.resource #>> '{mainOrganization,id}' = app.resource#>>'{authorOrganization,id}'
			AND app.resource @> '{"from":"reg"}'::jsonb) AS reg,
	count(app.*) FILTER (
		WHERE app.resource #>> '{mainOrganization,id}' = app.resource #>> '{authorOrganization,id}'
			AND app.resource @> '{"from":"doctor"}'::jsonb) AS doctor,
	count(app.*) FILTER (
		WHERE app.resource @> '{"from":"kc"}'::jsonb) AS kc,
	count(app.*) FILTER (
 		WHERE app.resource #>> '{mainOrganization,id}' != app.resource#>>'{authorOrganization,id}') AS other
	FROM appointment app
	JOIN practitionerrole prr ON prr.id = (jsonb_path_query_first(app.resource,	'$.participant[*] ? (@.actor.resourceType == "PractitionerRole").actor.id') #>> '{}')
		AND prr.resource #>> '{derived,morgid}' = '9d499f70-e3be-44bf-82e6-c8062069f38e'
	JOIN organization org ON org.id = prr.resource #>> '{derived, orgid}'
	WHERE app.resource ->> 'start' BETWEEN '2020-11-07' AND '2020-11-30T23:59:59'
		AND app.resource#>>'{status}' <> 'cancelled'
	GROUP BY org.id
) alias
		
SELECT *
FROM pg_catalog.pg_indexes 
WHERE tablename = 'appointment'
		
