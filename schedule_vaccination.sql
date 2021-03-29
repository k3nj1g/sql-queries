SELECT
	org.id
	, org.resource #>> '{alias,0}' org_name
FROM
	schedulerule sch
JOIN healthcareservice hcs ON
	hcs.resource @@ logic_include(sch.resource, 'healthcareService')
	OR hcs.id = ANY(ARRAY(SELECT jsonb_path_query(sch.resource, '$.healthcareService.id') #>> '{}'))
	AND hcs.resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "4000")'::jsquery
JOIN organization org ON
	org.id = sch.resource #>> '{mainOrganization,id}'
WHERE
	(CAST(sch.resource #>> '{planningHorizon, end}' AS timestamp) > current_timestamp
	OR NOT sch.resource -> 'planningHorizon' ?? 'end')
GROUP BY
	org.id
	
WITH orgs AS 
	(SELECT org.id, jsonb_agg(jsonb_path_query_first(hcs.resource, '$.type[*].coding[*] ? (@.system == "urn:CodeSystem:service").code'))
	FROM schedulerule sch
	JOIN healthcareservice hcs ON
		hcs.id = sch.resource #>> '{healthcareService,0,id}'
	JOIN organization org ON org.id = sch.resource #>> '{mainOrganization,id}'
	WHERE CAST(sch.resource #>> '{planningHorizon, end}' AS timestamp) > current_timestamp OR NOT sch.resource -> 'planningHorizon' ?? 'end'
	GROUP BY org.id
	HAVING NOT jsonb_agg(jsonb_path_query_first(hcs.resource, '$.type[*].coding[*] ? (@.system == "urn:CodeSystem:service").code')) @> '"4000"')
SELECT o.id, o.resource #>> '{alias,0}' org_name
FROM orgs
JOIN organization o ON o.id = orgs.id
JOIN organizationinfo oi ON oi.id = orgs.id
WHERE oi.resource @@ 'flags.#(system = "urn:CodeSystem:mo-flags" and code = "13")'::jsquery
ORDER BY org_name	
	