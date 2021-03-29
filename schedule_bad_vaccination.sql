WITH orgs AS 
	(SELECT org.id
	FROM schedulerule sch
	JOIN healthcareservice hcs ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
	JOIN organization org ON org.id = sch.resource #>> '{organization,id}'
	WHERE immutable_ts(COALESCE ((sch.resource #>> '{planningHorizon,end}'), 'infinity')) >= LOCALTIMESTAMP
	--CAST(sch.resource #>> '{planningHorizon, end}' AS timestamp) > current_timestamp OR sch.resource @@ 'not planningHorizon.end = *'::jsquery
		AND (
			(hcs.resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "4000")'::jsquery AND sch.resource @@ 'not availableTime.#.channel = web'::jsquery)
			OR
			(hcs.resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and not code = "4000")'::jsquery)
			OR
			(hcs.resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "4000")'::jsquery AND sch.resource @@ 'actor.#.resourceType = "PractitionerRole"'::jsquery)
		)
	GROUP BY org.id)
SELECT COALESCE (mo.resource #>> '{alias,0}', mo.resource ->> 'name') main_org, o.resource ->> 'name' org_name, jsonb_extract_path_text(jsonb_path_query_first(o.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization")'), 'value') "oid"
FROM orgs
JOIN organization o ON o.id = orgs.id	
JOIN organization mo ON mo.resource @@ logic_include(o.resource, 'mainOrganization') OR mo.id = any(array(SELECT jsonb_path_query(o.resource, '$.mainOrganization.id') #>> '{}'))
JOIN organizationinfo oi ON oi.id = mo.id AND oi.resource @@ 'flags.#(system = "urn:CodeSystem:mo-flags" and code = "13")'::jsquery
WHERE o.resource @@ 'identifier.#.system = "urn:identity:oid:Organization"'::jsquery
ORDER BY mo.resource ->> 'name'