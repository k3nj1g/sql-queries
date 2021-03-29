SELECT mo.resource #>> '{alias,0}' mo_from, mbe.mo_where #>> '{Reference,display}' mo_where
FROM (
	SELECT 
		jsonb_path_query_first(t.resource, '$.input[*] ? (exists (@.type.coding ? (@.system == "urn:CodeSystem:task-input-type" && @.code == "requestingOrganization"))).value') mo_from,
		jsonb_path_query_first(t.resource, '$.input[*] ? (exists (@.type.coding ? (@.system == "urn:CodeSystem:task-input-type" && @.code == "requestedOrganizationMain"))).value') mo_where
	FROM task t
	WHERE t.resource @@ 'code.coding.#(system = "urn:CodeSystem:chu-task-code" and code = "mbe")'::jsquery
		AND t.resource ->> 'status' = 'requested') mbe
JOIN organization o ON o.resource @@ concat('identifier.#(system="', mbe.mo_from #>> '{Reference,identifier,system}', '" and value="', mbe.mo_from #>> '{Reference,identifier,value}', '")')::jsquery		
JOIN organization mo ON mo.resource @@ logic_include(o.resource, 'mainOrganization')
