SELECT 
	p.resource ->> 'name' AS research
	, jsonb_extract_path_text(jsonb_path_query(a.resource, '$.code.coding[*] ? (@.system == "urn:CodeSystem:Nomenclature-medical-services")'), 'code') AS service_code
	, jsonb_extract_path_text(jsonb_path_query_first(a.resource, '$.code.coding[*] ? (@.system == "urn:CodeSystem:Nomenclature-medical-services")'), 'display') AS service_name
	, jsonb_extract_path_text(jsonb_path_query(o.resource, '$.code.coding[*] ? (@.system == "urn:CodeSystem:Laboratory-Research-and-Test")'), 'code') AS test_code
	, jsonb_extract_path_text(jsonb_path_query_first(o.resource, '$.code.coding[*] ? (@.system == "urn:CodeSystem:Laboratory-Research-and-Test")'), 'display') AS test_name
	, jsonb_extract_path_text(jsonb_path_query(o.resource, '$.method.coding[*] ? (@.system == "urn:CodeSystem:type-form-lab")'), 'display') AS test_result 
FROM plandefinition p 
LEFT JOIN activitydefinition a ON concat('ActivityDefinition/', a.id) = ANY (SELECT (jsonb_array_elements(p.resource -> 'action')) #>> '{definition,canonical}')
LEFT JOIN observationdefinition o ON o.id = ANY (SELECT (jsonb_array_elements(a.resource -> 'observationResultRequirement')) #>> '{id}')
WHERE p.resource #>> '{mainOrganization,id}' = '81d41979-06de-4f10-a901-db8029b2a671'
	AND p.resource @@ 'type.coding.#.code = "KDL"'::jsquery
