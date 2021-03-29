SELECT
	sr.id AS id,
	jsonb_path_query_first(sr.resource, '$.identifier[*]?(@.system == "urn:identity:Serial:ServiceRequest").value') AS query_code,
	sr.resource #> '{requester,display}' AS requester,
	sr.resource #> '{performer,0,display}' AS performer,
	sr.resource #> '{subject,display}' AS subject,
	jsonb_path_query_first(sr.resource, '$.performerInfo.requestActionHistory[*]?(@.action == "new").date') AS date,
	jsonb_path_query_first(p.resource, '$.identifier[*]?(@.system == "urn:identity:enp:Patient").value') AS enp,
	jsonb_path_query_first(p.resource, '$.identifier[*]?(@.system == "urn:identity:insurance-gov:Patient"||@.system == "urn:identity:insurance-gov-legacy:Patient" || @.system == "urn:identity:insurance-gov-temporary:Patient").assigner.display') AS smo,
	jsonb_path_query_first(sr.resource, '$.performerType.coding[*]?(@.system == "urn:CodeSystem:health-care-profiles").display') AS profile,
	sr.resource #> '{priority}' AS priority,
	concat((jsonb_path_query_first(sr.resource, '$.reasonCode[*] ? (exists (@.coding[*] ? (@.system == "urn:CodeSystem:icd-10"))).coding.code') #>> '{}'), ' ', (jsonb_path_query_first(sr.resource, '$.reasonCode[*] ? (exists (@.coding[*] ? (@.system == "urn:CodeSystem:icd-10"))).coding.display') #>> '{}')) AS diagnosis_tkp
--	(SELECT concat(resource #>> '{medicalReport,diagnosis,code}', ' ', resource #>> '{medicalReport,diagnosis,display}') 
--	FROM documentreference dr 
--	WHERE dr.resource @@ concat('context.related.#(resourceType = "ServiceRequest" and id = "', sr.id, '")')::jsquery
--	ORDER BY txid DESC
--	LIMIT 1) AS diagnosis_tkc
FROM
	ServiceRequest sr
INNER JOIN patient p ON
	(p.id = sr.resource #>> '{subject,id}'
	OR p.resource @@ logic_include(sr.resource, 'subject'))
WHERE
	(jsonb_path_exists(sr.resource, '$.category[*].coding[*] ? (@.system ==  "urn:CodeSystem:servicerequest-category" && @.code == "TMK")')
	AND jsonb_path_exists(sr.resource, '$.performerInfo.requestActionHistory[*]?(@.action == "completed")')
	AND ('2020-01-01T00:00' <= CAST(jsonb_path_query_first(sr.resource, '$.performerInfo.requestActionHistory[*]?(@.action == "completed").date') #>> '{}' AS date)
	AND CAST(jsonb_path_query_first(sr.resource, '$.performerInfo.requestActionHistory[*]?(@.action == "completed").date') #>> '{}' AS date) <= '2020-12-31T00:00'))
	
	