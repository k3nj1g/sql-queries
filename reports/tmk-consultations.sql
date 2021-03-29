set datestyle TO 'dmy';
SELECT
--	sr.id AS id,
	jsonb_path_query_first(sr.resource,	'$.identifier[*]?(@.system == "urn:identity:Serial:ServiceRequest").value') AS query_code,
	sr.resource #> '{requester,display}' AS requester,
	sr.resource #> '{performer,0,display}' AS performer,
	sr.resource #> '{subject,display}' AS subject,
--	p.id,
	jsonb_path_query_first(p.resource, '$.identifier[*] ? (@.system == "urn:identity:enp:Patient").value') AS enp,
	(SELECT ids.i_assigner
		FROM (
			SELECT jsonb_array_elements(p.resource#>'{identifier}') ->> 'system' AS i_system
		      		, jsonb_array_elements(p.resource#>'{identifier}') #>> '{assigner,display}' AS i_assigner
					, jsonb_array_elements(p.resource#>'{identifier}') -> 'period' AS i_period
			FROM (
				SELECT *
				FROM patient p
				WHERE p.id = 'dd86a366-1d03-4af2-8c2c-51f7cda23902'	
				) p
			) ids
		WHERE ids.i_system = 'urn:identity:insurance-gov:Patient'
			AND ids.i_period ->> 'end' IS NULL) AS smo,
	--	jsonb_path_query_first(p.resource, '$.identifier[*] ? (exists (@ ? (@.system == "urn:identity:insurance-gov:Patient" && @.period.end != null))).assigner.display') AS smo,
(	jsonb_path_query_first(sr.resource,
	'$.performerInfo.requestActionHistory[*]?(@.action == "new").date')#>> '{}')::timestamp AS date,
	jsonb_path_query_first(sr.resource,
	'$.performerType.coding[*]?(@.system == "urn:CodeSystem:health-care-profiles").display') AS profile,
	sr.resource #> '{priority}' AS priority
FROM
	ServiceRequest sr
JOIN patient p ON p.id = sr.resource #>> '{subject,id}'
WHERE
	(jsonb_path_exists(sr.resource,
	'$.category[*].coding[*] ? (@.system ==  "urn:CodeSystem:servicerequest-category" && @.code == "TMK")')
	AND jsonb_path_exists(sr.resource,
	'$.performerInfo.requestActionHistory[*]?(@.action == "completed")')
	AND ('2020-01-01' <= CAST(jsonb_path_query_first(sr.resource, '$.performerInfo.requestActionHistory[*]?(@.action == "completed").date') #>> '{}' AS date)
	AND CAST(jsonb_path_query_first(sr.resource, '$.performerInfo.requestActionHistory[*]?(@.action == "completed").date') #>> '{}' AS date) <= '2020-12-11')
	AND sr.resource @> '{"performer":[{"resourceType":"Organization","id":"ff0f409e-ce00-4707-9e44-d8e493cde996"}]}')
	
SELECT jsonb_path_query_first(p.resource, '$.identifier[*] ? (exists (@ ? (@.system == "urn:identity:insurance-gov:Patient" && @.period.end is unknown)))') AS smo
FROM patient p
WHERE p.id = 'dd86a366-1d03-4af2-8c2c-51f7cda23902'

SELECT ids.i_assigner
FROM (
	SELECT jsonb_array_elements(p.resource#>'{identifier}') ->> 'system' AS i_system
      		, jsonb_array_elements(p.resource#>'{identifier}') #>> '{assigner,display}' AS i_assigner
			, jsonb_array_elements(p.resource#>'{identifier}') -> 'period' AS i_period
	FROM (
		SELECT *
		FROM patient p
		WHERE p.id = 'dd86a366-1d03-4af2-8c2c-51f7cda23902'	
		) p
	) ids
WHERE ids.i_system = 'urn:identity:insurance-gov:Patient'
	AND ids.i_period ->> 'end' IS NULL 	

SELECT
	ids #>> '{system}' AS SYSTEM,
	ids #>> '{value}' AS value
FROM
	(
	SELECT
		*
	FROM
		patient
	WHERE
		id = ?) p,
	jsonb_array_elements(p.resource#>'{identifier}') ids
