SELECT *
FROM integrationqueue i 
WHERE i.ts > '2020-12-22'
	AND i.resource @@ 'payload.identifier.#(system = "urn:identity:Serial:ServiceRequest" and value = "1221246")'::jsquery
--	AND i.resource #>> '{payload,identifier,1,value}' ILIKE '%CAP%'
	
SELECT *
FROM servicerequest sr
WHERE sr.resource @@ 'identifier.#(system = "urn:identity:Serial:ServiceRequest" and value = "1221179")'::jsquery
--	AND jsonb_path_query_first(sr.resource, '$.identifier ? (@.system == "urn:identity:Serial:ServiceRequest").value')::text = '1221179' -- ILIKE '%CAP%'
	
SELECT max(d.id),count(*)
FROM diagnosticreport d 
WHERE d.resource @@ 'identifier.#.system = "urn:source:nexmed:DiagnosticReport"'::jsquery
GROUP BY d.resource

SELECT count(*)
FROM diagnosticreport d 
WHERE d.resource #>> '{basedOn,0,id}' = 'e5646ae4-5a23-4483-a6b1-f7ddac4df7a7'
GROUP BY d.resource #- '{result}'
	
SELECT sr.id, s.id, dr.id, obs.id --, jsonb_agg(dr.resource -> 'code'), jsonb_agg(dr.resource -> 'status')
--	dr.*, dr.resource -> 'identifier',  dr.resource -> 'code'
FROM servicerequest sr
LEFT JOIN specimen s ON s.resource @@ logic_include(sr.resource, 'specimen[*]', NULL) OR s.id = any(array(SELECT jsonb_path_query(sr.resource, '$.specimen[*].id') #>> '{}'))
LEFT JOIN diagnosticreport dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'basedOn.*')
LEFT JOIN observation obs ON obs.resource @@ logic_include(dr.resource, 'result[*]', NULL) OR obs.id = any(array(SELECT jsonb_path_query(dr.resource, '$.result[*].id') #>> '{}'))
WHERE sr.resource @@ 'identifier.#(system = "urn:identity:Serial:ServiceRequest" and value = "1220000023")'::jsquery
	AND dr.resource @@ 'identifier.#.system = "urn:source:nexmed:DiagnosticReport"'::jsquery	
--GROUP BY dr.resource #- '{result}' 

SELECT dr.* , dr.resource -> 'identifier',  dr.resource #>> '{code,coding,0,code}'
FROM servicerequest sr
JOIN diagnosticreport dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'basedOn.*')
JOIN observation obs ON obs.resource @@ logic_include(dr.resource, 'result[*]', NULL) OR obs.id = any(array(SELECT jsonb_path_query(dr.resource, '$.result[*].id') #>> '{}'))
WHERE 
	sr.resource @@ 'identifier.#(system = "urn:identity:Serial:ServiceRequest" and value = "122208")'::jsquery 
	dr.resource @@ 'identifier.#.system = "urn:source:nexmed:DiagnosticReport"'::jsquery	
	AND dr.cts > current_date 

SELECT *
FROM observation o 
WHERE o.resource @@ 'identifier.#.system = "urn:source:nexmed:Observation"'::jsquery


-- fix identifiers
UPDATE servicerequest 
SET resource = jsonb_set(resource, '{identifier}', (
						SELECT jsonb_agg(identifiers.idf)
						FROM (
							SELECT jsonb_set(identifier, '{value}', concat('"', REPLACE(jsonb_extract_path_text(identifier, 'value'), 'CAP', ''), '"')::jsonb) idf
							FROM jsonb_array_elements(resource -> 'identifier') identifier
							WHERE identifier #>> '{system}' = 'urn:identity:Serial:ServiceRequest'
							UNION
							SELECT identifier idf
							FROM jsonb_array_elements(resource -> 'identifier') identifier
							WHERE NOT identifier #>> '{system}' = 'urn:identity:Serial:ServiceRequest') identifiers))
WHERE resource @@ 'identifier.#(system = "urn:identity:Serial:ServiceRequest")'::jsquery
	AND jsonb_path_query_first(resource, '$.identifier ? (@.system == "urn:identity:Serial:ServiceRequest").value')::text ILIKE '%CAP%'
RETURNING id

--удаление тестовых направлений
WITH resources_to_delete AS (
	SELECT sr.id sr_id, s.id s_id, dr.id dr_id, obs.id obs_id
	FROM servicerequest sr
	LEFT JOIN specimen s ON s.resource @@ logic_include(sr.resource, 'specimen[*]', NULL) OR s.id = any(array(SELECT jsonb_path_query(sr.resource, '$.specimen[*].id') #>> '{}'))
	LEFT JOIN diagnosticreport dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'basedOn.*')
	LEFT JOIN observation obs ON obs.resource @@ logic_include(dr.resource, 'result[*]', NULL) OR obs.id = any(array(SELECT jsonb_path_query(dr.resource, '$.result[*].id') #>> '{}'))
	WHERE sr.resource @@ 'identifier.#(system = "urn:identity:Serial:ServiceRequest" and value = "1220000023")'::jsquery
		AND dr.resource @@ 'identifier.#.system = "urn:source:nexmed:DiagnosticReport"'::jsquery	
)
, observation_to_delete AS (
	DELETE FROM observation
	WHERE id IN (SELECT DISTINCT obs_id FROM resources_to_delete)
	RETURNING id
)
, diagnosticreport_to_delete AS (
	DELETE FROM diagnosticreport
	WHERE id IN (SELECT DISTINCT dr_id FROM resources_to_delete)
	RETURNING id
)
, specimen_to_delete AS (
	DELETE FROM specimen
	WHERE id IN (SELECT DISTINCT s_id FROM resources_to_delete)
	RETURNING id
)
, servicerequest_to_delete AS (
	DELETE FROM servicerequest
	WHERE id IN (SELECT DISTINCT sr_id FROM resources_to_delete)
	RETURNING id
)
SELECT concat('observation:', id) FROM observation_to_delete
UNION
SELECT concat('diagnosticreport:', id) FROM diagnosticreport_to_delete
UNION
SELECT concat('specimen:', id) FROM specimen_to_delete
UNION
SELECT concat('servicerequest:', id) FROM servicerequest_to_delete;
