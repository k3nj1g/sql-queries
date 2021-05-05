--- first version
--EXPLAIN ANALYZE 
SELECT DISTINCT org.resource #>> '{alias,0}' AS org_name,
       p.id AS patient_id,
       p.resource -> 'name' AS patient_name,
       p.resource ->> 'birthDate' AS patient_bd,
       enc.resource AS encounter_resource,
       jsonb_path_query_first(obs.resource,'$.value.CodeableConcept.coding[*] ? (@.system=="urn:CodeSystem:1.2.643.5.1.13.13.11.1006").display') AS CONDITION
       , enc.resource -> 'hospitalDepartment'
FROM observation obs
  INNER JOIN encounter enc ON enc.resource @@ logic_include (obs.resource,'encounter')
  INNER JOIN organization org ON org.resource @@ logic_include (enc.resource,'serviceProvider')
  INNER JOIN patient p ON p.resource @@ logic_include (obs.resource,'subject')
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code in ("3","4","6"))'::jsquery 
	AND daterange(cast(obs.resource #>> '{effective,Period,start}' AS date),cast(obs.resource #>> '{effective,Period,end}' AS date),'[]') @> current_date);

--- second version with tmk
--EXPLAIN ANALYZE 
SELECT DISTINCT 
	org.resource #>> '{alias,0}' AS org_name
	, obs.id
	, p.id AS patient_id
	, p.resource -> 'name' AS patient_name
	, p.resource ->> 'birthDate' AS patient_bd
	, enc.resource AS encounter_resource
	, jsonb_path_query_first(obs.resource,'$.value.CodeableConcept.coding[*] ? (@.system=="urn:CodeSystem:1.2.643.5.1.13.13.11.1006").display') AS "condition"
	, enc.resource -> 'hospitalDepartment' department
	, jsonb_insert(to_jsonb(tmk), '{resource,task-status}', COALESCE (t.resource -> 'status', '""')) tmk_row
	, to_jsonb(ambulance) ambulance_row
FROM observation obs
INNER JOIN encounter enc ON enc.resource @@ logic_include (obs.resource,'encounter')
INNER JOIN organization org ON org.resource @@ logic_include (enc.resource,'serviceProvider')
INNER JOIN patient p ON p.resource @@ logic_include (obs.resource,'subject')
	LEFT JOIN servicerequest tmk ON tmk.resource -> 'subject' @@ logic_revinclude(p.resource, p.id)
	AND tmk.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "TMK")'::jsquery
	AND CAST(tmk.resource ->> 'authoredOn' AS timestamptz) BETWEEN (enc.resource #>> '{period,start}')::timestamptz AND COALESCE ((enc.resource #>> '{period,end}')::timestamptz, 'infinity')	
LEFT JOIN servicerequest ambulance ON ambulance.resource @@ logic_revinclude(tmk.resource, tmk.id, 'basedOn.#')
	AND ambulance.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "ambulance")'::jsquery
LEFT JOIN task t ON t.resource @@ logic_revinclude(ambulance.resource, ambulance.id, 'basedOn.#')
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code in ("3","4","6"))'::jsquery 
	AND daterange(cast(obs.resource #>> '{effective,Period,start}' AS date),cast(obs.resource #>> '{effective,Period,end}' AS date),'[]') @> current_date)
	AND NOT jsonb_extract_path_text(jsonb_path_query_first(enc.resource, '$.contained.code.coding ? (@.system == "urn:CodeSystem:icd-10")'), 'code') IN ('U07.1', 'U07.2')
	AND current_date < COALESCE ((enc.resource #>> '{period,end}')::timestamptz, 'infinity')
	AND (NOT (org.resource -> 'address')::TEXT ILIKE '%Чебоксары%' AND NOT (org.resource -> 'address')::TEXT ILIKE '%Новочебоксарск%');
	
	