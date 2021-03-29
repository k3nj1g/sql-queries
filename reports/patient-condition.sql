SELECT org.resource #>> '{alias,0}' AS org_name,
       p.id AS patient_id,
       p.resource -> 'name' AS patient_name,
       p.resource ->> 'birthDate' AS patient_bd,
       enc.resource AS encounter_resource,
       jsonb_path_query_first(obs.resource,'$.value.CodeableConcept.coding[*] ? (@.system=="urn:CodeSystem:1.2.643.5.1.13.13.11.1006").display') AS condition
FROM observation obs
  INNER JOIN encounter enc ON enc.resource @@ logic_include (obs.resource,'encounter')
  INNER JOIN organization org ON org.resource @@ logic_include (enc.resource,'serviceProvider')
  INNER JOIN patient p ON p.resource @@ logic_include (obs.resource,'subject')
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code in ("3","4","6"))'::jsquery 
	AND daterange(cast(obs.resource #>> '{effective,Period,start}' AS date),cast(obs.resource #>> '{effective,Period,end}' AS date),'[]') @> cast('2021-03-22' AS date))