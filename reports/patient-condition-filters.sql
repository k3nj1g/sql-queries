-- base report
SELECT DISTINCT org.resource #>> '{alias,0}' AS org_name,
       p.id AS patient_id,
       p.resource -> 'name' AS patient_name,
       p.resource ->> 'birthDate' AS patient_bd,
       enc.resource AS encounter_resource,
       jsonb_path_query_first(obs.resource,'$.value.CodeableConcept.coding[*] ? (@.system=="urn:CodeSystem:1.2.643.5.1.13.13.11.1006").display') AS condition,
       to_jsonb(tmk) AS tmk_row,
       jsonb_insert(to_jsonb(ambulance),'{resource,task-status}',coalesce(t.resource -> 'status','""')) AS ambulance_row
FROM observation obs
  INNER JOIN encounter enc ON enc.resource @@ logic_include (obs.resource,'encounter')
  INNER JOIN organization org ON (org.resource @@ logic_include (enc.resource,'serviceProvider'))
  INNER JOIN patient p ON p.resource @@ logic_include (obs.resource,'subject')
  LEFT JOIN servicerequest tmk
         ON (tmk.resource -> 'subject' @@ logic_revinclude (p.resource,p.id)
        AND tmk.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="TMK")'::jsquery
        AND cast (tmk.resource ->> 'authoredOn' AS timestamptz) BETWEEN cast (enc.resource #>> '{period,start}' AS timestamptz)
        AND coalesce (cast (enc.resource #>> '{period,end}' AS timestamptz),'infinity'))
  LEFT JOIN servicerequest ambulance
         ON (ambulance.resource @@ logic_revinclude (tmk.resource,tmk.id,'basedOn.#')
        AND ambulance.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="ambulance")'::jsquery)
  LEFT JOIN task t ON t.resource @@ logic_revinclude (ambulance.resource,ambulance.id,'basedOn.#')
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code in ("3","4","6"))'::jsquery 
    AND NOT (jsonb_path_query_first(enc.resource,'$.contained.code.coding ? (@.system=="urn:CodeSystem:icd-10").code') #>> '{}' IN ('U07.1','U07.2'))
    AND immutable_tsrange(obs.resource #>> '{effective,Period,start}',obs.resource #>> '{effective,Period,end}','[]') @> '2021-08-01'::timestamp 
    AND '2021-08-01'::timestamp<coalesce(cast(enc.resource #>> '{period,end}' AS date),'infinity'))
    
-- filter not-city
SELECT DISTINCT org.resource #>> '{alias,0}' AS org_name,
       p.id AS patient_id,
       p.resource -> 'name' AS patient_name,
       p.resource ->> 'birthDate' AS patient_bd,
       enc.resource AS encounter_resource,
       jsonb_path_query_first(obs.resource,'$.value.CodeableConcept.coding[*] ? (@.system=="urn:CodeSystem:1.2.643.5.1.13.13.11.1006").display') AS condition,
       to_jsonb(tmk) AS tmk_row,
       jsonb_insert(to_jsonb(ambulance),'{resource,task-status}',coalesce(t.resource -> 'status','""')) AS ambulance_row
FROM observation obs
  INNER JOIN encounter enc ON enc.resource @@ logic_include (obs.resource,'encounter')
  INNER JOIN organization org ON (org.resource @@ logic_include (enc.resource,'serviceProvider'))
  INNER JOIN patient p ON p.resource @@ logic_include (obs.resource,'subject')
  LEFT JOIN servicerequest tmk
         ON (tmk.resource -> 'subject' @@ logic_revinclude (p.resource,p.id)
        AND tmk.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="TMK")'::jsquery
        AND cast (tmk.resource ->> 'authoredOn' AS timestamptz) BETWEEN cast (enc.resource #>> '{period,start}' AS timestamptz)
        AND coalesce (cast (enc.resource #>> '{period,end}' AS timestamptz),'infinity'))
  LEFT JOIN servicerequest ambulance
         ON (ambulance.resource @@ logic_revinclude (tmk.resource,tmk.id,'basedOn.#')
        AND ambulance.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="ambulance")'::jsquery)
  LEFT JOIN task t ON t.resource @@ logic_revinclude (ambulance.resource,ambulance.id,'basedOn.#')
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code in ("3","4","6"))'::jsquery 
    AND NOT (jsonb_path_query_first(enc.resource,'$.contained.code.coding ? (@.system=="urn:CodeSystem:icd-10").code') #>> '{}' IN ('U07.1','U07.2'))
    AND immutable_tsrange(obs.resource #>> '{effective,Period,start}',obs.resource #>> '{effective,Period,end}','[]') @> '2021-08-01'::timestamp 
    AND '2021-08-01'::timestamp<coalesce(cast(enc.resource #>> '{period,end}' AS date),'infinity') 
    AND (NOT cast(org.resource -> 'address' AS text) ilike '%Чебоксары%' AND NOT cast(org.resource -> 'address' AS text) ilike '%Новочебоксарск%'))

--filter by condition
SELECT DISTINCT org.resource #>> '{alias,0}' AS org_name,
       p.id AS patient_id,
       p.resource -> 'name' AS patient_name,
       p.resource ->> 'birthDate' AS patient_bd,
       enc.resource AS encounter_resource,
       jsonb_path_query_first(obs.resource,'$.value.CodeableConcept.coding[*] ? (@.system=="urn:CodeSystem:1.2.643.5.1.13.13.11.1006").display') AS condition,
       to_jsonb(tmk) AS tmk_row,
       jsonb_insert(to_jsonb(ambulance),'{resource,task-status}',coalesce(t.resource -> 'status','""')) AS ambulance_row
FROM observation obs
  INNER JOIN encounter enc ON enc.resource @@ logic_include (obs.resource,'encounter')
  INNER JOIN organization org ON (org.resource @@ logic_include (enc.resource,'serviceProvider'))
  INNER JOIN patient p ON p.resource @@ logic_include (obs.resource,'subject')
  LEFT JOIN servicerequest tmk
         ON (tmk.resource -> 'subject' @@ logic_revinclude (p.resource,p.id)
        AND tmk.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="TMK")'::jsquery
        AND cast (tmk.resource ->> 'authoredOn' AS timestamptz) BETWEEN cast (enc.resource #>> '{period,start}' AS timestamptz)
        AND coalesce (cast (enc.resource #>> '{period,end}' AS timestamptz),'infinity'))
  LEFT JOIN servicerequest ambulance
         ON (ambulance.resource @@ logic_revinclude (tmk.resource,tmk.id,'basedOn.#')
        AND ambulance.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="ambulance")'::jsquery)
  LEFT JOIN task t ON t.resource @@ logic_revinclude (ambulance.resource,ambulance.id,'basedOn.#')
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code="3")'::jsquery 
    AND NOT (jsonb_path_query_first(enc.resource,'$.contained.code.coding ? (@.system=="urn:CodeSystem:icd-10").code') #>> '{}' IN ('U07.1','U07.2')) 
    AND immutable_tsrange(obs.resource #>> '{effective,Period,start}',obs.resource #>> '{effective,Period,end}','[]') @> '2021-08-01'::timestamp 
    AND '2021-08-01'::timestamp<coalesce(cast(enc.resource #>> '{period,end}' AS date),'infinity'))
    
-- filter by mo
SELECT DISTINCT org.resource #>> '{alias,0}' AS org_name,
       p.id AS patient_id,
       p.resource -> 'name' AS patient_name,
       p.resource ->> 'birthDate' AS patient_bd,
       enc.resource AS encounter_resource,
       jsonb_path_query_first(obs.resource,'$.value.CodeableConcept.coding[*] ? (@.system=="urn:CodeSystem:1.2.643.5.1.13.13.11.1006").display') AS condition,
       to_jsonb(tmk) AS tmk_row,
       jsonb_insert(to_jsonb(ambulance),'{resource,task-status}',coalesce(t.resource -> 'status','""')) AS ambulance_row
FROM observation obs
  INNER JOIN encounter enc ON enc.resource @@ logic_include (obs.resource,'encounter')
  INNER JOIN organization org
          ON (org.resource @@ logic_include (enc.resource,'serviceProvider')
         AND org.resource @@ 'identifier.#(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1537")'::jsquery)
  INNER JOIN patient p ON p.resource @@ logic_include (obs.resource,'subject')
  LEFT JOIN servicerequest tmk
         ON (tmk.resource -> 'subject' @@ logic_revinclude (p.resource,p.id)
        AND tmk.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="TMK")'::jsquery
        AND cast (tmk.resource ->> 'authoredOn' AS timestamptz) BETWEEN cast (enc.resource #>> '{period,start}' AS timestamptz)
        AND coalesce (cast (enc.resource #>> '{period,end}' AS timestamptz),'infinity'))
  LEFT JOIN servicerequest ambulance
         ON (ambulance.resource @@ logic_revinclude (tmk.resource,tmk.id,'basedOn.#')
        AND ambulance.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="ambulance")'::jsquery)
  LEFT JOIN task t ON t.resource @@ logic_revinclude (ambulance.resource,ambulance.id,'basedOn.#')
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code in ("3","4","6"))'::jsquery 
    AND NOT (jsonb_path_query_first(enc.resource,'$.contained.code.coding ? (@.system=="urn:CodeSystem:icd-10").code') #>> '{}' IN ('U07.1','U07.2')) 
    AND immutable_tsrange(obs.resource #>> '{effective,Period,start}',obs.resource #>> '{effective,Period,end}','[]') @> '2021-08-01'::timestamp 
    AND '2021-08-01'::timestamp<coalesce(cast(enc.resource #>> '{period,end}' AS date),'infinity'))