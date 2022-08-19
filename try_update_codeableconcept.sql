SELECT *
FROM appointment
WHERE resource @@ 'serviceType.#.coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery
    AND resource ->> 'start' > '2021-10-01'
--LIMIT 10

    
--- trying AGGREGATE codeableconcept
SELECT id, jsonb_set(resource,'{type}'
    , (WITH hs_type AS 
           (SELECT value
            FROM jsonb_array_elements(resource -> 'type'))
       , codings AS             
           (SELECT jsonb_array_elements(value -> 'coding') v
            FROM hs_type)
       , updated_codings AS 
           (SELECT jsonb_build_object('coding', jsonb_agg(codings.v)) v
            FROM codings)
       , coding_array AS 
           (SELECT jsonb_agg
           FROM )
       SELECT jsonb_agg(updated_codings.v)
       FROM updated_codings))
FROM healthcareservice h
WHERE resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery

---
SELECT id, resource, jsonb_set(resource,'{type}'
    , COALESCE (
        (WITH hs_type AS 
           (SELECT value
            FROM jsonb_array_elements(resource -> 'type'))
         , codings AS 
             (SELECT jsonb_set(value, '{coding,0}', jsonb_build_object('code', '195', 'system', 'urn:CodeSystem:service', 'display', 'Профилактический прием (осмотр, консультация) врача-терапевта')) v
             FROM hs_type
             WHERE value @@ 'coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery
             UNION 
             SELECT *
             FROM hs_type
             WHERE NOT value @@ 'coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery)
         SELECT jsonb_agg(codings.v)
         FROM codings)
         , resource -> 'type'))
FROM healthcareservice h
WHERE resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="195")'::jsquery;

--- update hcs type
UPDATE healthcareservice
SET resource = jsonb_set(resource,'{type}'
    , COALESCE (
        (WITH hs_type AS 
           (SELECT value
            FROM jsonb_array_elements(resource -> 'type'))
         , codings AS 
             (SELECT jsonb_set(value, '{coding,0}', jsonb_build_object('code', '195', 'system', 'urn:CodeSystem:service', 'display', 'Профилактический прием (осмотр, консультация) врача-терапевта')) v
             FROM hs_type
             WHERE value @@ 'coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery
             UNION 
             SELECT *
             FROM hs_type
             WHERE NOT value @@ 'coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery)
         SELECT jsonb_agg(codings.v)
         FROM codings)
         , resource -> 'type'))
WHERE resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery
RETURNING id;

SELECT s.resource
    FROM schedulerule s 
    JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    WHERE h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="195")'::jsquery

    
--- update sch
WITH to_update AS (
    SELECT DISTINCT s.id
    FROM schedulerule s 
    JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    WHERE h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="195")'::jsquery
)
UPDATE schedulerule s
SET resource = jsonb_set(s.resource, '{healthcareService,0,display}', '"Профилактический прием (осмотр, консультация) врача-терапевта"')
FROM to_update tu
WHERE s.id = tu.id
RETURNING id;

