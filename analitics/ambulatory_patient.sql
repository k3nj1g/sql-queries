SELECT *
FROM pg_indexes
WHERE tablename = 'patient';

CREATE TABLE "temp".servicerequest_ambulatory AS 
SELECT *
FROM public.servicerequest 
WHERE resource @@ 'orderDetail.#.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.99.2.322" and code="2") and status="completed"'::jsquery;

SELECT count(DISTINCT p.id)
FROM "temp".servicerequest_ambulatory sr
JOIN patient p ON (p.resource @@ LOGIC_INCLUDE(sr.resource, 'subject')) OR (p.id = ANY(ARRAY((SELECT (JSONB_PATH_QUERY(sr.resource, '$.subject.id') #>> '{}')))))
WHERE sr.resource ->> 'authoredOn' > '2023-01-01';

CREATE INDEX CONCURRENTLY encounter_temp_amb ON encounter
((resource #>>'{period,end}'))
WHERE resource #>>'{period,end}' IS NOT NULL AND resource #>>'{class,code}' = 'AMB'

CREATE TABLE "temp".encounter_ambulatory AS
SELECT jsonb_select_keys(resource, '{subject}')
FROM encounter 
WHERE resource #>>'{period,end}' IS NOT NULL AND resource #>>'{class,code}' = 'AMB' AND resource #>>'{period,end}' > '2023-01-01';

CREATE INDEX encounter_ambulatory_subject_temp ON "temp".encounter_ambulatory
  ((REFERENCE_VALUE("jsonb_select_keys",'subject')));

CREATE TABLE "temp".encounter_ambulatory_patients AS 
SELECT count(DISTINCT id)
FROM patient
JOIN "temp".encounter_ambulatory 
  ON REFERENCE_VALUE("jsonb_select_keys",'subject') = ANY (IDENTIFIER_REFERENCE_VALUE(id,resource))
--  ON (("jsonb_select_keys" #>> '{subject,identifier,system}') || ("jsonb_select_keys" #>> '{subject,identifier,system}')) = ANY(ARRAY(SELECT array_agg(concat(idf->>'system', idf->>'value'))  FROM jsonb_array_elements(resource->'identifier') idfs(idf) WHERE idf#>>'{period,end}' IS NULL));
  
SELECT * FROM "temp".encounter_ambulatory_patients;
  
  SELECT REFERENCE_VALUE("jsonb_select_keys",'subject'), (("jsonb_select_keys" #>> '{subject,identifier,system}') || ("jsonb_select_keys" #>> '{subject,identifier,system}'))
FROM "temp".encounter_ambulatory
JOIN patient p 
 ON ((p.resource @@ LOGIC_INCLUDE(sr.resource, 'subject')) 
 AND JSONB_PATH_EXISTS(p.resource, CAST(CONCAT('$.identifier ? (@.system == ', (sr.resource #> '{subject,identifier,system}'), ' && @.value == ', (sr.resource #> '{subject,identifier,value}'), ' && (!exists(@.period.end) || @.period.end.datetime() > \"', current_date, '\".datetime()))') AS jsonpath)))
 OR (p.id = ANY(ARRAY((SELECT (JSONB_PATH_QUERY(sr.resource, '$.subject.id') #>> '{}')))))
