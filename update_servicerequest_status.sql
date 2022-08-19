EXPLAIN ANALYZE 
SELECT jsonb_agg(to_jsonb(s.*))
FROM servicerequest AS s
WHERE (s.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and not category.#.coding.#(system="urn:CodeSystem:lab-group" and code="601") and status="active" and performer.#.identifier(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1537")'::jsquery)
AND   ((s.resource ->> 'authoredOn') > '2022-04-07')
GROUP BY jsonb_path_query_first(s.resource,'$.identifier ? (@.system=="urn:identity:Serial:ServiceRequest").value')


SELECT jsonb_path_query_first(s.resource,'$.identifier ? (@.system=="urn:identity:Serial:ServiceRequest").value'), count(*)
FROM servicerequest AS s
WHERE (s.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and not category.#.coding.#(system="urn:CodeSystem:lab-group" and code="601") and status="active" and performer.#.identifier(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1537")'::jsquery)
  AND (cts > '2021-06-01')
GROUP BY jsonb_path_query_first(s.resource,'$.identifier ? (@.system=="urn:identity:Serial:ServiceRequest").value')

SELECT count(*)
FROM servicerequest AS s
WHERE (s.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and not category.#.coding.#(system="urn:CodeSystem:lab-group" and code="601") and status="active" and performer.#.identifier(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1537")'::jsquery)
  AND cts > '2022-04-07'

SELECT *
FROM servicerequest AS s
WHERE (s.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and not category.#.coding.#(system="urn:CodeSystem:lab-group" and code="601") and status="active" and performer.#.identifier(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1537")'::jsquery)
--  AND (cts > '2021-06-01')  
LIMIT 10

SELECT *
FROM pg_indexes
WHERE tablename = 'servicerequest'

WITH to_update AS (
  SELECT s.id id
  FROM servicerequest AS s
  WHERE (s.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") 
                        and performer.#.identifier(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1537") 
                        and performerInfo.requestStatus = completed
                        and not status = completed'::jsquery)
  LIMIT 10000)
UPDATE servicerequest s
SET resource = jsonb_set(s.resource, '{status}', '"completed"'::jsonb)
FROM to_update tu 
WHERE s.id = tu.id
--RETURNING *

WITH to_update AS (
  SELECT s.id id
  FROM servicerequest AS s
  WHERE (s.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") 
                        and performer.#.identifier(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1537") 
                        and status = active'::jsquery)
    AND cts < '2022-01-01'                      
  LIMIT 10000)
UPDATE servicerequest s
SET resource = jsonb_set(s.resource, '{status}', '"revoked"'::jsonb)
FROM to_update tu 
WHERE s.id = tu.id
RETURNING *


SELECT pg_cancel_backend(22345) 


SELECT jsonb_set(jsonb_build_object('foo', 'bar'), '{foo}', 'null')