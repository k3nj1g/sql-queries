--EXPLAIN ANALYZE 
SELECT sr.id AS id,
       jsonb_path_query_first(sr.resource,'$.identifier[*]?(@.system == "urn:identity:Serial:ServiceRequest").value') AS query_code,
       sr.resource #> '{requester,display}' AS requester,
       sr.resource #> '{performer,0,display}' AS performer,
       sr.resource #> '{subject,display}' AS subject,
       jsonb_path_query_first(sr.resource,'$.performerInfo.requestActionHistory[*]?(@.action == "new").date') AS date,
       jsonb_path_query_first(p.resource,'$.identifier[*]?(@.system == "urn:identity:enp:Patient").value') AS enp,
       jsonb_path_query_first(p.resource,'$.identifier[*]?(@.system == "urn:identity:snils:Patient").value') AS snils,
       jsonb_path_query_first(p.resource, '$.identifier[*]?                        (   @.system == "urn:identity:insurance-gov:Patient"                         || @.system == "urn:identity:insurance-gov-legacy:Patient"                         || @.system == "urn:identity:insurance-gov-temporary:Patient"                         ).assigner.display')  AS smo,
       jsonb_path_query_first(sr.resource,'$.performerType.coding[*]?(@.system == "urn:CodeSystem:oms.v002-care-profile").display') AS profile,
       sr.resource #> '{priority}' AS priority,
       dr.resource #> '{medicalReport,diagnosis}' AS performer_diagnosis,
       jsonb_path_query_first(sr.resource,'$.reasonCode[*].coding[*] ? (@.system == "urn:CodeSystem:icd-10")') AS requester_diagnosis
FROM serviceRequest sr
  INNER JOIN patient p
          ON (p.id = sr.resource #>> '{subject,id}'
          OR p.resource @@ logic_include (sr.resource, 'subject'))
  INNER JOIN documentreference dr
          ON ( 'current' = dr.resource #>> '{status}'
--         AND dr.resource @> '{"category":[{"coding":[{"system":"urn:CodeSystem:medrecord-group","code":"TMK-medical-report"}]}]}'
          AND dr.resource -> 'category' @@ '#.coding.#(system="urn:CodeSystem:medrecord-group" and code="TMK-medical-report")'::jsquery
         AND dr.resource -> 'context' @@ logic_revinclude (sr.resource, sr.id, 'related.#'))
WHERE (sr.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="TMK") and status="completed" and performerInfo.requestActionHistory.#(action="completed")'::jsquery 
    AND action_completed_date(sr.resource) between '2021-01-01' AND '2022-01-01')
--    AND sr.resource @@ 'performer.#(resourceType="Organization" and id="tkc\")'::jsquery

CREATE INDEX IF NOT EXISTS servicerequest_completed_date_tmk
  ON servicerequest ((action_completed_date(resource)))
  WHERE resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="TMK") and status="completed" and performerInfo.requestActionHistory.#(action="completed")'::jsquery;
  
VACUUM ANALYZE servicerequest;
  
CREATE OR REPLACE FUNCTION action_completed_date(resource jsonb)
 RETURNS date
 LANGUAGE sql
 IMMUTABLE
 AS $function$
   SELECT cast(jsonb_path_query_first(resource,'$.performerInfo.requestActionHistory[*]?(@.action == "completed").date') #>> '{}' AS date);
 $function$;
 
CREATE INDEX IF NOT EXISTS documentreference_resource__context_tmk__gin
  ON documentreference USING gin((resource -> 'context') jsonb_path_value_ops)
  WHERE resource -> 'category' @@ '#.coding.#(system="urn:CodeSystem:medrecord-group" and code="TMK-medical-report")'::jsquery;
 