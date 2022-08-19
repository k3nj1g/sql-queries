EXPLAIN ANALYZE 
WITH  
ordered_sr AS (
  SELECT s.*
  FROM organization AS o
    JOIN LATERAL (SELECT * 
                  FROM servicerequest AS sr
                  WHERE 
                    resource -> 'managingOrganization' @@ LOGIC_REVINCLUDE(o.resource,o.id)
--                    jsonb_path_query_first(resource, '$.managingOrganization') @@ LOGIC_REVINCLUDE(o.resource,o.id) 
--                    AND (jsonb_path_query_first(resource, '$.performer ? (@.resourceType == "Organization" || @.type == "Organization")')                  
--(sr.resource -> 'managingOrganization') @@ LOGIC_REVINCLUDE(o.resource,o.id)
                    AND jsonb_path_query_first(resource, '$.performer ? (@.resourceType == "Organization" || @.type == "Organization")') @@ logic_revinclude('{"identifier": [{"value": "1.2.643.5.1.13.13.12.2.21.1563","system": "urn:identity:oid:Organization"}]}', '1200d350-b334-42e4-a516-42df2c88d5f0')
                    AND (sr.resource @@ 'category.#.coding.#(code in ("Referral-IMI","Referral-LMI","Referral-Rehabilitation","Referral-Consultation","Referral-Hospitalization") and system="urn:CodeSystem:servicerequest-category")'::jsquery)
                  ORDER BY (sr.resource ->> 'authoredOn') DESC) AS s
    ON TRUE
  WHERE o.id = '81d41979-06de-4f10-a901-db8029b2a671'
--    AND (po.resource @@ LOGIC_INCLUDE(s.resource, 'performer')
--          OR (po.id = ANY (ARRAY((SELECT (jsonb_path_query(s.resource,'$.performer.id') #>> '{}'))))))
    AND ((SELECT mkb
          FROM unnest(KNIFE_EXTRACT_TEXT (s.resource,'[["reasonCode","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) AS mkb
          WHERE mkb SIMILAR TO 'F%'
          OR    mkb SIMILAR TO 'F1%'
          OR    mkb SIMILAR TO '(B2[0-4])%'
          OR    mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
          OR    mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
          LIMIT 1) IS NULL)
--  AND jsonb_path_query_first(s.resource, '$.category.coding.code') #>> '{}' = 'Referral-LMI'       
--   AND (s.resource #>> '{performerInfo,requestStatus}') = 'completed'
--   AND (s.resource #> '{identifier}') @@ 'not #.system in ("urn:source:id:ServiceRequest","urn:source:rmis:ServiceRequest")'::jsquery
--  AND (s.resource ->> 'authoredOn') >= '2021-07-16' 
--  AND jsonb_path_query_first(s.resource, '$.performerInfo.requestActionHistory ? (@.action == "AIS-integration-rejected").action') #>> '{}' IS NOT NULL 
--  AND s.resource #> '{performerInfo,requestActionHistory}' @@ 'not #.action="AIS-integration-in-progress"'::jsquery
  AND (s.resource #> '{identifier}') @@ '#.system="urn:source:rmis:ServiceRequest"'::jsquery
--    AND (s.resource #> '{performerInfo,requestActionHistory}') @@ '#.action="history"'::jsquery          
)
SELECT s.id,
       s.ts,
       s.cts,
       coalesce(jsonb_set(s.resource,'{subject,identifier}',coalesce(jsonb_build_object('system','urn:identity:enp:Patient','value',jsonb_path_query_first(p.resource,'$.identifier[*] ? (@.system=="urn:identity:enp:Patient").value')),(s.resource #> '{subject,identifier}'))),s.resource) AS resource
FROM ordered_sr AS s
LEFT JOIN patient AS p
  ON (p.resource @@ LOGIC_INCLUDE (s.resource,'subject'))
    OR (p.id = ANY (ARRAY ( (SELECT (jsonb_path_query(s.resource,'$.subject.id') #>> '{}')))))
--WHERE  ((IMMUTABLE_TSVECTOR(AIDBOX_TEXT_SEARCH(KNIFE_EXTRACT_TEXT(s.resource,'[["subject","display"],["subject","identifier","value"]]'))) @@ cast('à:*' AS tsquery)))
LIMIT 50
 
DROP INDEX sr_test;
DROP INDEX servicerequest_numbered_filters;


CREATE INDEX IF NOT EXISTS servicerequest_numbered_filters 
  ON servicerequest ((resource ->> 'authoredOn')
                   , (resource #>> '{performerInfo,requestStatus}')
                   , (jsonb_path_query_first(resource, '$.category.coding.code') #>> '{}')
                   , (jsonb_path_query_first(resource, '$.performer ? (@.resourceType == "Organization").id') #>> '{}')
                   , (jsonb_path_query_first(resource, '$.performerInfo.requestActionHistory ? (@.action == "AIS-integration-in-progress").action') #>> '{}')
                   , (jsonb_path_query_first(resource, '$.performerInfo.requestActionHistory ? (@.action == "AIS-integration-rejected").action') #>> '{}')
                   , (jsonb_path_query_first(resource, '$.performerInfo.requestActionHistory ? (@.action == "AIS-integration-cancelled").action') #>> '{}')
                   , (jsonb_path_query_first(resource, '$.performerInfo.requestActionHistory ? (@.action == "AIS-integration-failed").action') #>> '{}'))
  WHERE(resource @@ '"category".#."coding".#("code" IN ("Referral-IMI", "Referral-LMI", "Referral-Rehabilitation", "Referral-Consultation", "Referral-Hospitalization") AND "system" = "urn:CodeSystem:servicerequest-category")'::jsquery)
