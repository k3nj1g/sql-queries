EXPLAIN ANALYZE 
WITH ordered_sr AS
(
  SELECT *
  FROM organization AS o 
  JOIN LATERAL (
    SELECT *
    FROM servicerequest AS s
    WHERE (s.resource -> 'managingOrganization' @@ LOGIC_REVINCLUDE(o.resource,o.id))
      --AND ((resource @@ '"category".#."coding".#("code" = "Referral-IMI" OR ("code" = "Referral-Consultation" AND "system" = "urn:CodeSystem:servicerequest-category"))'::jsquery) AND (NOT (resource @@ '"supportingInfo".#."resourceType" = "Appointment"'::jsquery)))
      AND ((SELECT mkb
            FROM unnest(KNIFE_EXTRACT_TEXT (s.resource,'[["reasonCode","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) AS mkb
            WHERE mkb SIMILAR TO 'F%'
              OR mkb SIMILAR TO 'F1%'
              OR mkb SIMILAR TO '(B2[0-4])%'
              OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
            LIMIT 1) IS NULL)
      --AND jsonb_path_query_first(resource, '$.performer ? (@.resourceType == "Organization" || @.type == "Organization")') @@ logic_revinclude('{"identifier": [{"value": "1.2.643.5.1.13.13.12.2.21.1563","system": "urn:identity:oid:Organization"}]}', '1200d350-b334-42e4-a516-42df2c88d5f0')      
      AND jsonb_path_query_first(s.resource, '$.category.coding.code') #>> '{}' = 'Referral-LMI'       
      AND (s.resource #>> '{performerInfo,requestStatus}') = 'completed'
      --   AND (s.resource #> '{identifier}') @@ 'not #.system in ("urn:source:id:ServiceRequest","urn:source:rmis:ServiceRequest")'::jsquery
      --  AND (s.resource ->> 'authoredOn') >= '2021-07-16' 
      --  AND jsonb_path_query_first(s.resource, '$.performerInfo.requestActionHistory ? (@.action == "AIS-integration-rejected").action') #>> '{}' IS NOT NULL 
      --  AND s.resource #> '{performerInfo,requestActionHistory}' @@ 'not #.action="AIS-integration-in-progress"'::jsquery
--    AND (s.resource #> '{performerInfo,requestActionHistory}') @@ '#.action="history"'::jsquery
    ORDER BY (s.resource ->> 'authoredOn') DESC
    LIMIT 50) AS s ON TRUE
  WHERE (o.id = '81d41979-06de-4f10-a901-db8029b2a671')
)
SELECT *
--s.id,
--       s.ts,
--       s.cts
--       coalesce(jsonb_set(s.resource,'{subject,identifier}',jsonb_build_object('system','urn:identity:enp:Patient','value',coalesce((jsonb_path_query_first(p.resource,'$.identifier ? (@.system=="urn:identity:enp:Patient").value') #>> '{}'),(s.resource #>> '{subject,identifier,value}')))),s.resource) AS resource
FROM ordered_sr AS s
--  LEFT JOIN patient AS p
--         ON (p.resource @@ LOGIC_INCLUDE (s.resource,'subject'))
--         OR (p.id = ANY (ARRAY ( (SELECT (jsonb_path_query(s.resource,'$.subject.id') #>> '{}')))))

EXPLAIN ANALYZE 
WITH ordered_sr AS
(
  SELECT s.*
  FROM organization AS o 
  JOIN LATERAL (
    SELECT *
    FROM servicerequest s
    WHERE id IN (
      SELECT s.id
      FROM servicerequest AS s
      WHERE ((s.resource -> 'managingOrganization') @@ LOGIC_REVINCLUDE(o.resource,o.id))
        AND (s.resource @@ 'category.#.coding.#(code in ("Referral-IMI","Referral-LMI","Referral-Rehabilitation","Referral-Consultation","Referral-Hospitalization") and system="urn:CodeSystem:servicerequest-category")'::jsquery)
        AND ((jsonb_path_query_first(s.resource,'$.category.coding.code') #>> '{}') IN ('Referral-LMI'))  
        AND (s.resource #>> '{performerInfo,requestStatus}') = 'completed'
        AND jsonb_path_query_first(resource, '$.performer ? (@.resourceType == "Organization" || @.type == "Organization")') @@ logic_revinclude('{"identifier":[{"value":"213001001","system":"urn:identity:kpp:Organization"},{"value":"1022101282532","system":"urn:identity:ogrn:Organization"},{"value":"1.2.643.5.1.13.13.12.2.21.1521","system":"urn:identity:oid:Organization"},{"value":"1.2.643.5.1.13.3.25.21.26","system":"urn:identity:old-oid:Organization"},{"value":"6508238","system":"urn:identity:frmo-head:Organization"},{"value":"2129009282","system":"urn:identity:inn:Organization"},{"value":"f1a2e4ea-0436-11e8-b7c8-005056871882","system":"urn:source:1c:Organization"},{"value":"51e5d3b6-2c6d-4f8e-a812-853f67665064","system":"urn:source:rmis:Organization"}]}', '51e5d3b6-2c6d-4f8e-a812-853f67665064')
        AND resource ->> 'authoredOn' > '2022-03-13'
      ORDER BY (s.resource ->> 'authoredOn') DESC
      LIMIT 50)
    ORDER BY (s.resource ->> 'authoredOn') DESC
    LIMIT 50) AS s ON TRUE
  WHERE (o.id = '81d41979-06de-4f10-a901-db8029b2a671')
  AND   ((SELECT mkb
          FROM unnest(KNIFE_EXTRACT_TEXT (s.resource,'[["reasonCode","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) AS mkb
          WHERE mkb SIMILAR TO 'F%'
          OR    mkb SIMILAR TO 'F1%'
          OR    mkb SIMILAR TO '(B2[0-4])%'
          OR    mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
          OR    mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
          LIMIT 1) IS NULL)
  AND   ((s.resource #>> '{performerInfo,requestStatus}') = 'completed')
)
SELECT s.id,
       s.ts,
       s.cts,
       coalesce(jsonb_set(s.resource,'{subject,identifier}',jsonb_build_object('system','urn:identity:enp:Patient','value',coalesce((jsonb_path_query_first(p.resource,'$.identifier ? (@.system=="urn:identity:enp:Patient").value') #>> '{}'),(s.resource #>> '{subject,identifier,value}')))),s.resource) AS resource
FROM ordered_sr AS s
  LEFT JOIN patient AS p
         ON (p.resource @@ LOGIC_INCLUDE (s.resource,'subject'))
         OR (p.id = ANY(ARRAY((SELECT (jsonb_path_query(s.resource,'$.subject.id') #>> '{}')))))

SELECT *
FROM pg_indexes
WHERE tablename = 'servicerequest'         
         
CREATE INDEX servicerequest_numbered_filters 
  ON public.servicerequest USING gin(
    ((resource #>> '{authoredOn}'::text))
    , ((resource #>> '{performerInfo,requestStatus}'::text[]))
    , ((jsonb_path_query_first(resource, '$."category"."coding"."code"'::jsonpath) #>> '{}'::text[]))
    , ((jsonb_path_query_first(resource, '$."performer"?(@."resourceType" == "Organization")."id"'::jsonpath) #>> '{}'::text[]))
    , ((jsonb_path_query_first(resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-in-progress")."action"'::jsonpath) #>> '{}'::text[]))
    , ((jsonb_path_query_first(resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-rejected")."action"'::jsonpath) #>> '{}'::text[]))
    , ((jsonb_path_query_first(resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-cancelled")."action"'::jsonpath) #>> '{}'::text[]))
    , ((jsonb_path_query_first(resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-failed")."action"'::jsonpath) #>> '{}'::text[]))
    , jsonb_path_query_first(resource, '$."performer"?(@."resourceType" == "Organization" || @."type" == "Organization")'::jsonpath) jsonb_path_value_ops
    , ((resource -> 'managingOrganization'::text)) jsonb_path_value_ops)
WHERE(resource @@ '"category".#."coding".#("code" IN ("Referral-IMI", "Referral-LMI", "Referral-Rehabilitation", "Referral-Consultation", "Referral-Hospitalization") AND "system" = "urn:CodeSystem:servicerequest-category")'::jsquery)