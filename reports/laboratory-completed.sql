CREATE INDEX CONCURRENTLY IF NOT EXISTS servicerequest_for_researches 
  ON servicerequest USING gin (
    (resource ->> 'authoredOn')
    , (resource ->> 'status')
    , (jsonb_path_query_first(resource, '$.category.coding ? (@.system=="urn:CodeSystem:servicerequest-category").code') #>> '{}')
    , (resource -> 'instantiatesCanonical') jsonb_path_value_ops
    , jsonb_path_query_first(resource, '$.performer ? (@.resourceType == "PractitionerRole" || @.type == "PractitionerRole").id')
    , jsonb_path_query_first(resource, '$.performer ? (@.resourceType == "Organization" || @.type == "Organization")') jsonb_path_value_ops     
)
    
DROP INDEX servicerequest_for_researches;

CREATE INDEX CONCURRENTLY IF NOT EXISTS diagnosticreport_basedon__gin 
  ON diagnosticreport USING gin((resource -> 'basedOn') jsonb_path_value_ops)

SELECT *
FROM pg_indexes
WHERE tablename = 'servicerequest'

VACUUM ANALYZE servicerequest;

EXPLAIN ANALYZE
WITH main_org AS (
    SELECT *
    FROM organization
    WHERE id = '81d41979-06de-4f10-a901-db8029b2a671'
)
SELECT *
FROM servicerequest sr
CROSS JOIN main_org mo
JOIN diagnosticreport AS dr
  ON (dr.resource -> 'basedOn' @@ LOGIC_REVINCLUDE (sr.resource,sr.id,'#'))
    AND CAST ((dr.resource #>> '{effective,dateTime}') AS timestamp)  BETWEEN '2022-03-21T00:00:00' AND '2022-04-01T00:00:00'
WHERE (jsonb_path_query_first(sr.resource, '$.category.coding ? (@.system=="urn:CodeSystem:servicerequest-category").code') #>> '{}') = 'Referral-LMI'
  AND sr.resource ->> 'authoredOn' BETWEEN '2022-02-21' AND '2022-04-01'
  AND sr.resource ->> 'status' = 'completed'
  AND jsonb_path_query_first(sr.resource, '$.performer ? (@.resourceType == "Organization" || @.type == "Organization")') @@ logic_revinclude(mo.resource, mo.id)
--  AND jsonb_path_query_first(sr.resource, '$.performer ? (@.resourceType == "Organization" || @.type == "Organization")') @@ logic_revinclude('{"identifier":[{"system": "urn:identity:oid:Organization", "value": "1.2.643.5.1.13.13.12.2.21.1558"}]}','81d41979-06de-4f10-a901-db8029b2a671')
  AND ((sr.resource -> 'instantiatesCanonical') @@ '#="PlanDefinition/b841d7ac-659b-4d9a-be36-474378adce37"'::jsquery)
  
EXPLAIN ANALYZE 
WITH main_org AS
(
  SELECT *
  FROM organization
  WHERE id = '1150e915-f639-4234-a795-1767e0a0be5f'
)
SELECT jsonb_path_query_first(sr.resource,'$.identifier ? (@.system=="urn:identity:Serial:ServiceRequest").value') AS serialNumber,
       (sr.resource #>> '{subject,display}') AS fio,
       (sr.resource #> '{reasonCode,0,coding,0}') AS reasoncode,
       (sr.resource #>> '{managingOrganization,display}') AS managingOrganization,
       jsonb_path_query_first(sr.resource,'$.performer ? (@.type=="PractitionerRole" || @.resourceType=="PractitionerRole").display') AS performerPractitionerRole,
       jsonb_path_query_first(sr.resource,'$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services")') AS code,
       cast((sr.resource ->> 'authoredOn') AS timestamp) AS authoredOn,
       cast((dr.resource #>> '{effective,dateTime}') AS timestamp) AS effective,
       (sr.resource #>> '{paymentType,display}') AS payment,
       obs.valueString AS obsValueString
FROM servicerequest AS sr
INNER JOIN diagnosticreport AS dr
  ON ( (dr.resource -> 'basedOn') @@ LOGIC_REVINCLUDE (sr.resource,sr.id,'#'))
    AND cast ( (dr.resource #>> '{effective,dateTime}') AS timestamp) BETWEEN '2021-03-01T00:00:00' AND '2021-04-01T23:59:59' 
INNER JOIN LATERAL (
  SELECT string_agg(coalesce((o.resource #>> '{value,string}'),(o.resource #>> '{value,Quantity,value}') || ' ' ||(o.resource #>> '{value,Quantity,unit}')),', ') AS valueString
  FROM observation AS o
  WHERE (o.resource @@ LOGIC_INCLUDE(dr.resource,'result'))
    OR (o.id = ANY (ARRAY ((SELECT (jsonb_path_query(dr.resource,'$.result.id') #>> '{}')))))) AS obs ON TRUE
CROSS JOIN main_org mo
WHERE ((jsonb_path_query_first(sr.resource,'$.category.coding ? (@.system=="urn:CodeSystem:servicerequest-category").code') #>> '{}') = 'Referral-LMI')
AND   (sr.resource ->> 'authoredOn') BETWEEN '2022-02-01' AND '2022-04-01T23:59:59'
AND   ((sr.resource ->> 'status') = 'completed')
AND   (jsonb_path_query_first(sr.resource,'$.performer ? (@.resourceType=="Organization" || @.type=="Organization")') @@ LOGIC_REVINCLUDE(mo.resource,mo.id))
--AND   ((sr.resource -> 'instantiatesCanonical') @@ '#="PlanDefinition/b841d7ac-659b-4d9a-be36-474378adce37"'::jsquery)
AND   ((sr.resource #>> '{paymentType,code}') = '1')
AND   ((sr.resource -> 'category') @@ '#.coding.#(system="urn:CodeSystem:lab-group")'::jsquery)