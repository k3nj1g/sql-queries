EXPLAIN (ANALYZE, buffers)
WITH servicerequests AS (
    SELECT sr.*
    FROM servicerequest sr 
    WHERE
      (sr.resource ->> 'authoredOn') BETWEEN '2021-06-01' AND '2021-08-01'
      AND ((sr.resource ->> 'status') = 'completed')
      AND ((jsonb_path_query_first(sr.resource,'$.category.coding ? (@.system=="urn:CodeSystem:servicerequest-category").code') #>> '{}') = 'Referral-LMI')
      AND ((sr.resource -> 'instantiatesCanonical') @@ '#="PlanDefinition/8feb1298-81f8-44d6-b12f-30896e1f8a7d"'::jsquery)
      AND (jsonb_path_query_first(sr.resource,'$.performer ? (@.resourceType=="Organization" || @.type=="Organization")') @@ LOGIC_REVINCLUDE('{"identifier":[{"value":"212301001","system":"urn:identity:kpp:Organization"},{"value":"1132134000052","system":"urn:identity:ogrn:Organization"},{"value":"1.2.643.5.1.13.13.12.2.21.1556","system":"urn:identity:oid:Organization"},{"value":"1.2.643.5.1.13.3.25.21.73","system":"urn:identity:old-oid:Organization"},{"value":"6801988","system":"urn:identity:frmo-head:Organization"},{"value":"2123012970","system":"urn:identity:inn:Organization"},{"value":"13094474","system":"urn:identity:okpo:Organization"},{"value":"e52603bf-0436-11e8-b7c8-005056871882","system":"urn:source:1c:Organization"},{"value":"36483c69-e82a-4c5b-9b1d-f133fb9d2503","system":"urn:source:rmis:Organization"}]}'::jsonb,'36483c69-e82a-4c5b-9b1d-f133fb9d2503'))
      AND ((sr.resource -> 'category') @@ '#.coding.#(system="urn:CodeSystem:lab-group")'::jsquery))
, sr_dr_direct AS (
    SELECT sr.resource sr_resource, dr.resource dr_resource
    FROM servicerequests sr
    JOIN diagnosticreport dr
      ON dr.resource #>> '{basedOn,0,id}' = sr.id  
), sr_dr_logical AS (
    SELECT sr.resource sr_resource, dr.resource dr_resource
    FROM servicerequests sr
    JOIN diagnosticreport dr
      ON (dr.resource #>> '{basedOn,0,identifier,value}') || (dr.resource #>> '{basedOn,0,identifier,system}') = ANY(ARRAY((SELECT (value ->> 'value') || (value ->> 'system') FROM jsonb_array_elements(sr.resource->'identifier'))))
    WHERE (dr.resource #>> '{basedOn,0,id}') IS NULL
), sr_union AS (
    SELECT *
    FROM sr_dr_direct sr
    UNION 
    SELECT *
    FROM sr_dr_logical sr)
SELECT DISTINCT (jsonb_path_query_first(sr_resource,'$.identifier ? (@.system=="urn:identity:Serial:ServiceRequest").value')) AS serialNumber,
       (sr_resource #>> '{subject,display}') AS fio,
       (sr_resource #> '{reasonCode,0,coding,0}') AS reasoncode,
       (sr_resource #>> '{managingOrganization,display}') AS managingOrganization,
       jsonb_path_query_first(sr_resource,'$.performer ? (@.type=="PractitionerRole" || @.resourceType=="PractitionerRole").display') AS performerPractitionerRole,
       jsonb_path_query_first(sr_resource,'$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services")') AS nmucode,
       jsonb_path_query_first(sr_resource,'$.code.coding ? (@.system=="urn:CodeSystem:rmis:ServiceRequest")') AS rmiscode,
       cast((sr_resource ->> 'authoredOn') AS timestamp) AS authoredOn,
       cast((dr_resource #>> '{effective,dateTime}') AS timestamp) AS effective,
       (sr_resource #>> '{paymentType,display}') AS payment,
       obs.valueString
FROM sr_union
JOIN LATERAL (
  SELECT STRING_AGG(COALESCE((o.resource #>> '{value,string}'), (o.resource #>> '{value,Quantity,value}') || ' ' || (o.resource #>> '{value,Quantity,unit}')), ', ') AS valueString
  FROM observation AS o 
  WHERE o.id = ANY(ARRAY((SELECT (JSONB_PATH_QUERY(dr_resource, '$.result.id') #>> '{}'))))) obs ON TRUE
LEFT JOIN concept AS c 
  ON ((c.resource #>> '{code}') = (JSONB_PATH_QUERY_FIRST(dr_resource, '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services").code') #>> '{}')) 
    AND ((c.resource #>> '{system}') = 'urn:CodeSystem:Nomenclature-medical-services')
    
SELECT *
FROM organization 
WHERE resource @@ 'identifier.#.value="1.2.643.5.1.13.13.12.2.21.1556"'::jsquery

    

ANALYZE diagnosticreport;

DROP INDEX diagnosticreport_resource_basedon_servicerequest;
DROP INDEX diagnosticreport_basedon_logical;

CREATE INDEX CONCURRENTLY diagnosticreport_basedon_direct ON diagnosticreport
  ((resource #>> '{basedOn,0,id}'))
  
CREATE INDEX CONCURRENTLY diagnosticreport_basedon_logical ON diagnosticreport
  (((resource #>> '{basedOn,0,identifier,value}') || (resource #>> '{basedOn,0,identifier,system}')))
  WHERE (resource #>> '{basedOn,0,id}') IS NULL
  
SELECT
    tablename,
    pg_size_pretty(table_size) AS table_size,
    pg_size_pretty(indexes_size) AS indexes_size,
    pg_size_pretty(total_size) AS total_size
FROM (
    SELECT
        tablename,
        pg_table_size(TABLE_NAME) AS table_size,
        pg_indexes_size(TABLE_NAME) AS indexes_size,
        pg_total_relation_size(TABLE_NAME) AS total_size
    FROM (
        SELECT ('"' || table_schema || '"."' || TABLE_NAME || '"') AS TABLE_NAME, TABLE_NAME AS tablename
        FROM information_schema.tables
    ) AS all_tables
    ORDER BY total_size DESC
) AS pretty_sizes
WHERE tablename = 'diagnosticreport';

select count(*) from diagnosticreport;

SELECT *, pg_relation_size(indexname::text), pg_size_pretty(pg_relation_size(indexname::text))
FROM pg_indexes 
WHERE tablename = 'diagnosticreport'
ORDER BY indexname;

EXPLAIN 
SELECT *
FROM plandefinition pd
JOIN activitydefinition ad
  ON concat('ActivityDefinition/', ad.id) = ANY (SELECT (jsonb_array_elements(pd.resource -> 'action')) #>> '{definition,canonical}')
JOIN diagnosticreport dr 
  ON dr.resource #>> '{effective,dateTime}' BETWEEN '2022-06-01' AND '2022-07-01'
    AND dr.resource @@ concat('code.coding.#.code=', ad.resource #> '{code,coding,0,code}')::jsquery
WHERE pd.id ='8feb1298-81f8-44d6-b12f-30896e1f8a7d';

SELECT count(*)
FROM diagnosticreport dr
WHERE resource #>> '{effective,dateTime}' BETWEEN '2022-06-01' AND '2022-07-01'

SELECT *
FROM pg_indexes
WHERE tablename = 'diagnosticreport'


WHERE ((jsonb_path_query_first(sr.resource,'$.category.coding ? (@.system=="urn:CodeSystem:servicerequest-category").code') #>> '{}') = 'Referral-LMI')