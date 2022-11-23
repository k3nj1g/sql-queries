WITH grouped AS
(
  SELECT (sr.resource #>> '{instantiatesCanonical,0}') AS plandefinition,
         (jsonb_path_query_first(sr.resource,'$.category.coding ? (@.system=="urn:CodeSystem:lab-group").display') #>> '{}') AS lab_group,
         coalesce((jsonb_path_query_first(dr.resource,'$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services").code') #>> '{}'),'') AS med_service,
         trim((coalesce((jsonb_path_query_first(o.resource,'$.code.coding ? (@.system=="urn:CodeSystem:esli-li-test").display') #>> '{}'),'') || ' ' ||coalesce((jsonb_path_query_first(o.resource,'$.code.coding ? (@.system=="urn:CodeSystem:Laboratory-Research-and-Test").display') #>> '{}'),''))) AS test,
         count(DISTINCT sr.*) AS analyses,
         count(o.*) AS tests,
         count(o.*) FILTER (WHERE (jsonb_path_query_first(o.resource,'$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code') #>> '{}') = '1') AS tests_st,
         count(o.*) FILTER (WHERE (jsonb_path_query_first(o.resource,'$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code') #>> '{}') IS NULL) AS tests_no_location,
         count(o.*) FILTER (WHERE (jsonb_path_query_first(o.resource,'$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code') #>> '{}') = '2') AS tests_day_st,
         count(o.*) FILTER (WHERE (jsonb_path_query_first(o.resource,'$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code') #>> '{}') = '3') AS tests_amb
  FROM servicerequest AS sr
    INNER JOIN diagnosticreport AS dr ON ( (dr.resource -> 'basedOn') @@ LOGIC_REVINCLUDE (sr.resource,sr.id,'#'))
    INNER JOIN observation AS o
            ON (o.resource @@ LOGIC_INCLUDE (dr.resource,'result'))
            OR (o.id = ANY (ARRAY ( (SELECT (jsonb_path_query(dr.resource,'$.result.id') #>> '{}')))))
  WHERE (jsonb_path_query_first(sr.resource,'$.performer ? (@.resourceType=="Organization" || @.type=="Organization")') @@ LOGIC_REVINCLUDE('{"identifier":[{"value":"213001001","system":"urn:identity:kpp:Organization"},{"value":"1022100982056","system":"urn:identity:ogrn:Organization"},{"value":"1.2.643.5.1.13.13.12.2.21.1525","system":"urn:identity:oid:Organization"},{"value":"1.2.643.5.1.13.3.25.21.31","system":"urn:identity:old-oid:Organization"},{"value":"6802006","system":"urn:identity:frmo-head:Organization"},{"value":"2126002610","system":"urn:identity:inn:Organization"},{"value":"05213344","system":"urn:identity:okpo:Organization"},{"value":"6802006","system":"urn:source:frmo-head:Organization"},{"value":"eba5ea5c-0436-11e8-b7c8-005056871882","system":"urn:source:1c:Organization"},{"value":"ab241ca8-ff79-4330-9ca4-c80e6324b1ad","system":"urn:source:rmis:Organization"},{"value":"17b2a212-5354-4c04-a712-16de9e9bd329","system":"urn:source:paknitsmbu:Organization"},{"value":"212320","system":"urn:identity:ffoms.f003:OrganizationInfo"}]}','1150e915-f639-4234-a795-1767e0a0be5f'))
  AND   ((sr.resource #>> '{performerInfo,requestStatus}') = 'completed')
  AND   (sr.resource ->> 'authoredOn') BETWEEN '2022-05-01' AND '2022-06-01'
  AND   (sr.resource @@ 'category.#.coding.#(code in ("Referral-IMI","Referral-LMI","Referral-Rehabilitation","Referral-Consultation","Referral-Hospitalization") and system="urn:CodeSystem:servicerequest-category")'::jsquery)
  GROUP BY plandefinition,
           lab_group,
           med_service,
           test
)
SELECT plandefinition,
       lab_group,
       (c.resource ->> 'display') AS med_service,
       test,
       sum(analyses) OVER () AS analyzes_all,
       sum(tests) OVER (PARTITION BY plandefinition,lab_group) AS tests_by_lab_group,
       tests AS tests_all,
       tests_st,
       tests_no_location,
       tests_day_st,
       tests_amb
FROM grouped
  INNER JOIN concept AS c
          ON ( (c.resource #>> '{system}') = 'urn:CodeSystem:Nomenclature-medical-services')
         AND ( (c.resource #>> '{code}') = med_service)
WHERE split_part(plandefinition,'/',1) = 'PlanDefinition'