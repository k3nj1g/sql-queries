--EXPLAIN ANALYZE 
SELECT org.resource #>> '{alias,0}'
       ,count(*) "all"
       , count(DISTINCT eoc.id) FILTER (WHERE (cast(trunc(date_part('day', cast(now() AS timestamp) - cast((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + cast((o.resource #>> '{value,Quantity,value}') AS integer) AS integer)) >= 22) AS "after_22_weeks"
       , count(DISTINCT eoc.id) FILTER (WHERE CAST((jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="A07"))).period.start') #>> '{}') AS timestamp) > (cast(now() AS timestamp)) - '6 month'::interval) AS "covid_last_6_months"
       , count(DISTINCT eoc.id) FILTER (WHERE (cast(trunc(date_part('day',cast((jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') #>> '{}') AS timestamp) - cast((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + cast((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) < 22)) AS "vaccination_before_22"
       , count(DISTINCT eoc.id) FILTER (WHERE (cast(trunc(date_part('day',cast((jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') #>> '{}') AS timestamp) - cast((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + cast((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) >= 22)) AS "vaccination_after_22"
       , count(DISTINCT eoc.id) FILTER (WHERE (cast(trunc(date_part('day',cast((jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') #>> '{}') AS timestamp) - cast((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + cast((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) < 0)) AS "vaccination_before"
FROM episodeofcare AS eoc
LEFT JOIN patientflag AS pf
    ON ((pf.resource #> '{subject,resource}') @@ LOGIC_INCLUDE (eoc.resource,'patient'))
       AND EXISTS (SELECT 1
                   FROM jsonb_array_elements((pf.resource #> '{flag}')) AS flag
                   WHERE (flag @@ 'path.#.code in ("V01.19", "A07")'::jsquery)
--                     AND (tsrange(cast((eoc.resource #>> '{period,start}') AS timestamp),coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval))) @> cast((flag #>> '{period,start}') AS timestamp))
                     ) 
JOIN LATERAL (SELECT *
              FROM observation
              WHERE ((resource -> 'episodeOfCare') @@ LOGIC_REVINCLUDE(eoc.resource,eoc.id))
                AND ((resource -> 'category') @@ '#.coding.#(system="urn:CodeSystem:pregnancy" and code="current-pregnancy")'::jsquery)
                AND ((resource -> 'code') @@ 'coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery)
              ORDER BY (resource #>> '{effective,dateTime}') DESC LIMIT 1) AS o ON TRUE
JOIN organization AS org ON org.resource @@ LOGIC_INCLUDE(eoc.resource,'managingOrganization')
WHERE (IMMUTABLE_TSRANGE((eoc.resource #>> '{period,start}'),(eoc.resource #>> '{period,end}')) @> cast(now() AS timestamp))
  AND ((eoc.resource #>> '{period,start}') < COALESCE((eoc.resource #>> '{period,end}'),'infinity'))
  AND ((jsonb_path_query_first(eoc.resource,'$.type.coding ? (@.system=="urn:CodeSystem:episodeofcare-type").code') #>> '{}') = 'PregnantCard')
  AND (coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval)) > cast(now() AS timestamp))
GROUP BY 1
  --AND (eoc.resource @@ LOGIC_REVINCLUDE(org.resource,org.id,'managingOrganization'));
 