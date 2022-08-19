--EXPLAIN ANALYZE 
SELECT  
    TO_JSONB(eoc.*) AS episodeofcare,
    (pf.resource -> 'subject') AS patient,
    JSONB_PATH_QUERY_FIRST(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') AS vaccination_date,
    cast(TRUNC (DATE_PART ('day',cast((JSONB_PATH_QUERY_FIRST (pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') #>> '{}') AS timestamp) - CAST((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + CAST((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) AS gestational_age 
FROM episodeofcare AS eoc 
JOIN patientflag AS pf ON ((pf.resource #> '{subject,resource}') @@ LOGIC_INCLUDE(eoc.resource, 'patient')) 
    AND EXISTS (
        SELECT 1 
        FROM JSONB_ARRAY_ELEMENTS((pf.resource #> '{flag}')) AS flag 
        WHERE (flag @@ 'path.#.code="V01.19"'::jsquery) 
            AND TSRANGE(CAST((eoc.resource #>> '{period,start}') AS timestamp), COALESCE(CAST((eoc.resource #>> '{period,end}') AS timestamp), CAST((eoc.resource #>> '{period,start}') AS timestamp) + CAST('365 days' AS interval)))
                @> (flag #>> '{period,start}')::timestamp)
JOIN LATERAL (
    SELECT *
    FROM observation 
    WHERE resource -> 'episodeOfCare' @@ LOGIC_REVINCLUDE(eoc.resource, eoc.id)
        AND resource -> 'category' @@ '#."coding".#("system" = "urn:CodeSystem:pregnancy" AND "code" = "current-pregnancy")'::jsquery
        AND resource -> 'code' @@ 'coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery
    ORDER BY (resource #>> '{effective,dateTime}')  DESC 
    LIMIT 1) AS o ON TRUE 
--JOIN organization AS org ON org.id = 'pcv-org-1' 
WHERE (IMMUTABLE_TSRANGE((eoc.resource #>> '{period,start}'), (eoc.resource #>> '{period,end}')) @> CAST(NOW() AS timestamp)) 
    AND ((eoc.resource #>> '{period,start}') < COALESCE((eoc.resource #>> '{period,end}'), 'infinity')) 
    AND ((JSONB_PATH_QUERY_FIRST(eoc.resource, '$.type.coding ? (@.system=="urn:CodeSystem:episodeofcare-type").code') #>> '{}') = 'PregnantCard') 
    AND (COALESCE(CAST((eoc.resource #>> '{period,end}') AS timestamp), CAST((eoc.resource #>> '{period,start}') AS timestamp) + CAST('365 days' AS interval)) > CAST(NOW() AS timestamp)) 
--    AND (eoc.resource @@ LOGIC_REVINCLUDE(org.resource, org.id, 'managingOrganization'))

SELECT count(*)    
FROM observation o 
WHERE o.resource @@ 'code.coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery
    
--EXPLAIN ANALYZE
SELECT pf.resource, JSONB_PATH_QUERY_FIRST(pf.resource, '$.flag ? (exists (@.path ? (@.code=="V01.19"))).period') #>> '{}'
--       to_jsonb(eoc.*) AS episodeofcare,
--       to_jsonb(p.*) AS patient,
--       to_jsonb(f.*) AS flag,
--       cast(trunc(date_part('day',cast((f.resource #>> '{period,start}') AS timestamp) - cast((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + cast((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) AS gestational_age
FROM episodeofcare AS eoc
JOIN patientflag pf ON pf.resource #> '{subject,resource}' @@ LOGIC_INCLUDE (eoc.resource,'patient')
    AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements(pf.resource -> 'flag') flag
        WHERE flag @@ 'path.#.code = "V01.19"'::jsquery
            AND IMMUTABLE_TSRANGE((flag #>> '{period,start}'), (flag #>> '{period,end}')) && tsrange (cast ( (eoc.resource #>> '{period,start}') AS timestamp),coalesce (cast ( (eoc.resource #>> '{period,end}') AS timestamp),cast ( (eoc.resource #>> '{period,start}') AS timestamp) + cast ('365 days' AS interval)))
    )    
--JOIN patient AS p ON (p.resource @@ LOGIC_INCLUDE (eoc.resource,'patient'))
--JOIN flag AS f ON (f.resource @@ LOGIC_REVINCLUDE (p.resource,p.id,'subject'))
--    AND (f.resource #>> '{period,end}') > eoc.resource #>> '{period,start}'
--    AND ((jsonb_path_query_first (f.resource,'$.code.coding ? (@.system=="urn:CodeSystem:r21.tag").code') #>> '{}') LIKE 'V01.19%')
--    AND (IMMUTABLE_TSRANGE((f.resource #>> '{period,start}'), (f.resource #>> '{period,end}')) && tsrange (cast ( (eoc.resource #>> '{period,start}') AS timestamp),coalesce (cast ( (eoc.resource #>> '{period,end}') AS timestamp),cast ( (eoc.resource #>> '{period,start}') AS timestamp) + cast ('365 days' AS interval)))) 
--JOIN observation o ON (o.resource -> 'episodeOfCare' @@ LOGIC_REVINCLUDE(eoc.resource,eoc.id))
--    AND o.resource -> 'category' @@ '#."coding".#("system" = "urn:CodeSystem:pregnancy" AND "code" = "current-pregnancy")'::jsquery
--    AND o.resource -> 'code' @@ 'coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery
--JOIN LATERAL (
--    SELECT *
--    FROM observation
--    WHERE (resource -> 'episodeOfCare' @@ LOGIC_REVINCLUDE(eoc.resource,eoc.id))
--        AND resource -> 'category' @@ '#."coding".#("system" = "urn:CodeSystem:pregnancy" AND "code" = "current-pregnancy")'::jsquery
--        AND resource -> 'code' @@ 'coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery
--    ORDER BY (resource #>> '{effective,dateTime}') DESC 
--    LIMIT 1) AS o ON TRUE
WHERE (immutable_tsrange(eoc.resource #>> '{period,start}', eoc.resource #>> '{period,end}')) @> '2021-06-01'::timestamp
    AND eoc.resource #>> '{period,start}' < COALESCE (eoc.resource #>> '{period,end}', 'infinity')
    AND jsonb_path_query_first(eoc.resource, '$.type.coding ? (@.system == "urn:CodeSystem:episodeofcare-type").code') #>> '{}' = 'PregnantCard'
    AND COALESCE (CAST ((eoc.resource #>> '{period,end}') AS timestamp), CAST ((eoc.resource #>> '{period,start}') AS timestamp) + CAST ('365 days' AS interval)) > '2021-06-01'
--    (KNIFE_EXTRACT_TEXT(eoc.resource,'[["type",{},"coding",{},"code"]]') @> ARRAY['PregnantCard'])
--    AND CAST ((eoc.resource #>> '{period,start}') AS timestamp) < current_date 
--    AND COALESCE (CAST ((eoc.resource #>> '{period,end}') AS timestamp), CAST ((eoc.resource #>> '{period,start}') AS timestamp) + CAST ('365 days' AS interval)) > current_date 
--    cast(now() AS timestamp) BETWEEN cast((eoc.resource #>> '{period,start}') AS timestamp)
--    AND coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval))
--    (tsrange(cast((eoc.resource #>> '{period,start}') AS timestamp),coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval))) @> cast(now() AS timestamp)
    
SELECT *
FROM patientflag
LIMIT 10;
    
SELECT count(*)
FROM observation o 
WHERE resource @@ 'code.coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery
  
SELECT *
FROM pg_indexes
WHERE tablename = 'patientflag'

CREATE INDEX episodeofcare_period_pregnant__gist
    ON episodeofcare 
    USING gist (immutable_tsrange(resource #>> '{period,start}', resource #>> '{period,end}'))
    WHERE resource #>> '{period,start}' < COALESCE (resource #>> '{period,end}', 'infinity')
        AND jsonb_path_query_first(resource, '$.type.coding ? (@.system == "urn:CodeSystem:episodeofcare-type").code') #>> '{}' = 'PregnantCard'
        
CREATE INDEX patientflag_resource_subject_gin__jsquery 
    ON patientflag 
    USING gin ((resource #> '{subject,resource}') jsonb_path_value_ops)
        
        --CREATE INDEX flag_period__gist
--    ON flag 
--    USING gist (immutable_tsrange(resource #>> '{period,start}', resource #>> '{period,end}'))
--    WHERE resource #>> '{period,start}' < resource #>> '{period,end}';

CREATE INDEX flag_resource_tag__btree
    ON flag
    (((jsonb_path_query_first(resource,'$.code.coding ? (@.system=="urn:CodeSystem:r21.tag").code')) #>> '{}') text_pattern_ops)

select count(*) from patientflag;    
    
DROP INDEX episodeofcare_period_pregnant__gist;
        
VACUUM ANALYZE episodeofcare;
VACUUM ANALYZE flag;
VACUUM ANALYZE patientflag;

SELECT to_jsonb(eoc.*) AS episodeofcare,
       (pf.resource -> 'subject') AS patient,
       jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') AS vaccination_date,
       cast(trunc(date_part('day',cast((jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') #>> '{}') AS timestamp) - cast((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + cast((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) AS gestational_age
FROM episodeofcare AS eoc
  INNER JOIN patientflag AS pf
          ON ( (pf.resource #> '{subject,resource}') @@ LOGIC_INCLUDE (eoc.resource,'patient'))
         AND EXISTS (SELECT 1
                     FROM jsonb_array_elements((pf.resource #> '{flag}')) AS flag
                     WHERE (flag @@ 'path.#.code="V01.19"'::jsquery)
                     AND   (IMMUTABLE_TSRANGE((flag #>> '{period,start}'),(flag #>> '{period,end}')) && tsrange(cast((eoc.resource #>> '{period,start}') AS timestamp),coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval))))) INNER JOIN LATERAL (SELECT *
                                                                                                                                                                                                                                                                                                                                                   FROM observation
                                                                                                                                                                                                                                                                                                                                                   WHERE (observation.resource @@ LOGIC_REVINCLUDE(eoc.resource,eoc.id,'episodeOfCare',' and code.coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'))
                                                                                                                                                                                                                                                                                                                                                   ORDER BY (resource #>> '{effective,dateTime}') DESC LIMIT 1) AS o ON TRUE
WHERE (KNIFE_EXTRACT_TEXT(eoc.resource,'[["type",{},"coding",{},"code"]]') @> ARRAY['PregnantCard'])
AND   (tsrange(cast((eoc.resource #>> '{period,start}') AS timestamp),coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval))) @> cast(now() AS timestamp))

SELECT TO_JSONB(eoc.*) AS episodeofcare, (pf.resource -> 'subject') AS patient, JSONB_PATH_QUERY_FIRST(pf.resource, '$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') AS vaccination_date, CAST(TRUNC(DATE_PART('day', CAST((JSONB_PATH_QUERY_FIRST(pf.resource, '$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') #>> '{}') AS timestamp) - CAST((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + CAST((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) AS gestational_age FROM episodeofcare AS eoc INNER JOIN patientflag AS pf ON ((pf.resource #> '{subject,resource}') @@ LOGIC_INCLUDE(eoc.resource, 'patient')) AND EXISTS (SELECT 1 FROM JSONB_ARRAY_ELEMENTS((pf.resource #> '{flag}')) AS flag WHERE (flag @@ 'path.#.code="V01.19"'::jsquery) AND (IMMUTABLE_TSRANGE((flag #>> '{period,start}'), (flag #>> '{period,end}')) && TSRANGE(CAST((eoc.resource #>> '{period,start}') AS timestamp), COALESCE(CAST((eoc.resource #>> '{period,end}') AS timestamp), CAST((eoc.resource #>> '{period,start}') AS timestamp) + CAST('365 days' AS interval))))) INNER JOIN LATERAL (SELECT * FROM observation WHERE ((resource -> 'episodeOfCare') @@ LOGIC_REVINCLUDE(eoc.resource, eoc.id)) AND ((resource -> 'category') @@ '#.coding.#(system="urn:CodeSystem:pregnancy" and code="current-pregnancy")'::jsquery) AND ((resource -> 'code') @@ 'coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery) ORDER BY (resource #>> '{effective,dateTime}') DESC LIMIT 1) AS o ON TRUE WHERE (IMMUTABLE_TSRANGE((eoc.resource #>> '{period,start}'), (eoc.resource #>> '{period,end}')) @> CAST(NOW() AS timestamp)) AND ((eoc.resource #>> '{period,start}') < COALESCE((eoc.resource #>> '{period,end}'), 'infinity')) AND ((JSONB_PATH_QUERY_FIRST(eoc.resource, '$.type.coding ? (@.system=="urn:CodeSystem:episodeofcare-type").code') #>> '{}') = 'PregnantCard') AND (COALESCE(CAST((eoc.resource #>> '{period,end}') AS timestamp), CAST((eoc.resource #>> '{period,start}') AS timestamp) + CAST('365 days' AS interval)) > CAST(NOW() AS timestamp))

SELECT *
FROM patientflag
WHERE resource #>> '{subject,id}' = '7e123912-1d24-4a27-a86d-298ca00e0aa7'

--EXPLAIN ANALYSE 
SELECT  
    to_jsonb(eoc.*) AS episodeofcare,
    (pf.resource -> 'subject') AS patient,
    jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') AS vaccination_date,
    cast(trunc(date_part('day',cast((jsonb_path_query_first(pf.resource,'$.flag ? (exists (@.path ? (@.code=="V01.19"))).period.start') #>> '{}') AS timestamp) - cast((o.resource #>> '{effective,dateTime}') AS timestamp)) / 7) + cast((o.resource #>> '{value,Quantity,value}') AS integer) AS integer) AS gestational_age
FROM episodeofcare AS eoc
  JOIN patientflag AS pf
    ON ((pf.resource #> '{subject,resource}') @@ LOGIC_INCLUDE (eoc.resource,'patient'))
       AND EXISTS (SELECT 1
                   FROM jsonb_array_elements((pf.resource #> '{flag}')) AS flag
                   WHERE (flag @@ 'path.#.code="V01.19"'::jsquery)
                     AND (tsrange(cast((eoc.resource #>> '{period,start}') AS timestamp),coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval))) @> cast((flag #>> '{period,start}') AS timestamp))) 
  JOIN LATERAL (SELECT *
                FROM observation
                WHERE ((resource -> 'episodeOfCare') @@ LOGIC_REVINCLUDE(eoc.resource,eoc.id))
                  AND ((resource -> 'category') @@ '#.coding.#(system="urn:CodeSystem:pregnancy" and code="current-pregnancy")'::jsquery)
                  AND ((resource -> 'code') @@ 'coding.#(system="urn:CodeSystem:pregnancy-information" and code="gestational-age-start")'::jsquery)
                ORDER BY (resource #>> '{effective,dateTime}') DESC LIMIT 1) AS o ON TRUE
  JOIN organization AS org ON org.id = 'df0332f8-3334-45e2-8755-79f39ba50e1c'
WHERE (IMMUTABLE_TSRANGE((eoc.resource #>> '{period,start}'),(eoc.resource #>> '{period,end}')) @> cast(now() AS timestamp))
  AND ((eoc.resource #>> '{period,start}') <coalesce((eoc.resource #>> '{period,end}'),'infinity'))
  AND ((jsonb_path_query_first(eoc.resource,'$.type.coding ? (@.system=="urn:CodeSystem:episodeofcare-type").code') #>> '{}') = 'PregnantCard')
  AND (coalesce(cast((eoc.resource #>> '{period,end}') AS timestamp),cast((eoc.resource #>> '{period,start}') AS timestamp) + cast('365 days' AS interval)) > cast(now() AS timestamp))
  AND (eoc.resource @@ LOGIC_REVINCLUDE(org.resource,org.id,'managingOrganization'));
  
VACUUM ANALYZE episodeofcare;