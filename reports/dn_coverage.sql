--EXPLAIN ANALYZE 
--SELECT enc.resource #>> '{serviceProvider,identifier,value}'
--    , count(*) FILTER (WHERE age(to_date(p.resource ->> 'birthDate','YYYY-MM-DD'))  >  '18 years')
--    , count(*) FILTER (WHERE age(to_date(p.resource ->> 'birthDate','YYYY-MM-DD'))  >  '65 years')
--FROM encounter enc
--LEFT JOIN LATERAL (
--    SELECT contained.value AS resource
--    FROM jsonb_array_elements(enc.resource -> 'diagnosis') diagnosis (value)
--    JOIN (SELECT jsonb_array_elements(enc.resource -> 'contained'::text) AS value) AS contained
--        ON (contained.value ->> 'id') = (diagnosis.value #>> '{condition,localRef}')
--    WHERE diagnosis.value @@ 'use.coding.#(system = "urn:CodeSystem:diagnosis-type" and code = "1")'::jsquery
--    LIMIT 1) main_condition
--  ON main_condition.resource IS NOT NULL 
--JOIN patient p ON p.resource @@ logic_include(enc.resource, 'subject')
--WHERE enc.resource #>> '{period,start}' >= '2021-01-01'
----knife_extract_max_timestamptz(enc.resource,'[["period", "start"]]'::jsonb) > '2021-01-01'
--    AND enc.resource @@ 'contained.#.code.coding.#(system = "urn:CodeSystem:icd-10" and code in ("C0","C1","C2","C3","C4","C5","C6","C7","C8","C90","C91","C92","C93","C94","C95","C96","C97","D00","D01","D02","D03","D04","D05","D06","D07","I20.1","I20.8","I20.9","I25.0","I25.1","I25.2","I25.5","I25.6","I25.8","I25.9","I10","I11","I12","I13","I15","I50.0","I50.1","I50.9","I48","I47","I65.2","R73.0","R73.9","E11","I69.0","I69.1","I69.2","I69.3","I69.4","I67.8","E78","K20","K21.0","K21.0","K25","K26","K29.4","K29.5","K31.7","K86","J41.0","J41.1","J41.8","J44.0","J44.8","J44.9","J47.0","J45.0","J45.1","J45.8","J45.9","J12","J13","J14","J84.1","B86","N18.1","N18.1","N18.9","M81.5"))'::jsquery    
--    AND main_condition.resource @@ 'code.coding.#(system = "urn:CodeSystem:icd-10" and code in ("C0","C1","C2","C3","C4","C5","C6","C7","C8","C90","C91","C92","C93","C94","C95","C96","C97","D00","D01","D02","D03","D04","D05","D06","D07","I20.1","I20.8","I20.9","I25.0","I25.1","I25.2","I25.5","I25.6","I25.8","I25.9","I10","I11","I12","I13","I15","I50.0","I50.1","I50.9","I48","I47","I65.2","R73.0","R73.9","E11","I69.0","I69.1","I69.2","I69.3","I69.4","I67.8","E78","K20","K21.0","K21.0","K25","K26","K29.4","K29.5","K31.7","K86","J41.0","J41.1","J41.8","J44.0","J44.8","J44.9","J47.0","J45.0","J45.1","J45.8","J45.9","J12","J13","J14","J84.1","B86","N18.1","N18.1","N18.9","M81.5"))'::jsquery
--GROUP BY 1

SELECT count(*)
FROM encounter enc
WHERE main_diagnosis_code(resource) IN ('C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C90', 'C91', 'C92', 'C93', 'C94', 'C95', 'C96', 'C97', 'D00', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'I20.1', 'I20.8', 'I20.9', 'I25.0', 'I25.1', 'I25.2', 'I25.5', 'I25.6', 'I25.8', 'I25.9', 'I10', 'I11', 'I12', 'I13', 'I15', 'I50.0', 'I50.1', 'I50.9', 'I48', 'I47', 'I65.2', 'R73.0', 'R73.9', 'E11', 'I69.0', 'I69.1', 'I69.2', 'I69.3', 'I69.4', 'I67.8', 'E78', 'K20', 'K21.0', 'K21.0', 'K25', 'K26', 'K29.4', 'K29.5', 'K31.7', 'K86', 'J41.0', 'J41.1', 'J41.8', 'J44.0', 'J44.8', 'J44.9', 'J47.0', 'J45.0', 'J45.1', 'J45.8', 'J45.9', 'J12', 'J13', 'J14', 'J84.1', 'B86', 'N18.1', 'N18.1', 'N18.9', 'M81.5')

EXPLAIN --ANALYZE 
SELECT enc.resource #>> '{serviceProvider,identifier,value}' org,
    enc.resource #>> '{subject,identifier,value}' patient, 
    main_diagnosis_code(resource) diagnosis  
FROM encounter enc
WHERE main_diagnosis_code(resource) IN ('C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C90', 'C91', 'C92', 'C93', 'C94', 'C95', 'C96', 'C97', 'D00', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'I20.1', 'I20.8', 'I20.9', 'I25.0', 'I25.1', 'I25.2', 'I25.5', 'I25.6', 'I25.8', 'I25.9', 'I10', 'I11', 'I12', 'I13', 'I15', 'I50.0', 'I50.1', 'I50.9', 'I48', 'I47', 'I65.2', 'R73.0', 'R73.9', 'E11', 'I69.0', 'I69.1', 'I69.2', 'I69.3', 'I69.4', 'I67.8', 'E78', 'K20', 'K21.0', 'K21.0', 'K25', 'K26', 'K29.4', 'K29.5', 'K31.7', 'K86', 'J41.0', 'J41.1', 'J41.8', 'J44.0', 'J44.8', 'J44.9', 'J47.0', 'J45.0', 'J45.1', 'J45.8', 'J45.9', 'J12', 'J13', 'J14', 'J84.1', 'B86', 'N18.1', 'N18.1', 'N18.9', 'M81.5')
ORDER BY patient    

VACUUM ANALYSE encounter;

SELECT *
FROM pg_indexes
WHERE tablename = 'encounter'

CREATE INDEX encounter_dn_coverage 
  ON encounter ((resource #>> '{subject,identifier,value}'))
    WHERE main_diagnosis_code(resource) IN ('C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C90', 'C91', 'C92', 'C93', 'C94', 'C95', 'C96', 'C97', 'D00', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'I20.1', 'I20.8', 'I20.9', 'I25.0', 'I25.1', 'I25.2', 'I25.5', 'I25.6', 'I25.8', 'I25.9', 'I10', 'I11', 'I12', 'I13', 'I15', 'I50.0', 'I50.1', 'I50.9', 'I48', 'I47', 'I65.2', 'R73.0', 'R73.9', 'E11', 'I69.0', 'I69.1', 'I69.2', 'I69.3', 'I69.4', 'I67.8', 'E78', 'K20', 'K21.0', 'K21.0', 'K25', 'K26', 'K29.4', 'K29.5', 'K31.7', 'K86', 'J41.0', 'J41.1', 'J41.8', 'J44.0', 'J44.8', 'J44.9', 'J47.0', 'J45.0', 'J45.1', 'J45.8', 'J45.9', 'J12', 'J13', 'J14', 'J84.1', 'B86', 'N18.1', 'N18.1', 'N18.9', 'M81.5')


-------- Encounter --------
--EXPLAIN ANALYSE 
WITH base_selection AS (
    SELECT enc.resource #>> '{serviceProvider,identifier,value}' org,
        enc.resource #>> '{subject,identifier,value}' patient, 
        main_diagnosis_code(resource) diagnosis  
    FROM encounter enc
    WHERE main_diagnosis_code(resource) IN ('C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C90', 'C91', 'C92', 'C93', 'C94', 'C95', 'C96', 'C97', 'D00', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'I20.1', 'I20.8', 'I20.9', 'I25.0', 'I25.1', 'I25.2', 'I25.5', 'I25.6', 'I25.8', 'I25.9', 'I10', 'I11', 'I12', 'I13', 'I15', 'I50.0', 'I50.1', 'I50.9', 'I48', 'I47', 'I65.2', 'R73.0', 'R73.9', 'E11', 'I69.0', 'I69.1', 'I69.2', 'I69.3', 'I69.4', 'I67.8', 'E78', 'K20', 'K21.0', 'K21.0', 'K25', 'K26', 'K29.4', 'K29.5', 'K31.7', 'K86', 'J41.0', 'J41.1', 'J41.8', 'J44.0', 'J44.8', 'J44.9', 'J47.0', 'J45.0', 'J45.1', 'J45.8', 'J45.9', 'J12', 'J13', 'J14', 'J84.1', 'B86', 'N18.1', 'N18.1', 'N18.9', 'M81.5')
    ORDER BY patient
),
sorted_patient AS (
    SELECT jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:enp:Patient")."value"') #>> '{}' enp
        , age(to_date(resource ->> 'birthDate','YYYY-MM-DD')) patient_age
    FROM patient p 
    ORDER BY jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:enp:Patient")."value"')
)
, with_patient AS (
    SELECT DISTINCT ON (bs.patient, bs.diagnosis) 
        bs.org
        , sp.patient_age
    FROM base_selection bs
    JOIN sorted_patient sp ON sp.enp = bs.patient
)
SELECT org
    , count(*) FILTER (WHERE patient_age > '18 years') older_18
    , count(*) FILTER (WHERE patient_age > '65 years') older_65
FROM with_patient 
GROUP BY org

-------- EpisodeOfCare --------
WITH base_selection AS (
    SELECT resource #>> '{managingOrganization,identifier,value}' org,
        resource #> '{patient,identifier,value}' patient,
        jsonb_path_query_first(resource, '$.diagnosis ? (exists (@ ? (@.rank==1))).condition.code.coding ? (@.system == "urn:CodeSystem:icd-10").code') diagnosis
    FROM episodeofcare
    WHERE resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="CardDN") and not period.end = *'::jsquery      
    ORDER BY patient
),
sorted_patient AS (
    SELECT jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:enp:Patient")."value"') enp
        , age(to_date(resource ->> 'birthDate','YYYY-MM-DD')) patient_age
    FROM patient p 
    ORDER BY jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:enp:Patient")."value"')
)
, with_patient AS (
    SELECT DISTINCT ON (bs.patient, bs.diagnosis) 
        bs.org
        , sp.patient_age
    FROM base_selection bs
    JOIN sorted_patient sp ON sp.enp = bs.patient
    WHERE org IS NOT NULL)
SELECT org
    , count(*) FILTER (WHERE patient_age > '18 years') older_18
    , count(*) FILTER (WHERE patient_age > '65 years') older_65
FROM with_patient 
GROUP BY org   

CREATE INDEX encounter_dn_coverage_period_start 
  ON encounter (((resource #>> '{period,start}'))) 
    WHERE (resource @@ '"contained".#."code"."coding".#("system" = "urn:CodeSystem:icd-10" AND "code" IN ("C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C90", "C91", "C92", "C93", "C94", "C95", "C96", "C97", "D00", "D01", "D02", "D03", "D04", "D05", "D06", "D07", "I20.1", "I20.8", "I20.9", "I25.0", "I25.1", "I25.2", "I25.5", "I25.6", "I25.8", "I25.9", "I10", "I11", "I12", "I13", "I15", "I50.0", "I50.1", "I50.9", "I48", "I47", "I65.2", "R73.0", "R73.9", "E11", "I69.0", "I69.1", "I69.2", "I69.3", "I69.4", "I67.8", "E78", "K20", "K21.0", "K21.0", "K25", "K26", "K29.4", "K29.5", "K31.7", "K86", "J41.0", "J41.1", "J41.8", "J44.0", "J44.8", "J44.9", "J47.0", "J45.0", "J45.1", "J45.8", "J45.9", "J12", "J13", "J14", "J84.1", "B86", "N18.1", "N18.1", "N18.9", "M81.5"))'::jsquery)    
    
SELECT *
    FROM episodeofcare
    WHERE resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="CardDN") and not period.end = *'::jsquery