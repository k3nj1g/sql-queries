--- count resources ---
SELECT count(*)
FROM documentreference d ;    

--- look subject subject ref ---
SELECT *
FROM "documentreference"
LIMIT 10;    

--- create index if needed ---
CREATE INDEX CONCURRENTLY documentreference_resource_subject_ref_valid 
ON "documentreference" ((enp_valid(resource #>> '{subject,identifier,value}')))
WHERE resource @@ 'subject.identifier.system = "urn:identity:insurance-gov:Patient"'::jsquery;

--- find broken subject refs ---
--EXPLAIN ANALYZE 
SELECT --base.resource #>> '{subject,display}', p.resource -> 'name', p.resource -> 'birthDate', p.id, p.resource 
    base.id, jsonb_agg(base.resource #>> '{subject,display}'), count(*)
FROM "documentreference" base
--LEFT JOIN patient p
--          ON p.resource #>> '{name,0,given,0}' ILIKE split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',2)
--         AND p.resource #>> '{name,0,given,1}' ILIKE concat ('%', split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',3), '%')
--         AND p.resource #>> '{name,0,family}' ILIKE split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',1)
--         AND p.resource #>> '{birthDate}' = to_date (split_part (base.resource #>> '{subject,display}',',',2),'DD.mm.YYYY')::TEXT
--         AND NOT p.resource ->> 'active' = 'false'
----         AND NOT p.resource @@ 'extension.#(url = "urn:extension:patient-type" and valueCode = "partially-identified")'::jsquery
--         AND jsonb_array_length(p.resource -> 'identifier') > 1
----              OR jsonb_path_query_first (p.resource,'$.identifier[*] ? (@.system=="urn:identity:snils:Patient").value') IS NOT NULL) 
WHERE base.resource @@ 'subject.identifier.system = "urn:identity:insurance-gov:Patient"'::jsquery
    AND NOT enp_valid(base.resource #>> '{subject,identifier,value}')
GROUP BY base.id
ORDER BY 2;

--- fix with different systems
WITH pat_match AS (
    SELECT base.id, jsonb_agg(p.resource) pat_resource
    FROM "documentreference" base
    JOIN Patient p
         ON p.resource #>> '{name,0,given,0}' ILIKE split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',2)
        AND p.resource #>> '{name,0,given,1}' ILIKE concat ('%', split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',3), '%')
        AND p.resource #>> '{name,0,family}' ILIKE split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',1)
        AND p.resource #>> '{birthDate}' = to_date (split_part (base.resource #>> '{subject,display}',',',2),'DD.mm.YYYY')::TEXT
        AND NOT p.resource ->> 'active' = 'false'
        AND NOT p.resource @@ 'extension.#(url = "urn:extension:patient-type" and valueCode = "partially-identified")'::jsquery
        AND jsonb_array_length(p.resource -> 'identifier') > 2
--        AND jsonb_path_query_first (p.resource,'$.identifier[*] ? (@.system=="urn:identity:snils:Patient").value') IS NOT NULL 
        AND jsonb_path_query_first (p.resource,'$.identifier[*] ? (@.system=="urn:identity:enp:Patient").value') IS NOT NULL 
    WHERE base.resource @@ 'subject.identifier.system = "urn:identity:insurance-gov:Patient"'::jsquery
        AND NOT enp_valid(base.resource #>> '{subject,identifier,value}')
    GROUP BY base.id
    HAVING count(*) = 1)
UPDATE "documentreference" base
SET resource = base.resource || jsonb_build_object('subject', jsonb_build_object('type', base.resource #>> '{subject,type}', 'display', base.resource #>> '{subject,display}', 'identifier', 
                                  (SELECT COALESCE(
                                    (SELECT jsonb_build_object('value', value -> 'value', 'system', value -> 'system') 
                                     FROM jsonb_path_query(pat_resource -> 0, '$.identifier ? (@.system == "urn:identity:insurance-gov:Patient")') value 
                                     WHERE enp_valid(value->> 'value')
                                     LIMIT 1),
                                    (SELECT jsonb_build_object('value', value -> 'value', 'system', value -> 'system') 
                                     FROM jsonb_path_query_first(pat_resource -> 0, '$.identifier ? (@.system == "urn:identity:snils:Patient")') value
                                     WHERE value -> 'value' IS NOT NULL),
                                    (SELECT jsonb_build_object('value', value -> 'value', 'system', value -> 'system') 
                                     FROM jsonb_path_query_first(pat_resource -> 0, '$.identifier ? (@.system == "urn:identity:passport-rf:Patient")') value
                                     WHERE value -> 'value' IS NOT NULL),
                                    (SELECT jsonb_build_object('value', value -> 'value', 'system', value -> 'system') 
                                     FROM jsonb_path_query_first(pat_resource -> 0, '$.identifier ? (@.system == "urn:source:rmis:Patient")') value),
                                     base.resource #> '{subject,identifier}'))))
FROM pat_match
WHERE base.id = pat_match.id
RETURNING base.id;

SELECT jsonb_build_object('identifier', null)

--- drop index ---
DROP INDEX questionnaireresponse_resource_subject_ref_valid;

--- analyze table ---
VACUUM ANALYZE observation;


--- find enp not valid ---
SELECT *
FROM flag base
WHERE base.resource @@ 'subject.identifier.system = "urn:identity:insurance-gov:Patient"'::jsquery
    AND NOT enp_valid(base.resource #>> '{subject,identifier,value}')
    
--- working with urn:identity:insurance-gov:Patient only
WITH pat_match AS (
    SELECT base.id, jsonb_agg(p.resource) pat_resource
    FROM allergyintolerance base
    JOIN Patient p
      ON p.resource #>> '{name,0,given,0}' ILIKE split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',2)
         AND p.resource #>> '{name,0,given,1}' ILIKE split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',3)
         AND p.resource #>> '{name,0,family}' ILIKE split_part (split_part (base.resource #>> '{subject,display}',',',1),' ',1)
         AND p.resource #>> '{birthDate}' = to_date (split_part (base.resource #>> '{subject,display}',',',2),'DD.mm.YYYY')::text
    WHERE base.resource @@ 'subject.identifier.system = "urn:identity:insurance-gov:Patient"'::jsquery
          AND NOT enp_valid(base.resource #>> '{subject,identifier,value}')
    GROUP BY base.id
    HAVING count(*) = 1)
SELECT base.id, resource, jsonb_set(base.resource, '{subject,identifier,value}', (SELECT value FROM jsonb_path_query(pat_resource -> 0, '$.identifier ? (@.system == "urn:identity:insurance-gov:Patient").value') value WHERE enp_valid(value #>> '{}')))
FROM pat_match
JOIN allergyintolerance base ON base.id = pat_match.id
--UPDATE allergyintolerance base
--SET resource = COALESCE (jsonb_set(base.resource, '{subject,identifier,value}', (SELECT value FROM jsonb_path_query(pat_resource -> 0, '$.identifier ? (@.system == "urn:identity:insurance-gov:Patient").value') value WHERE enp_valid(value #>> '{}'))), resource)
--FROM pat_match
--WHERE base.id = pat_match.id
--RETURNING base.id    

SELECT c.ts, c.resource #>> '{identifier,0,value}', c.resource #>> '{identifier,0,system}', enc.resource #>> '{serviceProvider,display}'
FROM "condition" c
JOIN encounter enc ON enc.resource @@ logic_include(c.resource, 'encounter')
WHERE c.resource @@ 'subject.identifier.system = "urn:identity:insurance-gov:Patient"'::jsquery
    AND NOT enp_valid(c.resource #>> '{subject,identifier,value}')
ORDER BY c.ts DESC 
    
    
SELECT *
FROM integrationqueue i 
WHERE ts > current_date
    AND resource @@ 'payload.identifier.#.value = "1c5269e7-7d15-4e8c-b4b7-af78a09b653a"'::jsquery;

SELECT *
FROM pg_indexes
WHERE tablename = 'integrationqueue'
    
SELECT *
FROM patient p 
WHERE p.resource @@ 'identifier.#.value = "0000000000000000"'::jsquery