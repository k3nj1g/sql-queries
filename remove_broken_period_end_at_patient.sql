SELECT count(*) 
FROM patient
WHERE resource @@ 'identifier.#.period.end = "2079-06-06"'::jsquery

-- select patient with broken period.end
SELECT id, 
       jsonb_set(resource, '{identifier}', 
                (SELECT jsonb_agg(value) 
                 FROM (SELECT *
                     FROM jsonb_array_elements(resource -> 'identifier') 
                     WHERE NOT value #>> '{period,end}' = '2079-06-06' OR value #>> '{period,end}' IS NULL 
                     UNION
                     SELECT value || jsonb_set(value, '{period}', (value -> 'period')- 'end')
                     FROM jsonb_array_elements(resource -> 'identifier') 
                     WHERE value #>> '{period,end}' = '2079-06-06') idfs)) resource
FROM patient
WHERE resource @@ 'identifier.#.period.end = "2079-06-06"'::jsquery

-- remove broken period.end
WITH to_update AS (
    SELECT id, 
           jsonb_set(resource, '{identifier}', 
                    (SELECT jsonb_agg(value) 
                     FROM (SELECT *
                         FROM jsonb_array_elements(resource -> 'identifier') 
                         WHERE NOT value #>> '{period,end}' = '2079-06-06' OR value #>> '{period,end}' IS NULL 
                         UNION
                         SELECT value || jsonb_set(value, '{period}', (value -> 'period')- 'end')
                         FROM jsonb_array_elements(resource -> 'identifier') 
                         WHERE value #>> '{period,end}' = '2079-06-06') idfs)) resource
    FROM patient
    WHERE resource @@ 'identifier.#.period.end = "2079-06-06"'::jsquery
)
UPDATE patient p
SET resource = tu.resource
FROM to_update tu
WHERE p.id = tu.id
RETURNING p.id
