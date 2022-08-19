-- select patient with broken snils
SELECT resource -> 'identifier'
       , (SELECT jsonb_agg(value) 
          FROM (SELECT *
                FROM jsonb_array_elements(resource -> 'identifier') 
                WHERE NOT value ->> 'system' = 'urn:identity:snils:Patient'
                UNION
                SELECT *
                FROM jsonb_array_elements(resource -> 'identifier') 
                WHERE value ->> 'system' = 'urn:identity:snils:Patient'
                    AND snils_valid(value ->> 'value')) idfs)
FROM patient
WHERE resource @@ 'identifier.#.system = "urn:identity:snils:Patient"'::jsquery 
    AND NOT snils_valid(jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:snils:Patient")."value"') #>> '{}')
LIMIT 10

-- remove broken snils
WITH to_update AS (
    SELECT id,
           (SELECT jsonb_agg(value) 
            FROM (SELECT *
                  FROM jsonb_array_elements(resource -> 'identifier') 
                  WHERE NOT value ->> 'system' = 'urn:identity:snils:Patient'
                  UNION
                  SELECT *
                  FROM jsonb_array_elements(resource -> 'identifier') 
                  WHERE value ->> 'system' = 'urn:identity:snils:Patient'
                      AND snils_valid(value ->> 'value')) idfs) fixed_idfs
    FROM patient
    WHERE resource @@ 'identifier.#.system = "urn:identity:snils:Patient"'::jsquery 
        AND NOT snils_valid(jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:snils:Patient")."value"') #>> '{}')
)
UPDATE patient p
SET resource = jsonb_set(p.resource, '{identifier}', tu.fixed_idfs)
FROM to_update tu
WHERE p.id = tu.id