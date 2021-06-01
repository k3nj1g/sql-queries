--- check ---
SELECT id, jsonb_path_query_first (p.resource,'$.identifier[*] ? (@.system=="urn:identity:snils:Patient").value'), jsonb_path_query_first (p.resource,'$.identifier[*] ? (@.system=="urn:identity:enp:Patient").value')
FROM patient p 
WHERE p.resource @@ 'identifier.#.value in ("0000000000000000", "000-000-000 00")'::jsquery

--- update ---
UPDATE patient 
SET resource = jsonb_set(resource, '{identifier}', 
                         (WITH idfs AS (SELECT * FROM jsonb_array_elements(resource->'identifier'))
                          SELECT COALESCE (jsonb_agg(value), '""')
                          FROM idfs
                          WHERE NOT value ->> 'value' IN ('0000000000000000', '000-000-000 00')))
WHERE resource @@ 'identifier.#.value in ("0000000000000000", "000-000-000 00")'::jsquery
RETURNING id;

--- count ---
SELECT count(*)
FROM patient p 
WHERE p.resource @@ 'identifier.#(system = "urn:identity:snils:Patient" and value = "000-000-000 00")'::jsquery;

VACUUM ANALYSE patient;
