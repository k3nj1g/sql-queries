SELECT id, resource, jsonb_set(resource, '{extension}', 
    (SELECT COALESCE (jsonb_agg(agg.extn), '[]'::jsonb)
     FROM (
        SELECT extn
        FROM jsonb_array_elements(resource -> 'extension') extn
        WHERE NOT (extn ->> 'url' = 'urn:extension:patient-type' AND extn ->> 'valueCode' = 'newborn')
        ) agg)
) 
FROM patient p 
WHERE resource @@ 'identifier.#.system = "urn:source:tfoms:Patient" and extension.#(url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery;

UPDATE patient 
SET resource = jsonb_set(resource, '{extension}', 
    (SELECT COALESCE (jsonb_agg(agg.extn), '[]'::jsonb)
     FROM (
        SELECT extn
        FROM jsonb_array_elements(resource -> 'extension') extn
        WHERE NOT (extn ->> 'url' = 'urn:extension:patient-type' AND extn ->> 'valueCode' = 'newborn')
        ) agg)
) 
WHERE resource @@ 'identifier.#.system = "urn:source:tfoms:Patient" and extension.#(url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery
RETURNING id;


