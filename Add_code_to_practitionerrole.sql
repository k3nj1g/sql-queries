SELECT jsonb_set(prr.resource, '{code,0}', prr.resource #> '{code,0,coding}' || jsonb_build_object('system', 'urn:CodeSystem:1002.position', 'code', c.resource #>> '{property,federal_code}'))
FROM practitionerrole prr
JOIN concept c 
  ON c.resource #>> '{system}' = 'urn:CodeSystem:frmr.position'
     AND c.resource #>> '{code}' = jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system=="urn:CodeSystem:frmr.position").code') #>> '{}'
WHERE prr.resource @@ 'active=true and code.#.coding.#.system="urn:CodeSystem:frmr.position"'::jsquery

---
WITH to_update AS (
    SELECT prr.id, jsonb_set(prr.resource, '{code,0,coding}', prr.resource #> '{code,0,coding}' || jsonb_build_object('system', 'urn:CodeSystem:1002.position', 'code', c.resource #>> '{property,federal_code}')) resource
    FROM practitionerrole prr
    JOIN concept c 
      ON c.resource #>> '{system}' = 'urn:CodeSystem:frmr.position'
         AND c.resource #>> '{code}' = jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system=="urn:CodeSystem:frmr.position").code') #>> '{}'
    WHERE prr.resource @@ 'active=true and code.#.coding.#.system="urn:CodeSystem:frmr.position"'::jsquery
)
UPDATE practitionerrole prr
SET resource = tu.resource
FROM to_update tu
WHERE prr.id = tu.id
RETURNING prr.id

--- fix value ->> code
SELECT id 
       , jsonb_set(resource, '{code,0,coding}', 
                   (SELECT jsonb_agg(coding) 
                    FROM (SELECT jsonb_build_object('code', value->>'value', 'system', value->>'system') AS coding
                          FROM jsonb_array_elements(resource #> '{code,0,coding}') 
                          WHERE value @@ 'system = "urn:CodeSystem:1002.position"'::jsquery
                          UNION 
                          SELECT jsonb_set(value, '{identifier,system}', '"urn:identity:snils:Practitioner"') AS coding
                          FROM jsonb_array_elements(resource #> '{code,0,coding}') 
                          WHERE NOT value @@ 'system = "urn:CodeSystem:1002.position"'::jsquery
) codings)) resource
FROM practitionerrole prr
WHERE prr.resource @@ 'active=true and code.#.coding.#.system="urn:CodeSystem:1002.position"'::jsquery
