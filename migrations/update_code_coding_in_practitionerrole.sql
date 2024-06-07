WITH to_update AS (
  SELECT prr.id
    , jsonb_set_lax(
        prr.resource
        , '{code}'
        , (SELECT jsonb_agg(
            jsonb_set_lax(
               codeable_concept
               , '{coding}'
               , (SELECT jsonb_agg(
                    CASE 
                      WHEN coding->>'system'='urn:CodeSystem:V021.speciality' 
                      THEN jsonb_set_lax(coding, '{code}', c.resource #> '{property,IDSPEC}')
                      ELSE coding
                    END)
                  FROM jsonb_array_elements(codeable_concept->'coding') codings(coding))))
           FROM jsonb_array_elements(prr.resource->'code') codes(codeable_concept))) resource
  FROM practitionerrole prr
  JOIN concept c 
    ON c.resource #>> '{system}' = 'urn:CodeSystem:V021.speciality'
      AND c.resource #>> '{code}' = jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system=="urn:CodeSystem:V021.speciality").code') #>> '{}'
  WHERE prr.resource @@ 'code.#.coding.#(system="urn:CodeSystem:V021.speciality" and code)'::jsquery)
UPDATE practitionerrole prr
SET resource = tu.resource
FROM to_update tu
WHERE tu.id = prr.id
RETURNING *;