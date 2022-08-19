SELECT c.id, c.resource, prr.resource, prr.id,
    jsonb_set(
      prr.resource,
      '{code}',
      jsonb_build_array(
        jsonb_set(
          prr.resource #> '{code,0}',
          '{coding}', 
          jsonb_build_array(
            jsonb_build_object(
              'code',   jsonb_path_query_first(prr.resource, '$.identifier ? (@.system == "urn:identity:frmr:PractitionerRole").position.code'),
              'system', 'urn:CodeSystem:frmr.position'),
            jsonb_build_object(
              'code',   c.resource #>> '{property,Код из справочника 1.2.643.5.1.13.13.11.1002}',
              'system', 'urn:CodeSystem:1002.position')
          ))))
--    jsonb_path_query_first(resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code')
--    , jsonb_path_query_first(resource, '$.code.coding ? (@.system == "urn:CodeSystem:1002.position").code')    
--    , jsonb_path_query_first(resource, '$.identifier ? (@.system == "urn:identity:frmr:PractitionerRole").position.code')
FROM practitionerrole prr
JOIN concept c 
    ON c.resource ->> 'system' = 'urn:CodeSystem:frmr.position' 
        AND c.resource ->> 'code' = jsonb_path_query_first(prr.resource, '$.identifier ? (@.system == "urn:identity:frmr:PractitionerRole").position.code') #>> '{}'
WHERE prr.resource @@ 'identifier.#(system = "urn:identity:frmr:PractitionerRole" and not period.end = * and position.code = *)'::jsquery

WITH to_update AS (
    SELECT prr.id,
        jsonb_set(
          prr.resource,
          '{code}',
          jsonb_build_array(
            jsonb_set(
              prr.resource #> '{code,0}',
              '{coding}', 
              jsonb_build_array(
                jsonb_build_object(
                  'code',   COALESCE(jsonb_path_query_first(prr.resource, '$.identifier ? (@.system == "urn:identity:frmr:PractitionerRole").position.code'), '""'),
                  'system', 'urn:CodeSystem:frmr.position'),
                jsonb_build_object(
                  'code',   COALESCE(c.resource #>> '{property,Код из справочника 1.2.643.5.1.13.13.11.1002}', '""'),
                  'system', 'urn:CodeSystem:1002.position'))))) AS resource
    FROM practitionerrole prr
    JOIN concept c 
        ON c.resource ->> 'system' = 'urn:CodeSystem:frmr.position' 
        AND c.resource ->> 'code' = jsonb_path_query_first(prr.resource, '$.identifier ? (@.system == "urn:identity:frmr:PractitionerRole").position.code') #>> '{}'
    WHERE prr.resource @@ 'identifier.#(system = "urn:identity:frmr:PractitionerRole" and not period.end = * and position.code = *)'::jsquery)
UPDATE practitionerrole prr
SET resource = tu.resource
FROM to_update tu
WHERE prr.id = tu.id