--EXPLAIN ANALYZE 
WITH deprecated_locations AS (
  SELECT DISTINCT
    (knife_extract_text (l.resource,$$[[ "identifier",{ "system": "urn:identity:oid:Location" }, "value"]] $$))[1] AS "oid"
    , (knife_extract_text(morg.resource,$$[[ "identifier",{ "system": "urn:identity:oid:Organization" }, "value"]] $$))[1] AS oid_mo
  FROM concept c
  JOIN "location" l ON l.resource @@ concat('identifier.#(system="urn:identity:oid:Location" and value=', c.resource->'code', ') and type.#.coding.#.code = "BLD"')::jsquery
  JOIN organization morg ON morg.id = l.resource #>> '{mainOrganization,id}'
  WHERE c.resource #>> '{system}' = 'urn:CodeSystem:frmo-1.2.643.5.1.13.13.99.2.115'
    AND COALESCE(c.resource ->> 'deprecated', 'false') = 'true')
SELECT dl."oid" AS oid_from
  , mappings."mapping" ->> 'to' oid_to
  , dl.oid_mo
FROM deprecated_locations dl
JOIN (
  SELECT jsonb_array_elements(resource #> '{property,mapping}') "mapping"
  FROM auxiliaryconfig aux 
  WHERE aux.id = 'location-remapping') mappings
  ON mappings."mapping" ->> 'from' = dl.oid_mo
  
