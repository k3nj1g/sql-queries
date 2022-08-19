WITH to_update AS (
    SELECT DISTINCT prr.id
    FROM practitionerrole prr
    JOIN organization o ON o.resource @@ logic_include(prr.resource, 'organization')
        AND o.resource @@ 'identifier.#.system = "urn:identity:oid:Organization"'::jsquery
    JOIN concept c ON c.resource @@ 'system = "urn:CodeSystem:frmo-1.2.643.5.1.13.13.99.2.114"'::jsquery
        AND c.resource #>> '{code}' = jsonb_path_query_first(o.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}'
        AND c.resource #>> '{property,depart_type_id}' = '4'
    WHERE prr.resource::TEXT LIKE '%Медицинская сестра%'
        AND prr.resource @@ 'active = true'::jsquery
)
--SELECT jsonb_set(prr.resource, '{code,0,coding,0,code}', '"159"'::jsonb) 
--FROM to_update tu
--JOIN practitionerrole prr
--    ON prr.id = tu.id
UPDATE practitionerrole prr
SET resource = jsonb_set(prr.resource, '{code,0,coding,0,code}', '"159"'::jsonb)
FROM to_update tu
WHERE prr.id = tu.id
RETURNING prr.id

WITH to_update AS (
    SELECT DISTINCT id, jsonb_set(resource, '{code,0,coding,0,code}', '"145"'::jsonb) resource
    FROM practitionerrole
    WHERE resource::TEXT LIKE '%Заведующий фельдшерско-акушерским пунктом - фельдшер%'
        AND resource @@ 'active = true'::jsquery
)
UPDATE practitionerrole prr
SET resource = tu.resource
FROM to_update tu
WHERE prr.id = tu.id
RETURNING prr.id, prr.resource
