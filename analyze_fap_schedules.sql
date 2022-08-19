SELECT s.resource #>> '{mainOrganization,display}' org_name, c.resource ->> 'display' fap_name, c.resource ->> 'code' fap_oid
FROM concept c 
JOIN "location" l
    ON l.resource @@ concat('identifier.#.value="', c.resource ->> 'code', '"')::jsquery
JOIN schedulerule s 
    ON s.resource @@ logic_revinclude(l.resource, l.id, 'location') 
    AND immutable_ts(COALESCE ((s.resource #>> '{planningHorizon,end}'), 'infinity')) >= LOCALTIMESTAMP
WHERE c.resource @@ 'property.depart_kind_id in ("1166","1167")'::jsquery
ORDER BY org_name, fap_name

SELECT * 
FROM "location"
WHERE id = 'af0005e1-920d-483b-9738-b07a13a62756'

SELECT *
FROM pg_indexes
WHERE tablename = 'location'

SELECT *
FROM schedulerule s 
LIMIT 10