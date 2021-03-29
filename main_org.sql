SELECT o.resource #>> '{alias, 0}', jsonb_path_query_first(o.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}'
FROM organization o 
WHERE o.resource -> 'partOf' IS NULL AND COALESCE (o.resource ->> 'active', 'true') = 'true'

