SELECT o.resource #>> '{alias, 0}' "name"
       , jsonb_path_query_first(o.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' "oid"
       , jsonb_path_query_first(o.resource, '$.address ? (@.type == "physical").text') #>> '{}' address
FROM organization o 
WHERE o.resource -> 'partOf' IS NULL AND COALESCE (o.resource ->> 'active', 'true') = 'true'