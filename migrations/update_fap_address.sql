WITH to_update AS
(
  SELECT l.id, fap."location",
         jsonb_set(l.resource,'{address}',jsonb_build_object('text',fap.location)) resource
  FROM "location" l
    JOIN "temp".fap_location fap ON fap.id = ANY (ARRAY (SELECT jsonb_path_query(l.resource,'$.identifier ? (@.system=="urn:source:tfoms:Location").value') #>> '{}'))
    AND length(fap."location") > 1
  WHERE NOT l.resource @@ 'address'::jsquery
) UPDATE "location" l
   SET resource = tu.resource
FROM to_update tu
WHERE tu.id = l.id 
RETURNING l.*;
