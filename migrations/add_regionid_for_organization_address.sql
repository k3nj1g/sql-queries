UPDATE organization org
SET resource = jsonb_set_lax(resource, '{address}', (
  SELECT jsonb_agg(jsonb_set_lax(addr, '{regionId}', to_jsonb(substr(f.kladr, 1, 2)), TRUE, 'delete_key'))
  FROM jsonb_array_elements(resource->'address') addrs(addr)
  LEFT JOIN LATERAL (SELECT * FROM fias f WHERE f.aoguid = (addr->>'fias')::TEXT::uuid LIMIT 1) f ON true))
WHERE org.resource @@ 'address.#(not regionId=* and fias=*)'::jsquery
RETURNING *;