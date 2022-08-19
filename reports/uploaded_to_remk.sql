EXPLAIN ANALYZE 
WITH grouped AS (
    SELECT jsonb_path_query_first(dr.resource, '$.author ? (@.type == "Organization").identifier.value') #>> '{}' org_oid
        , count(*) total
        , count(*) FILTER (WHERE (dr.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code = "AMB")'::jsquery)) AS osm
        , count(*) FILTER (WHERE (dr.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code = "IMI")'::jsquery)) AS dusl
        , count(*) FILTER (WHERE (dr.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code = "LMI")'::jsquery)) AS lis
        , count(*) FILTER (WHERE (dr.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code = "OPS")'::jsquery)) AS oper
        , count(*) FILTER (WHERE (dr.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code = "ST")'::jsquery)) AS stac
    FROM documentreference dr
    WHERE knife_extract_min_timestamptz(dr.resource, '[["date"]]') >= knife_date_bound ('2021-02-01','min')
      AND knife_extract_min_timestamptz(dr.resource, '[["date"]]') <= knife_date_bound ('2021-03-01','max')
      AND resource @@ 'identifier.#.system = "urn:source:rmis:DocumentReference" and category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code in ("AMB", "IMI", "LMI", "OPS", "ST"))'::jsquery
--      AND dr.resource -> 'identifier' @@ '#.system = "urn:source:rmis:DocumentReference"'::jsquery
--      AND dr.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code in ("AMB", "IMI", "LMI", "OPS", "ST"))'::jsquery
    GROUP BY 1
)
SELECT o.resource #>> '{alias,0}' mo, g.*
FROM grouped g
JOIN organization o
    ON o.resource @@ concat('identifier.#(system="urn:identity:oid:Organization" and value="', g.org_oid, '")')::jsquery
WHERE o.id = '70042e7c-cf2d-4a22-9d6a-c89444857b07'    
ORDER BY 2 DESC 

SELECT jsonb_path_query_first(dr.resource, '$.author ? (@.type == "Organization").identifier.value') #>> '{}' org_oid
    , jsonb_path_query_first(dr.resource, '$.category.coding ? (@.system == "urn:CodeSystem:medrecord-group").code') #>> '{}' category
    , count(*) cnt
FROM documentreference dr
WHERE knife_extract_min_timestamptz(dr.resource, '[["date"]]') >= knife_date_bound ('2021-02-01','min')
  AND knife_extract_min_timestamptz(dr.resource, '[["date"]]') <= knife_date_bound ('2021-02-02','max')
  AND resource @@ 'identifier.#.system = "urn:source:rmis:DocumentReference" and category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code in ("AMB", "IMI", "LMI", "OPS", "ST"))'::jsquery
GROUP BY 1,2

WITH grouped AS (
    SELECT jsonb_path_query_first(dr.resource, '$.author ? (@.type == "Organization").identifier.value') org_oid
    , jsonb_path_query_first(dr.resource, '$.category.coding ? (@.system == "urn:CodeSystem:medrecord-group").code') #>> '{}' category
    , count(*) cnt
    FROM documentreference dr
    WHERE knife_extract_min_timestamptz(dr.resource, '[["date"]]') >= knife_date_bound ('2021-02-01','min')
      AND knife_extract_min_timestamptz(dr.resource, '[["date"]]') <= knife_date_bound ('2021-02-02','max')
      AND resource @@ 'identifier.#.system = "urn:source:rmis:DocumentReference" and category.#.coding.#(system = "urn:CodeSystem:medrecord-group" and code in ("AMB", "IMI", "LMI", "OPS", "ST"))'::jsquery
    GROUP BY 1,2
)
SELECT o.resource #>> '{alias,0}' mo, g.*
FROM grouped g
JOIN organization o
    ON o.resource @@ concat('identifier.#(system="urn:identity:oid:Organization" and value=', g.org_oid, ')')::jsquery
--WHERE o.id = '70042e7c-cf2d-4a22-9d6a-c89444857b07'    
ORDER BY 2 DESC 