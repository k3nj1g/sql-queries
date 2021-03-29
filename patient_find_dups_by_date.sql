--- final --- 
SELECT p.id
	, p.resource
	, jsonb_agg(d.*) 	
FROM patient p
JOIN patient d ON
--    d.resource @@ concat('identifier.#(system = "urn:identity:snils:Patient" and value = "', jsonb_extract_path_text(jsonb_path_query_first(p.resource,'$.identifier [*] ? (@.system == "urn:identity:snils:Patient")'), 'value'), '")')::jsquery
	jsonb_path_query_first(d.resource,'$.identifier[*] ? (@.system == "urn:identity:snils:Patient").value') = jsonb_path_query_first(p.resource,'$.identifier [*] ? (@.system == "urn:identity:snils:Patient").value')
	AND d.resource ->> 'birthDate' = p.resource ->> 'birthDate'
	AND d.id <> p.id
	AND coalesce (d.resource->>'active', 'true') = 'true'
WHERE p.ts > DATE 'yesterday'
	AND coalesce (p.resource->>'active', 'true') = 'true'
GROUP BY p.id
---

SELECT * FROM
  (SELECT *, count(*)
  OVER
    (PARTITION BY
      firstname,
      lastname
    ) AS count
  FROM people) tableWithCount
  WHERE tableWithCount.count > 1;	

SELECT dups.p_id
FROM (SELECT p.id p_id, count(*) OVER(PARTITION BY jsonb_path_query_first(p.resource,'$.identifier [*] ? (@.system == "urn:identity:snils:Patient").value'), p.resource ->> 'birthDate') AS "row"
FROM patient p) dups
WHERE dups."row" > 1
GROUP BY dups.p_id

SELECT id
	, jsonb_path_query_array(p.resource, '$.identifier ? (@.system == "urn:source:rmis:Patient").value')	
	, array_to_string(ARRAY (SELECT jsonb_path_query(p.resource, '$.identifier [*] ? (@.system == "urn:source:rmis:Patient").value')), ',')
FROM patient p
WHERE jsonb_array_length(jsonb_path_query_array(p.resource, '$.identifier ? (@.system == "urn:source:rmis:Patient")')) > 1
LIMIT 10


SELECT jsonb_agg(p.*) AS pts
FROM (SELECT * FROM patient p ORDER BY p.id) p
WHERE p.resource @@ 'identifier.#(system="urn:identity:snils:Patient" and not value="000-000-000 00")'::jsquery
	AND coalesce (p.resource->>'active', 'true') = 'true'
GROUP BY jsonb_path_query_first(p.resource,'$.identifier [*] ? (@.system == "urn:identity:snils:Patient").value'),
         p.resource ->> 'birthDate'
HAVING count(*) > 1