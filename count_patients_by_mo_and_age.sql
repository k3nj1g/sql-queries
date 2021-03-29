WITH addr AS 
	(SELECT date_part('year', age('2020-06-01'::timestamp, CAST(p.resource ->> 'birthDate' AS timestamp))) p_age, concat(left(f.okato, 5), '000000') okato, count(p.*) p_count
	FROM patient p 
	JOIN fias f 
		ON f.aoguid = CAST(COALESCE(jsonb_path_query_first(p.resource, '$.address ? (@.use == "temp").fias'), jsonb_path_query_first(p.resource, '$.address ? (@.use == "home").fias')) #>> '{}' AS uuid)
		AND f.is_actual
	WHERE date_part('year', age('2020-06-01'::timestamp, CAST(p.resource ->> 'birthDate' AS timestamp))) BETWEEN 0 AND 12 
		AND COALESCE(p.resource ->> 'active', 'true') = 'true'
		AND jsonb_array_length(p.resource -> 'address') > 0
		AND NOT p.resource -> 'deceased' ?? 'datetime'
	GROUP BY left(f.okato, 5), p_age)
SELECT addr.okato, f.name, addr.p_age, addr.p_count
FROM addr 
LEFT JOIN (
	SELECT DISTINCT ON (name) * 
	FROM fias
	WHERE address_type IN ('ã', 'ð-í') AND parentguid = '878fc621-3708-46c7-a97f-5a13a4176b3e' AND okato LIKE '%000000'
	) f ON f.okato = addr.okato
WHERE f.parentguid = '878fc621-3708-46c7-a97f-5a13a4176b3e'
