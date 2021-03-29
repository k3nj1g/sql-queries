--select count(*) from patient p
--JOIN personbinding pb ON NOT pb.resource @@ logic_revinclude(p.resource, p.id, 'subject')
--WHERE resource @@ 'address.#(not (type = *))'::jsquery

select count(*) from patient p
LEFT JOIN personbinding pb ON pb.resource @@ logic_revinclude(p.resource, p.id, 'subject')
WHERE pb.id IS NULL AND p.resource @@ 'address.#(not (type = *))'::jsquery

--test1-- не работает
EXPLAIN ANALYSE 
select count(*) from patient  
WHERE resource @@ 'address.#(not (type = *))'::jsquery

--test2--
EXPLAIN ANALYSE 
select * from patient  
WHERE NOT resource @@ 'address.#(type = *)'::jsquery

--test3--
select count(*) from patient  
WHERE knife_extract_text(resource, '[["address", {}, "type"]]') IS null

--test4--
EXPLAIN ANALYSE 
SELECT count(*) from patient  
WHERE jsonb_path_exists(resource, '$.address[*] ? (!(@.type == "both") && @.use == "home")')

--test5--
EXPLAIN ANALYSE 
SELECT count(*) from patient  
WHERE NOT resource @? '$.address[*].type'::jsonpath AND resource ?? 'address' 

UPDATE patient 
SET resource = jsonb_set(resource, 
                            '{address}', 
                            (SELECT jsonb_agg(jsonb_insert(value, '{type}', CASE WHEN value ->> 'use' = 'home' THEN '"both"'::jsonb WHEN value->> 'use' = 'temp' THEN '"physical"'::jsonb ELSE '' END)::jsonb) 
                             FROM jsonb_array_elements(resource -> 'address')))
WHERE resource @@ 'address.#(not (type = *))'::jsquery
RETURNING resource 

-- v2
UPDATE patient 
SET resource = jsonb_set(resource, 
                            '{address}', 
                            (SELECT jsonb_agg(agg)
                            FROM (
	                     		SELECT jsonb_insert(address , '{type}', 'both') 
	                     		FROM jsonb_array_elements(resource -> 'address') address 
	                     		WHERE address ->> 'use' == 'home'
	                     		UNION
	                     		SELECT jsonb_insert(address, '{type}', 'physical') 
	                     		FROM jsonb_array_elements(resource -> 'address') address  
	                     		WHERE address ->> 'use' == 'temp') agg)
WHERE resource @@ 'address.#(not (type = *))'::jsquery
RETURNING resource 


SELECT *
--jsonb_agg(jsonb_insert(value, '{type}', CASE WHEN value ->> 'use' = 'home' THEN '"both"'::jsonb WHEN value->> 'use' = 'temp' THEN '"physical"'::jsonb END)::jsonb) 
FROM jsonb_array_elements('[
    {
      "use": "home",
      "fias": "3965BDD7-86F6-4846-A89F-10C9C0937BBF",
      "text": "Чувашская Республика - Чувашия, Алатырь г, 3 Интернационала ул, дом 31, кв. 225",
      "kladr": "21000022000001400",
      "apartment": "225",
      "houseNumber": "31"
    },
    {
      "use": "temp",
      "fias": "3965BDD7-86F6-4846-A89F-10C9C0937BBF",
      "text": "Чувашская Республика - Чувашия, Алатырь г, 3 Интернационала ул, дом 31, кв. 225",
      "kladr": "21000022000001400",
      "apartment": "225",
      "houseNumber": "31"
    }
  ]'::jsonb) 

SELECT id, resource -> 'address',
	jsonb_set(resource,   '{address}',   (SELECT jsonb_agg(agg.ins)
    FROM (
 		SELECT jsonb_insert(address , '{type}', '"both"') ins
 		FROM jsonb_array_elements(resource -> 'address') address 
 		WHERE address ->> 'use' = 'home'
 		UNION
 		SELECT jsonb_insert(address, '{type}', '"physical"') ins
 		FROM jsonb_array_elements(resource -> 'address') address  
 		WHERE address ->> 'use' = 'temp'
 		UNION
 		SELECT address ins
 		FROM jsonb_array_elements(resource -> 'address') address  
 		WHERE address ->> 'use' NOT IN ('home', 'temp') OR NOT address ? 'use'
 		) agg))
FROM patient 
WHERE id = 'd3bb9b69-9871-4ac4-917d-62e90a150a54'

WITH addrs AS (SELECT jsonb_array_elements(resource -> 'address') address  
FROM patient
WHERE id = 'd3bb9b69-9871-4ac4-917d-62e90a150a54')
SELECT *
FROM addrs
WHERE address ->> 'use' NOT IN ('home', 'temp') OR NOT address ? 'use'

SELECT *
FROM patient
WHERE resource @@ ''::jsquery
LIMIT 1

select count(*) from patient  
WHERE resource @@ 'address.#(use = "home" and not type = "both")'::jsquery

--- migration ---
UPDATE patient 
SET resource = jsonb_set(resource, 
                         '{address}', 
                         (SELECT jsonb_agg(addresses.ins)
                          FROM (SELECT jsonb_insert(address , '{type}', '"both"') ins
								FROM jsonb_array_elements(resource -> 'address') address 
								WHERE address ->> 'use' = 'home'
								UNION
								SELECT jsonb_insert(address, '{type}', '"physical"') ins
								FROM jsonb_array_elements(resource -> 'address') address  
								WHERE address ->> 'use' = 'temp'
								UNION
								SELECT address ins
 								FROM jsonb_array_elements(resource -> 'address') address  
								WHERE address ->> 'use' NOT IN ('home', 'temp') OR NOT address ? 'use') addresses))
WHERE jsonb_array_length(resource -> 'address') > 0 AND NOT resource @@ 'address.#(type = *)'::jsquery
RETURNING resource
