EXPLAIN ANALYZE
SELECT *
FROM servicerequest res
JOIN patient ptn 
	ON res.resource @@ logic_revinclude(pt.resource, pt.id, 'subject')::jsquery
JOIN organization pfo 
	ON res.resource @@ logic_revinclude(pfo.resource, pfo.id, 'performer.#')::jsquery
WHERE pt.id = '8cb18bba-d7f1-4699-8a11-455f5033fae8'
--	AND NOT res.resource @> '{\"code\":{\"coding\":[{\"system\":\"urn:CodeSystem:rmis:ServiceRequest\",\"code\":\"PGI\"}]}}'::jsonb
	AND res.resource#>>'{priority}' IN(SELECT (UNNEST(string_to_array('"urgent,asap,stat"', ','))))
--	AND res.resource @> ?::jsonb
--	AND pfo.id = ? 
--	AND res.resource @@ concat('category.#.coding.#(system = "urn:CodeSystem:lab-group" AND NOT code IN("', REPLACE(?, ',', '","') , '"))')::jsquery
	AND (res.id || ' ' || res.resource::TEXT) ILIKE ALL (ARRAY(SELECT format('%%%s%%', t.val) FROM UNNEST(string_to_array(?, ' ')) AS t(val)))
ORDER BY knife_extract_max_timestamptz(res.resource, $$[["authoredOn"]]$$) DESC
LIMIT 10

SELECT s.resource 
FROM organization o
JOIN servicerequest s ON s.resource @@ logic_revinclude(o.resource, o.id, 'performer.#')
	AND s.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")
					   and category.#.coding.#(system = "urn:CodeSystem:lab-group" and not code in ("501","601","602")) 
					   and code.coding.#(system = "urn:CodeSystem:rmis:ServiceRequest" and not code = "PGI")
					   '::jsquery
	AND (s.id || ' ' || s.resource::TEXT) ILIKE ALL (ARRAY(SELECT format('%%%s%%', t.val) FROM UNNEST(string_to_array('ßÇÛÊÎÂÀ', ' ')) AS t(val)))
WHERE o.id = '1150e915-f639-4234-a795-1767e0a0be5f'	
ORDER BY s.resource ->> 'authoredOn' DESC
LIMIT 10

--EXPLAIN ANALYSE 
WITH d AS (SELECT s.id --s.resource 
FROM servicerequest s 
--JOIN patient p ON p.resource @@ logic_include(s.resource, 'subject') OR p.id = any(array(SELECT jsonb_path_query(s.resource, '$.subject.id') #>> '{}'))
JOIN organization o ON (o.resource @@ logic_include(s.resource, 'performer[*]', NULL) OR o.id = any(array(SELECT jsonb_path_query(s.resource, '$.performer[*].id') #>> '{}')))
	AND s.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")
					   and category.#.coding.#(system = "urn:CodeSystem:lab-group" and not code in ("501","601","602")) 
					   and code.coding.#(system = "urn:CodeSystem:rmis:ServiceRequest" and not code = "PGI")'
--					   and priority in ("urgent","asap","stat")
					   ::jsquery 
--	AND (s.id || ' ' || s.resource::TEXT) ILIKE ALL (ARRAY(SELECT format('%%%s%%', t.val) FROM UNNEST(string_to_array('ßÇÛÊÎÂÀ', ' ')) AS t(val)))
WHERE o.id = 'ff0f409e-ce00-4707-9e44-d8e493cde996'	
--	AND s.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")
--				   and category.#.coding.#(system = "urn:CodeSystem:lab-group" and not code in ("501","601","602")) 
--				   and code.coding.#(system = "urn:CodeSystem:rmis:ServiceRequest" and not code = "PGI")'
--				  ::jsquery
ORDER BY s.resource ->> 'authoredOn' DESC
LIMIT 10
),
p AS (--EXPLAIN ANALYZE
SELECT res.id /*res.id*/ FROM "servicerequest" res
--JOIN "organization" pfo ON res.resource @@ logic_revinclude(pfo.resource, pfo.id, 'performer.#', NULL)
WHERE /* performer_organization_id */ res.id = 'ff0f409e-ce00-4707-9e44-d8e493cde996' AND
/* resource_jsquery */  res.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")
				and category.#.coding.#(system = "urn:CodeSystem:lab-group" and not code in ("501","601","602"))                
				and code.coding.#(system = "urn:CodeSystem:rmis:ServiceRequest" and not code = "PGI")
			    '::jsquery
			    )
SELECT *
FROM d
LEFT OUTER JOIN p ON d.id = p.id
WHERE p.id IS NULL
--WHERE d.c1 <> d.c2
--ORDER BY res.resource ->> 'authoredOn' DESC
--LIMIT 10

SELECT resource -> 'performer'
FROM servicerequest s 
WHERE resource @@ 'performer.#.identifier.value = "1.2.643.5.1.13.13.12.2.21.1537"'::jsquery
LIMIT 3

SELECT resource -> 'performer', resource -> 'category', resource -> 'code'
FROM servicerequest s 
WHERE id IN ('5a1917ef-a057-49a7-852f-7a0a45260a2a', '6672753d-8ced-4223-a288-59e38083d762', 'e87e7af0-a392-4b63-a1d0-42415ea74d0b')

SELECT resource -> 'performer'
FROM servicerequest s 
WHERE s.resource @@ 'performer.#.identifier = *'::jsquery
LIMIT 10

SELECT res.id /*res.id*/ FROM "servicerequest" res
--JOIN "organization" pfo ON res.resource @@ logic_revinclude(pfo.resource, pfo.id, 'performer.#', NULL)
WHERE /* performer_organization_id */ 
/* resource_jsquery */  res.resource @@ '
				category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")
				and category.#.coding.#(system = "urn:CodeSystem:lab-group" and not code in ("501","601","602"))                
				and code.coding.#(system = "urn:CodeSystem:rmis:ServiceRequest" and not code = "PGI")
				and performer.#.identifier(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1537")
			    '::jsquery
ORDER BY res.resource ->> 'authoredOn' DESC
LIMIT 10			    
