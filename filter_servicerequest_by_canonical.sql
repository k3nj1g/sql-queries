--- join lateral (1s)---
EXPLAIN ANALYSE 
SELECT count(*) 
FROM servicerequest sr
JOIN LATERAL (WITH
			  plan_url AS (
			  	SELECT split_part(link, '/', 2) url
				FROM jsonb_array_elements_text(sr.resource -> 'instantiatesCanonical') AS link
				WHERE split_part(link, '/', 1) = 'PlanDefinition'
			  )
			  SELECT 1
			  FROM plandefinition p 
              WHERE p.id = ANY (SELECT url 
              				    FROM plan_url)) pd ON true				  				
WHERE  
	sr.resource @@ 'identifier.#(system="urn:source:rmis:ServiceRequest") and status = completed and instantiatesCanonical = *'::jsquery
	AND sr.ts BETWEEN '2021-01-01' AND '2021-02-01'
	
--- exists subquery (bad)---
EXPLAIN ANALYSE 
WITH sr AS (
	SELECT *
	FROM servicerequest sr
	WHERE  
		sr.resource @@ 'identifier.#(system="urn:source:rmis:ServiceRequest") and status = completed and instantiatesCanonical = *'::jsquery
		AND sr.ts BETWEEN '2021-01-01' AND '2021-02-01')
SELECT count(*)
FROM sr, LATERAL (
	SELECT 1
	FROM plandefinition pd
	WHERE pd.id = ANY (
		SELECT url 
		FROM (SELECT split_part(link, '/', 1) resourcetype, split_part(link, '/', 2) url
			  FROM jsonb_array_elements_text(sr.resource -> 'instantiatesCanonical') AS link) canonical
        WHERE resourcetype = 'PlanDefinition')
	) AS pd
