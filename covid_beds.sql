SELECT m.resource #>> '{reporter,display}' AS mo, count(*) AS beds
FROM measurereport m 
WHERE m.resource ->> 'measure' = 'urn:Measure:r21.hosp-free-beds'
GROUP BY m.resource #>> '{reporter,identifier,value}', m.resource #>> '{reporter,display}'

SELECT --m.resource #>> '{reporter,display}' AS mo
	 sum((m.resource #>> '{group,0,measureScore,value}')::integer) --, jsonb_agg(m.resource -> 'group') 
FROM measurereport m
JOIN healthcareservice hcs ON hcs.resource @@ logic_include(m.resource, 'evaluatedResource[*]') OR hcs.id = any(array(SELECT jsonb_path_query(m.resource, '$.evaluatedResource[*].id') #>> '{}'))
WHERE m.resource ->> 'measure' = 'urn:Measure:r21.hosp-free-beds'
	AND hcs.resource ->> 'isCovid19Department' = 'true'
	AND m.resource @@ 'group.#.measureScore.value > 0'::jsquery
	AND (m.resource ->> 'date')::timestamp > current_date
--GROUP BY m.resource #>> '{reporter,identifier,value}', m.resource #>> '{reporter,display}'

SELECT *
FROM measurereport m
WHERE m.resource @@ 'group.#.population.#.code.coding.#.code = "additional"'::jsquery
