--- init
SELECT count(*)
FROM (
	SELECT 
--        ROW_NUMBER () OVER (PARTITION BY subspecies.id ORDER BY subspecies.id) row_num,
		to_jsonb(subspecies) AS subspecies,
        to_jsonb(research) AS research,
        to_jsonb(test) AS test 
	FROM plandefinition subspecies
	INNER JOIN activitydefinition research ON research.id = ANY (
         SELECT split_part(actions.value #>> '{definition,canonical}', '/', 2)
         FROM (
         	SELECT jsonb_array_elements(subspecies.resource -> 'action') AS value
         ) AS actions)
	INNER JOIN observationdefinition test
		ON (test.resource @@ logic_include (research.resource,'observationResultRequirement')
	    OR test.id = ANY (array ( (SELECT jsonb_path_query(research.resource, '$.observationResultRequirement.id') #>> '{}')))) 
	WHERE subspecies.resource @@ 'type.coding.#(code="ESLI.TYPE_LI.3" and system="urn:CodeSystem:plandefinition-li-type")'::jsquery) "rows"

--- with limit 10
SELECT *
FROM (
	SELECT 
		to_jsonb(subspecies) AS subspecies,
        to_jsonb(research) AS research,
        to_jsonb(test) AS test
	FROM (SELECT *
		  FROM plandefinition 
          WHERE resource @@ 'type.coding.#(code="ESLI.TYPE_LI.3" and system="urn:CodeSystem:plandefinition-li-type")'::jsquery
		  AND resource::TEXT ILIKE '%яд%'
          LIMIT 10) subspecies
	LEFT JOIN activitydefinition research ON research.id = ANY (
         SELECT split_part(actions.value #>> '{definition,canonical}', '/', 2)
         FROM (
         	SELECT jsonb_array_elements(subspecies.resource -> 'action') AS value
         ) AS actions)
	INNER JOIN observationdefinition test
		ON (test.resource @@ logic_include (research.resource,'observationResultRequirement')
	    OR test.id = ANY (array ( (SELECT jsonb_path_query(research.resource, '$.observationResultRequirement.id') #>> '{}'))))) "rows"
UNION 
SELECT *
FROM (
	SELECT 
		to_jsonb(subspecies) AS subspecies,
        to_jsonb(research) AS research,
        to_jsonb(test) AS test
	FROM (SELECT *
		  FROM plandefinition 
          WHERE resource @@ 'type.coding.#(code="ESLI.TYPE_LI.3" and system="urn:CodeSystem:plandefinition-li-type")'::jsquery
		  LIMIT 10) subspecies
	LEFT JOIN activitydefinition research ON research.id = ANY (
         SELECT split_part(actions.value #>> '{definition,canonical}', '/', 2)
         FROM (
         	SELECT jsonb_array_elements(subspecies.resource -> 'action') AS value
         ) AS actions)
    	 AND research.resource::TEXT ILIKE '%яд%'
	INNER JOIN observationdefinition test
		ON (test.resource @@ logic_include (research.resource,'observationResultRequirement')
	    OR test.id = ANY (array ( (SELECT jsonb_path_query(research.resource, '$.observationResultRequirement.id') #>> '{}'))))) "rows"
UNION 
SELECT *
FROM (
	SELECT 
		to_jsonb(subspecies) AS subspecies,
        to_jsonb(research) AS research,
        to_jsonb(test) AS test
	FROM (SELECT *
		  FROM plandefinition 
          WHERE resource @@ 'type.coding.#(code="ESLI.TYPE_LI.3" and system="urn:CodeSystem:plandefinition-li-type")'::jsquery
		  LIMIT 10) subspecies
	LEFT JOIN activitydefinition research ON research.id = ANY (
         SELECT split_part(actions.value #>> '{definition,canonical}', '/', 2)
         FROM (
         	SELECT jsonb_array_elements(subspecies.resource -> 'action') AS value
         ) AS actions)
	INNER JOIN observationdefinition test
		ON (test.resource @@ logic_include (research.resource,'observationResultRequirement')
	    OR test.id = ANY (array ( (SELECT jsonb_path_query(research.resource, '$.observationResultRequirement.id') #>> '{}')))
       	AND test.resource::TEXT ILIKE '%яд%'
       )) "rows";	  