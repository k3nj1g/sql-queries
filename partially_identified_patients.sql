SELECT id, jsonb_path_query_array(resource, '$.identifier.system'), jsonb_path_query_array(resource, '$.extension'), resource 
FROM patient
WHERE resource @@ 'identifier.#:(not system = "urn:identity:snils:Patient" '
								'and not system = "urn:identity:enp:Patient" '
								'and not system = "urn:identity:newborn:Patient") '
				  'and not extension.#(url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery
	AND jsonb_array_length(resource->'identifier') > 1

--------------
UPDATE patient 
SET resource = jsonb_set(resource, '{extension}', (
		SELECT jsonb_agg(exts.ext)
	    FROM (
	 		SELECT jsonb_build_object('url', 'urn:extension:patient-type', 'valueCode', 'partially-identified') ext
	 		UNION
 			SELECT *
 			FROM jsonb_array_elements(resource -> 'extension') ext
 		) exts))
WHERE resource @@ 'identifier.#:(not system = "urn:identity:snils:Patient" '
								'and not system = "urn:identity:enp:Patient" '
								'and not system = "urn:identity:newborn:Patient") '
				  'and not extension.#(url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery
	AND jsonb_array_length(resource->'identifier') > 1
RETURNING resource

--- снова ---
UPDATE patient 
    SET resource = jsonb_set(resource, '{extension}', (
		SELECT jsonb_agg(exts.ext)
    	FROM (SELECT jsonb_build_object('url', 'urn:extension:patient-type', 'valueCode', 'partially-identified') ext
    	 	  UNION
     		  SELECT *
     		  FROM jsonb_array_elements(resource -> 'extension') ext) exts))
WHERE resource @@ 'identifier.#:(not system = "urn:identity:snils:Patient"'
				                'and not system = "urn:identity:enp:Patient"'
								'and not system = "urn:identity:newborn:Patient")'
                  'and not extension.#(url = "urn:extension:patient-type" and valueCode in ("newborn", "partially-identified"))'::jsquery
  AND jsonb_array_length(resource->'identifier') > 1
  
  ---
  SELECT *
  FROM patient
  WHERE resource @@ 'identifier.#:(not system = "urn:identity:snils:Patient"'
				                'and not system = "urn:identity:enp:Patient"'
								'and not system = "urn:identity:newborn:Patient")'
                  'and not extension.#(url = "urn:extension:patient-type" and valueCode in ("newborn", "partially-identified", "unidentified"))'::jsquery