WITH updated AS (
  SELECT (SELECT jsonb_agg(identifiers.idf)
		  FROM (SELECT identifier idf
				FROM jsonb_array_elements(resource -> 'identifier') identifier
				WHERE NOT identifier #>> '{system}' = 'urn:identity:tfoms:Patient') identifiers)
  FROM patient
  WHERE id='16598eb7-77b5-411b-91ea-d231469fdf58'
)
SELECT * 
FROM updated
