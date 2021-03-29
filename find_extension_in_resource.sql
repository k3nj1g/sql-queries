WITH sup_info AS (SELECT jsonb_array_elements(resource -> 'supportingInfo') s 
FROM servicerequest s 
WHERE resource @@ 'category.#.coding.#.code = TMK'::jsquery),
splited AS (SELECT reverse(split_part(reverse(sup_info.s ->> 'display'), '.', 1)) part
FROM sup_info 
WHERE sup_info.s ->> 'resourceType' = 'DocumentReference' AND split_part(sup_info.s ->> 'display', '.', 3) = '')	
SELECT lower(splited.part), count(*)
FROM splited
WHERE char_length(splited.part) BETWEEN 2 AND 5 
	AND NOT splited.part ~ '([0-9]{2})'
GROUP BY lower(splited.part)