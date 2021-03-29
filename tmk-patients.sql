SELECT DISTINCT (jsonb_path_query_first(p.resource,'$.identifier[*] ? (@.system == "urn:identity:snils:Patient").value')) #>> '{}'
FROM servicerequest sr
JOIN documentreference dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'context.related.#', ' and category.#.coding.#.code = "TMK-medical-report" and remdStatus = "synced"') 
JOIN patient p ON p.id = sr.resource #>> '{subject,id}'
WHERE sr.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "TMK")'  
				  'and paymentType.code = "1"'::jsquery
	AND sr.resource ->> 'authoredOn' BETWEEN '2020-01-01' AND '2021-01-01'
				  
				  
SELECT *
FROM documentreference d 
WHERE d.resource @@ 'category.#.coding.#.code = "TMK-medical-report"'::jsquery