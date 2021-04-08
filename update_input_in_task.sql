SELECT jsonb_set(resource, '{input}',
				 (SELECT jsonb_agg(inputs."input")
				 FROM (
				 	SELECT jsonb_insert("input", '{value,Address,fias}', '"d14cb781-74c3-4096-9560-57a42b8a7f7c"') "input"
					FROM jsonb_array_elements(resource -> 'input') "input"
					WHERE "input" @@ 'type.coding.#.code = "requestingOrganizationAddress"'::jsquery
					UNION
					SELECT "input"
					FROM jsonb_array_elements(resource -> 'input') "input"
					WHERE NOT "input" @@ 'type.coding.#.code = "requestingOrganizationAddress"'::jsquery
					) inputs))
FROM task t 
WHERE id = '21faaadf-8b3a-4e99-8cda-6a81fcb2b06c';
