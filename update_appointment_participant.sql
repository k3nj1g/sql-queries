SELECT jsonb_set(resource, '{participant}',
				 (SELECT jsonb_agg(elements.el)
				 FROM (
				 	SELECT jsonb_set(array_object, '{actor}', jsonb_build_object('id', '2469fecd-7332-4bed-b4a1-d1c91547a93e', 'resourceType', 'Patient', 'display', 'Фёдорова Ольга Валентиновна')) el
					FROM jsonb_array_elements(resource -> 'participant') array_object
					WHERE array_object @@ 'actor.display = "Нет данных"'::jsquery
					UNION
					SELECT array_object el
					FROM jsonb_array_elements(resource -> 'participant') array_object
					WHERE NOT array_object @@ 'actor.display = "Нет данных"'::jsquery
					) elements))
FROM appointment a 
WHERE id = '2bdc89f4-735e-4164-af3b-3fce52d7008f';
