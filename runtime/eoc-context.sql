SELECT row_to_json(episodeofcare_subselect.*) AS episodeofcare 
FROM (SELECT id, resource_type, status, ts, txid, (resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS resource,
			(SELECT json_agg(row_to_json(encounter_subselect.*)) AS encounter 
			FROM (SELECT id, resource_type, status, ts, txid, (resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS resource, 
						(SELECT json_agg(row_to_json(documentreference_subselect.*)) AS documentreference 
						FROM (SELECT id, resource_type, status, ts, txid, (resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS resource 
							 FROM documentreference 
							 WHERE (documentreference.resource @@ logic_revinclude(encounter.resource, encounter.id, 'context.encounter.#') 
								AND (documentreference.resource->>'active' is null or documentreference.resource->>'active' = 'true')) 
							 ORDER BY CAST(documentreference.resource ->> 'date' AS timestamptz) DESC NULLS LAST LIMIT 6) documentreference_subselect
						) AS document 
				 FROM encounter 
				 WHERE encounter.resource @@ logic_revinclude(episodeofcare.resource, episodeofcare.id, 'episodeOfCare.#') 
				 ORDER BY CAST(encounter.resource #>> '{period,start}' AS timestamptz) DESC NULLS FIRST LIMIT 6) encounter_subselect
			) AS encounter, 
			(SELECT json_agg(row_to_json(medrecordzno_subselect.*)) AS medrecordzno 
			FROM (SELECT id, resource_type, status, ts, txid, (resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS resource 
				 FROM (SELECT logic_revinclude(enc.resource, enc.id, 'encounter.#') AS js_q 
				 	  FROM encounter enc 
					  WHERE enc.resource @@ logic_revinclude(episodeofcare.resource, episodeofcare.id, 'episodeOfCare.#')) enc 
					  INNER JOIN medrecordzno medrecordzno 
					             ON medrecordzno.resource @@ enc.js_q 
					  ORDER BY CAST(medrecordzno.resource ->> 'date' AS timestamptz) DESC NULLS LAST LIMIT 6) medrecordzno_subselect
			) AS med_record_zno, 
			(SELECT row_to_json(patient_subselect.*) AS patient 
			FROM (SELECT id, resource_type, status, ts, txid, (resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS resource, 
						 (SELECT row_to_json(sector_subselect.*) AS sector 
						 FROM (SELECT id, resource_type, status, ts, txid, (resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS resource 
							  FROM (SELECT sector.* 
							       FROM personbinding pb 
								   INNER JOIN sector sector 
								              ON identifier_match(sector.resource, pb.resource, 'urn:source:tfoms:Sector', 'sector') 
								   WHERE ref_match(patient.resource, pb.resource, 'urn:source:tfoms:Patient', 'subject') LIMIT 1
								   ) as sector 
							  LIMIT 1
							  ) as sector_subselect
						 ) as sector 
				 FROM patient 
				 WHERE (patient.resource->>'active' is null or patient.resource->>'active' = 'true')
				       AND (patient.resource @@ logic_include(episodeofcare.resource, 'patient') 
					   OR patient.id = episodeofcare.resource#>>'{patient,id}') 
				 LIMIT 1
				 ) patient_subselect
			) AS patient 
		FROM episodeofcare WHERE id = '17bf4be0-9e58-4ffa-8e12-1101620dfc14'
	) episodeofcare_subselect