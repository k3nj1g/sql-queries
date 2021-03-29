
UPDATE appointment 
SET resource = jsonb_set(resource, 
						'{participant}',
						(SELECT jsonb_agg(participants.actor)
						FROM (
								SELECT jsonb_set(participant, '{actor,id}', '"8ef8ef0c-6dff-4015-b7e6-1d4363a4c4c6"') actor
								FROM jsonb_array_elements(resource -> 'participant') participant
								WHERE participant #>> '{actor,resourceType}' = 'PractitionerRole'
								UNION
								SELECT participant actor
								FROM jsonb_array_elements(resource -> 'participant') participant
								WHERE NOT participant #>> '{actor,resourceType}' = 'PractitionerRole' OR NOT participant #> '{actor}' ? 'resourceType') participants))
WHERE id = '760c13b0-b5eb-4bb0-aae0-166e217b83d4'

--- final ---
UPDATE appointment 
SET resource = jsonb_set(resource, 
						'{participant}',
						(SELECT jsonb_agg(participants.actor)
						FROM (
								SELECT jsonb_set(participant, '{actor,id}', '"8ef8ef0c-6dff-4015-b7e6-1d4363a4c4c6"') actor
								FROM jsonb_array_elements(resource -> 'participant') participant
								WHERE participant #>> '{actor,resourceType}' = 'PractitionerRole'
								UNION
								SELECT participant actor
								FROM jsonb_array_elements(resource -> 'participant') participant
								WHERE NOT participant #>> '{actor,resourceType}' = 'PractitionerRole' OR NOT participant #> '{actor}' ?? 'resourceType') participants))
WHERE resource @@ 'schedule.id = "6adf6c81-e845-4ef6-b9c3-82a570f0302b"'::jsquery AND (resource ->> 'start')::timestamp > '2020-09-01'::timestamp
RETURNING id, resource 
-------

SELECT * FROM appointment a WHERE id = '760c13b0-b5eb-4bb0-aae0-166e217b83d4'