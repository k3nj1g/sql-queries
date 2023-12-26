
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
WHERE resource @@ 'schedule.id = "393b07af-692c-44a3-ac24-d94ef0af0f10" and participant.#.actor.resourceType = PractitionerRole'::jsquery
	AND resource ->> 'start' > '2021-02-09'
RETURNING id, resource 
-------

--- update from practitionerrole to location ---
UPDATE appointment 
SET resource = jsonb_set(resource, 
						'{participant}',
						(SELECT jsonb_agg(participants.actor)
						FROM (
								SELECT jsonb_set(participant, '{actor}', jsonb_build_object('id', 'e46ebe68-6fba-4345-828e-248765e2d5d5', 'display', '������� ���������� �� COVID-19 �106', 'resourceType', 'Location')) actor
								FROM jsonb_array_elements(resource -> 'participant') participant
								WHERE participant #>> '{actor,resourceType}' = 'PractitionerRole'
								UNION
								SELECT participant actor
								FROM jsonb_array_elements(resource -> 'participant') participant
								WHERE NOT participant #>> '{actor,resourceType}' = 'PractitionerRole' OR NOT participant #> '{actor}' ?? 'resourceType') participants))
WHERE resource @@ 'schedule.id = "393b07af-692c-44a3-ac24-d94ef0af0f10" and participant.#.actor.resourceType = PractitionerRole'::jsquery
	AND resource ->> 'start' > '2021-02-09'
RETURNING id, resource 

--- location to practitionerrole
UPDATE appointment 
SET resource = jsonb_set(resource, 
                        '{participant}',
                        (SELECT jsonb_agg(participants.actor)
                         FROM (
                           SELECT jsonb_set(participant, '{actor}'
                                           , jsonb_build_object('id', 'c00c4606-7886-4fb9-afec-d62d9c97e884',
                                                                'display', '��������� ����� �������� (���� �������������� �����������) ���������������� (����������)',
                                                                'resourceType', 'PractitionerRole')) actor
                           FROM jsonb_array_elements(resource -> 'participant') participant
                           WHERE participant #>> '{actor,resourceType}' = 'Location'
                           UNION
                           SELECT participant actor
                           FROM jsonb_array_elements(resource -> 'participant') participant
                           WHERE NOT participant #>> '{actor,resourceType}' = 'Location' OR NOT participant #> '{actor}' ?? 'resourceType') participants))
WHERE resource @@ 'schedule.id = "67b72d21-3785-4dc3-973f-739b62f5c865" and participant.#.actor.resourceType = Location'::jsquery
    AND resource ->> 'start' > '2021-12-22'
RETURNING id;


WITH to_update AS (
  SELECT app.id app_id
    , jsonb_set_lax(
	    app.resource
	    , '{participant}'
	    , (SELECT jsonb_agg(participants.actor)
           FROM (
    	     SELECT actor
    	     FROM jsonb_array_elements(app.resource -> 'participant') participant(actor)
    	     WHERE actor #>> '{actor,resourceType}' IS NOT NULL
    	     UNION
    	     SELECT jsonb_set_lax(actor, '{actor}', jsonb_build_object('display', jsonb_path_query_first(p.resource, '$.address ? (@.use=="temp").text')))
    	     FROM jsonb_array_elements(app.resource -> 'participant') participant(actor)
    	     WHERE actor #>> '{actor,resourceType}' IS NULL) participants)) resource
  FROM appointment app
  JOIN patient p ON p.id = jsonb_path_query_first(app.resource, '$.participant.actor ? (@.resourceType=="Patient").id') #>> '{}'
  WHERE app.resource @@ 'serviceType.#.coding.#.code="153" and not participant.#:.actor.display=* and not from=web'::jsquery
  AND app.ts > '2023-10-18T22:00:00.000+03:00'::timestamp
)
UPDATE appointment app
SET resource = tu.resource
FROM to_update tu
WHERE app.id = tu.app_id
RETURNING app.*;