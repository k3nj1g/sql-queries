SELECT *
-- app.resource, jsonb_set(app.resource, '{participant}',
--                (SELECT jsonb_agg(participants.actor)
--                 FROM (
--                   SELECT jsonb_set(participant, '{actor}', sch.resource #> '{actor,0}') actor
--                   FROM jsonb_array_elements(app.resource -> 'participant') participant
--                   WHERE participant #>> '{actor,resourceType}' = 'Location'
--                   UNION
--                   SELECT participant actor
--                   FROM jsonb_array_elements(app.resource -> 'participant') participant
--                   WHERE NOT participant #>> '{actor,resourceType}' = 'Location' OR NOT participant #> '{actor}' ?? 'resourceType') participants))
FROM schedulerule sch 
JOIN LATERAL (
    SELECT resource -> 'schedule' ->> 'id'
    FROM appointment
    WHERE 
        AND resource #>> '{start}' > '2021-12-23'
        AND resource @@ 'participant.#.actor.resourceType="Location"'::jsquery 
    GROUP BY 1, resource #>> '{start}'
    HAVING count(*) = 1
) app ON TRUE 
WHERE immutable_ts(COALESCE((sch.resource #>> '{planningHorizon,end}'), 'infinity')) >= '2021-12-23'
    AND sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
  
--- without doubles on same time    
WITH app_select AS (
    SELECT resource -> 'schedule' ->> 'id' sch_id, max(id) id, jsonb_agg(resource) #> '{0}' resource
    FROM appointment
    WHERE 
        resource #>> '{start}' > '2021-12-23'
        AND resource @@ 'participant.#.actor.resourceType="Location"'::jsquery 
    GROUP BY 1, resource #>> '{start}'
    HAVING count(*) = 1  
)
, to_update AS (
    SELECT apps.id
           , jsonb_set(apps.resource, '{participant}',
                      (SELECT jsonb_agg(participants.actor)
                       FROM (
                           SELECT jsonb_set(participant, '{actor}', sch.resource #> '{actor,0}') actor
                           FROM jsonb_array_elements(apps.resource -> 'participant') participant
                           WHERE participant #>> '{actor,resourceType}' = 'Location'
                           UNION
                           SELECT participant actor
                           FROM jsonb_array_elements(apps.resource -> 'participant') participant
                           WHERE NOT participant #>> '{actor,resourceType}' = 'Location' OR NOT participant #> '{actor}' ?? 'resourceType') participants)) resource
    FROM app_select apps
    JOIN schedulerule sch ON sch.id = apps.sch_id
    WHERE immutable_ts(COALESCE((sch.resource #>> '{planningHorizon,end}'), 'infinity')) >= '2021-12-23'
        AND sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery 
)
UPDATE appointment app
SET resource = tu.resource
FROM to_update tu
WHERE app.id = tu.id
RETURNING app.id

SELECT * FROM appointment a WHERE id = '71adf190-a717-4089-9a4a-277e406d229e'


WITH to_update AS (
    SELECT app.id
           , jsonb_set(app.resource, '{participant}',
                      (SELECT jsonb_agg(participants.actor)
                       FROM (
                           SELECT jsonb_set(participant, '{actor}', sch.resource #> '{actor,0}') actor
                           FROM jsonb_array_elements(app.resource -> 'participant') participant
                           WHERE participant #>> '{actor,resourceType}' = 'Location'
                           UNION
                           SELECT participant actor
                           FROM jsonb_array_elements(app.resource -> 'participant') participant
                           WHERE NOT participant #>> '{actor,resourceType}' = 'Location' OR NOT participant #> '{actor}' ?? 'resourceType') participants)) resource
    FROM schedulerule sch 
    JOIN appointment app ON app.resource -> 'schedule' ->> 'id' = sch.id
        AND app.resource #>> '{start}' > '2021-12-23'
        AND app.resource @@ 'participant.#.actor.resourceType="Location"'::jsquery
    WHERE immutable_ts(COALESCE((sch.resource #>> '{planningHorizon,end}'), 'infinity')) >= '2021-12-23'
        AND sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery)
UPDATE appointment app
SET resource = tu.resource
FROM to_update tu
WHERE app.id = tu.id
RETURNING app.id
