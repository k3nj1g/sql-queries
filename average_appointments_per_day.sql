SELECT AVG(avg_per_day.cnt)
FROM (SELECT count(*) cnt
FROM appointment app
WHERE app.resource @@ 'participant.#.actor.resourceType = "PractitionerRole"'::jsquery
  AND app.resource ->> 'start' > '2020-01-01'
GROUP BY date_trunc('day', (app.resource ->> 'start')::timestamp)) avg_per_day

SELECT count(app.id)
FROM appointment app
WHERE app.resource @@ 'participant.#.actor.resourceType = "PractitionerRole"'::jsquery
	AND app.resource ->> 'start' > '2020-01-01'