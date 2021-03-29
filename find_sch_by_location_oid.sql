SELECT sch.*
FROM location l
JOIN schedulerule sch ON sch.resource @@ logic_include(l.resource, 'location') OR sch.resource #>> '{location,id}' = l.id
WHERE l.resource @@ 'identifier.#(system = "urn:identity:oid:Location" and value = "1.2.643.5.1.13.13.12.2.21.1525.0.145998")'::jsquery
	AND sch.resource @@ 'availableTime.#.channel.# = "web"'::jsquery
	AND immutable_tsrange(sch.resource #>> '{planningHorizon, start}', coalesce(sch.resource#>> '{planningHorizon,end}', 'infinity')) @> localtimestamp 

