--1 скрипт, который показывает список МО, которые не сформировали расписание на услугу профилактический медицинский осмотр на врача с доступном епгу с признаком имеет прикреплённое население, расписание должно быть дейтствующее
WITH orgs AS 
	(SELECT org.id, jsonb_agg(jsonb_path_query_first(hcs.resource, '$.type[*].coding[*] ? (@.system == "urn:CodeSystem:service").code'))
	FROM schedulerule sch
	JOIN healthcareservice hcs ON
		hcs.id = sch.resource #>> '{healthcareService,0,id}'
	JOIN organization org ON org.id = sch.resource #>> '{mainOrganization,id}'
	WHERE (CAST(sch.resource #>> '{planningHorizon, end}' AS timestamp) > current_timestamp OR NOT sch.resource -> 'planningHorizon' ?? 'end') 
		AND sch.resource @@ 'actor.#(resourceType = "PractitionerRole") and availableTime.#.channel.# = "web"'::jsquery
	GROUP BY org.id
	HAVING NOT jsonb_agg(jsonb_path_query_first(hcs.resource, '$.type[*].coding[*] ? (@.system == "urn:CodeSystem:service").code')) @> '"3000"')
SELECT o.id, o.resource #>> '{alias,0}' org_name
FROM orgs
JOIN organization o ON o.id = orgs.id
JOIN organizationinfo oi ON oi.id = orgs.id
WHERE oi.resource @@ 'flags.#(system = "urn:CodeSystem:mo-flags" and code = "3")'::jsquery
ORDER BY org_name

--2 скрипт, который показывает список МО, которые сформировали расписание на услугу профилактический медицинский осмотр на врача, но не указали доступ епгу, с признаком имеет прикреплённое население, расписание должно быть дейтствующее
WITH orgs AS 
	(SELECT org.id
	FROM schedulerule sch
	JOIN healthcareservice hcs ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
		AND hcs.resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "3000")'::jsquery
	JOIN organization org ON org.id = sch.resource #>> '{mainOrganization,id}'
	WHERE (CAST(sch.resource #>> '{planningHorizon, end}' AS timestamp) > current_timestamp OR NOT sch.resource -> 'planningHorizon' ?? 'end') 
		AND sch.resource @@ 'actor.#(resourceType = "PractitionerRole")'::jsquery
	GROUP BY org.id
	HAVING NOT jsonb_agg(jsonb_path_query_first(sch.resource, '$.availableTime[*] ? (exists(@.channel[*] ? (@ == "web"))).channel')) @> '[["web"]]')
SELECT o.id, o.resource #>> '{alias,0}' org_name
FROM orgs
JOIN organization o ON o.id = orgs.id
JOIN organizationinfo oi ON oi.id = orgs.id
WHERE oi.resource @@ 'flags.#(system = "urn:CodeSystem:mo-flags" and code = "3")'::jsquery
ORDER BY org_name

--3 скрипт, который показывает список МО, которые сформировали расписание на услугу профилактический медицинский осмотр на кабинет и указали доступ епгу, с признаком имеет прикреплённое население, расписание должно быть дейтствующее
WITH orgs AS 
	(SELECT org.id, string_agg(sch.id::TEXT, ';')
	FROM schedulerule sch
	JOIN healthcareservice hcs ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
		AND hcs.resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "3000")'::jsquery
	JOIN organization org ON org.id = sch.resource #>> '{mainOrganization,id}'
	WHERE (CAST(sch.resource #>> '{planningHorizon, end}' AS timestamp) > current_timestamp OR NOT sch.resource -> 'planningHorizon' ?? 'end') 
		AND sch.resource @@ 'actor.#(resourceType = "Location") and availableTime.#.channel.# = "web"'::jsquery
	GROUP BY org.id)
SELECT o.id, o.resource #>> '{alias,0}' org_name, orgs.*
FROM orgs
JOIN organization o ON o.id = orgs.id
JOIN organizationinfo oi ON oi.id = orgs.id
WHERE oi.resource @@ 'flags.#(system = "urn:CodeSystem:mo-flags" and code = "3")'::jsquery
ORDER BY org_name
