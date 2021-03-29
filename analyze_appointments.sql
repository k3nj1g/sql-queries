--- общее количество записей по епгу и по мо ---
WITH web AS (
	SELECT count(*) web_app
	FROM appointment a 
	WHERE a.resource @@ 'serviceType.#.coding.#.code = "153" and from = web and not status = cancelled'::jsquery
	  AND a.resource ->> 'start' BETWEEN '2020-01-01' AND '2021-01-01'
),
mo AS (
	SELECT count(*) org_app
	FROM appointment a 
	WHERE a.resource @@ 'serviceType.#.coding.#.code = "153" and not from = web and not status = cancelled'::jsquery
	  AND a.resource ->> 'start' BETWEEN '2020-01-01' AND '2021-01-01'
)
SELECT *
FROM web, mo

--- count all appointments with service.code ---
SELECT org.resource #>> '{alias,0}' org_id, count(*)
FROM appointment a 
JOIN organization org ON org.id = a.resource #>> '{mainOrganization,id}'
WHERE a.resource @@ 'serviceType.#.coding.#.code = "153"'::jsquery
  AND a.resource ->> 'start' BETWEEN '2020-12-01' AND '2021-02-09'
GROUP BY org_id