SELECT *
FROM integrationqueue i 
WHERE i.resource @@ 'status = "pending"'::jsquery
	AND i.ts > '2021-03-16'
ORDER BY ts DESC;
--LIMIT 1

SELECT *
FROM pg_indexes
WHERE tablename = 'integrationqueue';

SELECT resource ->> 'status', i.ts, i.ts - i.cts
FROM integrationqueue i
WHERE i.ts > current_date;

SELECT id
	, resource ->> 'status' AS status
	, resource ->> 'locked' AS LOCKED
	, resource ->> 'start' AS start
	, (resource ->> 'nextStart')::timestamptz - (resource ->> 'start')::timestamptz AS next_start
	, resource 
FROM aidboxjobstatus a;

SELECT *
FROM aidboxjobstatus a;

SELECT *
FROM integrationqueue i
WHERE i.id = '70d382b7-cd2a-4f03-8ccd-150d30ad6096';

-- by Nibaev --
SELECT i.resource #>> '{clientId}' client
    , i.resource #>> '{payload,resourceType}' resourceType
	, i.resource ->> 'status' "status"
	, count(*)
FROM integrationqueue i
WHERE i.ts > current_date
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3 DESC;

-- by Nibaev --
WITH grouped as (
  SELECT i.resource #>> '{payload,resourceType}' resourceType
      , resource
  	, count(*)
  FROM integrationqueue i
  WHERE i.ts > current_date
    and resource ->> 'clientId' = 'lis-galen'
  GROUP BY 1,2
  HAVING count(*) > 1)
SELECT resource #>> '{payload,identifier,0,value}'
    , "count"
FROM grouped
ORDER BY "count" DESC;

SELECT count(*)
FROM integrationqueue i
WHERE i.ts BETWEEN '2021-01-11T00:00:00' AND '2021-01-11T23:59:59'
	AND i.resource @@ 'status = "pending"'::jsquery;

UPDATE integrationqueue
SET resource = jsonb_set(resource, '{status}', '"failed"')
WHERE id = '17b42bb5-0999-4615-9905-7ce077c4e211';
--WHERE ts BETWEEN '2021-01-11T00:00:00' AND '2021-01-11T23:59:59'
--	AND resource @@ 'status = "pending"'::jsquery
	
	
SELECT *
FROM integrationqueue i 
WHERE i.resource @@ 'payload.identifier.#(system = "urn:identity:newborn:Patient" and value = "2154510886000155_2021-02-16_1")'::jsquery
	AND i.ts > '2021-03-01'
ORDER BY ts DESC;

SELECT * FROM patient LIMIT 1;

UPDATE patient SET resource = jsonb_set(resource, '{active}', 'false', true) WHERE id = 'a64ad0fd-bcc9-48eb-9ab6-a6f9b0a14c9d';

SELECT * FROM patient WHERE id = 'a64ad0fd-bcc9-48eb-9ab6-a6f9b0a14c9d';
