SELECT r.resource
FROM "user" u
JOIN "role" r ON r.resource #>> '{user,id}' = u.id 
WHERE r.resource @@ 'name = "practitioner-lab"'::jsquery

SELECT *
FROM "user" u 
JOIN "role" r ON r.resource #>> '{user,id}' = u.id
--WHERE u.id IN ('0b3fbd10-3b4f-4733-a432-36fa5cc5b22d', '0ac4a0b6-dad9-4315-b98b-7c22b90a6eeb')    
    
WITH all_labs AS
(
  SELECT DISTINCT u.id
  FROM "user" u
    JOIN "role" r ON r.resource #>> '{user,id}' = u.id
  WHERE r.resource @@ 'name = "practitioner-lab"'::jsquery
),
practitioners AS 
(
  SELECT u.id, jsonb_agg(r.resource -> 'links') links
  FROM all_labs u
  JOIN "role" r ON r.resource #>> '{user,id}' = u.id
  WHERE r.resource @@ 'name = "practitioner"'::jsquery
  GROUP BY u.id
),
no_practitioners AS 
(
  SELECT u.id
  FROM all_labs u
  LEFT JOIN "role" r ON r.resource #>> '{user,id}' = u.id
    AND r.resource @@ 'name = "practitioner"'::jsquery
  WHERE r IS NULL
),
for_update AS 
(
  SELECT r.id, u.links -> 0 link
  FROM practitioners u
    JOIN "role" r ON r.resource #>> '{user,id}' = u.id
  WHERE r.resource @@ 'name = "practitioner-lab"'::jsquery
)
UPDATE "role" r
SET resource = jsonb_set(r.resource, '{links}', upd.link)
FROM for_update upd
WHERE r.id = upd.id

