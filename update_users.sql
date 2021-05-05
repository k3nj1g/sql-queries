WITH users AS (
	SELECT DISTINCT ON (u.id) u.id u_id, o.resource o_resource, prr_main_org.resource prr_main_org_resource
	FROM "user" u
	JOIN "role" r ON r.resource #>> '{user,id}' = u.id
	LEFT JOIN organization o ON o.id = r.resource #>> '{links,organization,id}'
	LEFT JOIN practitionerrole prr ON prr.id = r.resource #>> '{links,practitionerRole,id}'
	LEFT JOIN organization prr_org ON prr_org.resource @@ logic_include(prr.resource, 'organization')
	LEFT JOIN organization prr_main_org ON prr_main_org.resource @@ logic_include(prr_org.resource, 'mainOrganization')
	WHERE u.resource @@ 'psychiatry = true'::jsquery)
, update_psychiatry AS (
	UPDATE "user" u
	SET resource = u.resource || '{"psychiatry": true, "narcology": false}'::jsonb
	FROM (SELECT *
	      FROM users
	      WHERE o_resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1542")'::jsquery
             OR prr_main_org_resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1542")'::jsquery) us 
	WHERE u.id = us.u_id
    RETURNING u.id)
, update_narcology AS (
	UPDATE "user" u
	SET resource = u.resource || '{"psychiatry": false, "narcology": true}'::jsonb
	FROM (SELECT *
	      FROM users
	      WHERE o_resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1544")'::jsquery
             OR prr_main_org_resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1544")'::jsquery) us 
	WHERE u.id = us.u_id
    RETURNING u.id)    
UPDATE "user" u
SET resource = u.resource || '{"psychiatry": false, "narcology": false}'::jsonb
FROM (SELECT u_id FROM users 
      EXCEPT 
      SELECT id FROM update_psychiatry
      EXCEPT 
      SELECT id FROM update_narcology) us 
WHERE u.id = us.u_id;