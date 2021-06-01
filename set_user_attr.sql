WITH users AS (
    SELECT DISTINCT u.id
    FROM organization main_org
    JOIN organization org ON org.resource @@ logic_revinclude(main_org.resource, main_org.id, 'mainOrganization')
    JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(org.resource, org.id, 'organization')
    JOIN "role" r ON r.resource #>> '{links,practitionerRole,id}' = prr.id
    JOIN "user" u ON u.id = r.resource #>> '{user,id}' AND u.resource #>> '{userName}' LIKE 'med\\rptd%'
    WHERE main_org.resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1541")'::jsquery)
UPDATE "user" u
SET resource = u.resource || '{"tuberculosis": true}'::jsonb
FROM users us
WHERE u.id = us.id
RETURNING u.id;
