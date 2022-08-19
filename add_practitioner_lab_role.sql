WITH users AS
(
    SELECT u.id, jsonb_agg(DISTINCT r.resource -> 'links') links
    FROM "user" u
    JOIN "role" r ON r.resource #>> '{user,id}' = u.id
    JOIN practitionerrole prr ON prr.id = r.resource #>> '{links,practitionerRole,id}'
        AND (prr.resource @@ 'code.#.text in ("Фельдшер-лаборант", "Врач клинической лабораторной диагностики", "Врач-лаборант", "Врач-бактериолог", "Лаборант")'::jsquery
            OR jsonb_path_query_first(prr.resource, '$.code ? (exists (@.coding ? (@.system == "urn:CodeSystem:frmr.position" && (@.code == "7" || @.code == "17")))).text') #>> '{}' LIKE '%лаборатори%')
    WHERE r.resource @@ 'name = "practitioner"'::jsquery
    GROUP BY u.id
)
INSERT INTO "role"
SELECT gen_random_uuid() as id
       , nextval('transaction_id_seq') as txid
       , current_timestamp as cts
       , current_timestamp as ts,
       'Role' as resource_type,
       'created' as status,
       jsonb_build_object('name', 'practitioner-lab',
                          'user', jsonb_build_object('id', u.id, 
                                                     'resourceType', 'User'),
                          'links', u.links -> 0) AS resource 
FROM users u
