SELECT split_part(split_part(u.resource ->> 'userName', '\', 2), '-', 1) org
	, u.resource -> 'userName' username	
	, string_agg(prr.resource #>> '{code,0,text}', ', ') prrole
FROM "user" u
JOIN "role" r ON r.resource #>> '{user,id}' = u.id
LEFT JOIN practitionerrole prr ON prr.id = r.resource #>> '{links,practitionerRole,id}' 
WHERE u.resource @@ 'psychiatry = true'::jsquery
GROUP BY  org, username
