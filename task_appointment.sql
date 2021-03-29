SELECT prr.id, concat(pr.resource #>> '{name,0,family}', ' ' , pr.resource #>> '{name,0,given,0}', ' ' , pr.resource #>> '{name,0,given,1}'), prr.resource #>> '{code,0,text}'
FROM practitionerrole prr 
JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
WHERE NOT prr.resource ?? 'derived'

SELECT id
FROM appointment a 
WHERE
	a.resource ->> 'start' > '2020-09-30T00:00:00'
	AND a.resource #>> '{mainOrganization,id}' IS NULL 
--	AND NOT a.resource -> 'mainOrganization' ?? 'id'
--ORDER BY txid

SELECT a.resource ->> 'author'
FROM appointment a 
WHERE
	a.resource ->> 'start' > '2020-09-30T00:00:00'
	AND NOT a.resource ?? 'authorOrganization'
	AND a.resource ->> 'from' = 'kc'
--GROUP BY a.resource ->> 'from'
--	AND NOT a.resource -> 'mainOrganization' ?? 'id'
--ORDER BY txid

SELECT count(*)
FROM appointment a 
WHERE
	a.resource ->> 'start' > '2020-09-30T00:00:00'
--	AND NOT a.resource ?? 'authorOrganization'
	AND a.resource ->> 'from' = 'kc'
	
--EXPLAIN ANALYZE
SELECT *
FROM practitionerrole AS pr
JOIN organization AS o ON o.resource @@ logic_include(pr.resource, 'organization')
JOIN organization AS mo ON mo.resource @@ logic_include(o.resource, 'mainOrganization')	
WHERE pr.id = '1cb4dfc0-e86b-426e-b45d-5668d0095c28'

--EXPLAIN ANALYZE 
SELECT *
FROM organization mo
WHERE mo.resource @> (
	SELECT jsonb_build_object('identifier', jsonb_build_array(o.resource#>'{mainOrganization,identifier}'))
	FROM organization o 
	WHERE o.resource @> (
		SELECT jsonb_build_object('identifier', jsonb_build_array(p.resource#>'{organization,identifier}'))
		FROM practitionerrole p 
		WHERE id = '1cb4dfc0-e86b-426e-b45d-5668d0095c28'))
	