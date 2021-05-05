SELECT *
FROM "location" l
JOIN schedulerule sr ON sr.resource @@ logic_revinclude(l.resource, l.id, 'location') 
	OR sr.resource @@ logic_revinclude(l.resource, l.id, 'actor.#')
WHERE l.id = 'a807da4d-31de-46fb-a2ea-4061cef0b729'

EXPLAIN ANALYZE 
SELECT *
FROM schedulerule_history s 
WHERE s.resource::text ~~ '%1d50b994-efed-4335-936e-2724518d04f7%'

SELECT count(*)
FROM schedulerule s 