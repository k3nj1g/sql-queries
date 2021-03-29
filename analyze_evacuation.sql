-- evacutaion requester ---
SELECT sr.resource #>> '{requester,display}' org_name, count(*) sended
FROM servicerequest sr
WHERE sr.resource @@ 'category.#.coding.#(code = "interhospital-evacuation" and system = "urn:CodeSystem:servicerequest-category") and not status in (draft, revoked)'::jsquery 
GROUP BY sr.resource #>> '{requester,display}'

-- evacutaion performer ---
WITH performer AS (
	SELECT jsonb_path_query_first(sr.resource, '$.performer ? (exists (@.code.coding ? (@.code == "main-organization"))).identifier.value') org_idf
		, count(*) received
		, count(*) FILTER (WHERE t.resource @@ 'status = requested'::jsquery) not_processed
		, count(*) FILTER (WHERE t.resource @@ 'status in (accepted, ready, received, in-progress, completed)'::jsquery) completed
		, count(*) FILTER (WHERE t.resource @@ 'status = rejected'::jsquery) rejected
	FROM servicerequest sr
	JOIN task t ON t.resource @@ logic_revinclude(sr.resource, sr.id, 'focus')
	WHERE sr.resource @@ 'category.#.coding.#(code = "interhospital-evacuation" and system = "urn:CodeSystem:servicerequest-category") and not status in (draft, revoked)'::jsquery 
	GROUP BY org_idf)
SELECT org.resource #>> '{alias,0}' org_name, performer.received, performer.not_processed, performer.completed, performer.rejected
FROM performer
JOIN organization org ON org.resource @@ concat('identifier.#.value = ', performer.org_idf, '')::jsquery

