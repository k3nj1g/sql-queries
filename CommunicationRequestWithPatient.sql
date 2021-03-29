SELECT  
	cr.resource || (concat('{"id":"', cr.id, '"}'))::jsonb AS communicationrequest
	, p.resource || (concat('{"id":"', p.id, '"}'))::jsonb || concat('{"sector": ', COALESCE(s.resource, 'null'), '}')::jsonb
FROM CommunicationRequest cr
LEFT JOIN patient p ON p.resource @@ logic_include(cr.resource, 'subject')
LEFT JOIN personbinding pb ON ref_match(p.resource, pb.resource, 'urn:source:tfoms:Patient', 'subject')
LEFT JOIN sector s ON identifier_match(s.resource, pb.resource, 'urn:source:tfoms:Sector', 'sector')
WHERE
	(cr.ts >= '2020-12-08T20:55'
	OR '2020-12-08T20:55' IS NULL)
	AND (cr.ts <= null::timestamp
	OR null::timestamp IS NULL)
	AND (cr.ID = null::TEXT
	OR null::TEXT IS NULL)
	AND cr.resource @@ concat('category.#.coding.#.code IN (lp900)'::TEXT, 'AND NOT category.#.coding.#.code IN (RegCOVID19_Hosp)'::TEXT)::jsquery

	
SELECT * FROM pg_catalog.pg_indexes 
WHERE tablename = 'communicationrequest'

SELECT count(*)
FROM communicationrequest

create index if not exists communicationrequest_ts_btree on communicationrequest (ts);
vacuum analyze communicationrequest;


CREATE INDEX IF NOT EXISTS communicationrequest_ts_btree ON
public.communicationrequest
	USING btree (ts);