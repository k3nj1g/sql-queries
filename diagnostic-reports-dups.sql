SELECT *
FROM servicerequest s 
--JOIN diagnosticreport d ON d.resource @@ logic_revinclude(s.resource, s.id, 'basedOn.#')
WHERE s.resource @@ 'code.coding.#(system = "urn:CodeSystem:Nomenclature-medical-services" and code = "A26.08.008.001")'::jsquery

SELECT *
FROM diagnosticreport d 
WHERE d.resource ?? 'basedOn'
LIMIT 1

SELECT *
FROM pg_indexes
WHERE tablename = 'diagnosticreport'

SELECT count(*),  jsonb_agg(s.id) AS s_id, jsonb_agg(d.id) AS d_id, jsonb_agg(o.id) AS o_id
FROM servicerequest s
JOIN diagnosticreport d ON d.resource @@ logic_revinclude(s.resource, s.id, 'basedOn.#')
LEFT JOIN observation o ON o.resource @@ logic_include(d.resource, 'result[*]')
WHERE s.resource @@ 'code.coding.#(system = "urn:CodeSystem:Nomenclature-medical-services" and code = "A26.08.008.001")'::jsquery
GROUP BY s.id
HAVING count(*) > 1


SELECT count(*), jsonb_agg(d.resource -> 'result'), jsonb_agg(d.id)
FROM diagnosticreport d 
WHERE d.resource @@ 'code.coding.#(system = "urn:CodeSystem:Nomenclature-medical-services" and code = "A26.08.008.001")'::jsquery
GROUP BY d.resource -> 'basedOn'
HAVING count(*) > 1

SELECT jsonb_agg(o.*) 
FROM observation o 
WHERE o.resource @@ 'code.coding.#(system = "urn:CodeSystem:Laboratory-Research-and-Test" and code = "1140094")'::jsquery
GROUP BY o.resource -> 'identifier'
HAVING count(*) > 1

SELECT *
FROM servicerequest s
--WHERE s.resource @@ 'performer.#(identifier.value = "1.2.643.5.1.13.13.12.2.21.1519")'::jsquery
WHERE s.resource @@ 'code.coding.#(system = "urn:CodeSystem:rmis:ServiceRequest" and code = "covid_rkvd")'::jsquery
	AND NOT s.resource @@ 'performer.#.type = "Organization"'::jsquery
