SELECT sr.resource #>> '{managingOrganization,display}' org_name
     ,(count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp < '24 hour'::INTERVAL)) less_24_hours
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp BETWEEN '24 hour'::INTERVAL AND '48 hour'::INTERVAL)) between_24_48_hours
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp BETWEEN '3 day'::INTERVAL AND '5 day'::INTERVAL)) between_3_5_days
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp BETWEEN '6 day'::INTERVAL AND '7 day'::INTERVAL)) between_6_7_days
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp > '7 day'::INTERVAL)) more_7_days
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime') IS NULL)) no_received_time
FROM servicerequest sr 
JOIN specimen s ON s.resource @@ logic_include(sr.resource, 'specimen')
WHERE sr.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")'::jsquery
    AND sr.resource ->> 'authoredOn' BETWEEN '2021-11-22' AND '2021-11-29'
GROUP BY 1   
ORDER BY 1
    
--LIMIT 10

SELECT resource
FROM servicerequest sr 
--JOIN specimen s ON s.resource @@ logic_include(sr.resource, 'specimen')
WHERE sr.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")'::jsquery
LIMIT 10

SELECT resource ->> 'name', resource
FROM organization o 
WHERE o.resource::TEXT ILIKE '%алат%'
    AND resource -> 'partOf' IS NULL

SELECT *
FROM pg_indexes
WHERE tablename = 'servicerequest'


SELECT (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp < '24 hour'::interval)) less_24_hours
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp BETWEEN '24 hour'::INTERVAL AND '48 hour'::INTERVAL)) between_24_48_hours
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp BETWEEN '3 day'::INTERVAL AND '5 day'::INTERVAL)) between_3_5_days
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp BETWEEN '6 day'::INTERVAL AND '7 day'::INTERVAL)) between_6_7_days
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime')::timestamp - (sr.resource ->> 'authoredOn')::timestamp > '7 day'::INTERVAL)) more_7_days
     , (count(*) FILTER (WHERE (s.resource ->> 'receivedTime') IS NULL)) no_received_time
FROM organization o 
JOIN servicerequest sr ON sr.resource @@ logic_revinclude(o.resource, o.id, 'managingOrganization', ' and category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")') 
    AND sr.resource ->> 'authoredOn' BETWEEN '2021-11-22' AND '2021-11-29' 
JOIN specimen s ON s.resource @@ logic_include(sr.resource, 'specimen')
--WHERE o.id = '0be4ac6b-2556-4da4-80d5-0d7c4e7926ce';