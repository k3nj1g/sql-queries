--- all lmi by org
SELECT o.resource #>> '{alias,0}' org_name, count(*)
FROM servicerequest s
JOIN organization o ON o.resource @@ logic_include(s.resource, 'managingOrganization') OR (o.id = ANY(ARRAY((SELECT (jsonb_path_query(s.resource, '$.managingOrganization.id') #>> '{}' )))))
WHERE s.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI") and not performerInfo.requestStatus = "completed"'::jsquery
    AND s.resource ->> 'authoredOn' BETWEEN '2021-10-01' AND '2021-11-01'
GROUP BY org_name
ORDER BY org_name

--- completed lmi 
SELECT o.resource #>> '{alias,0}' org_name
       , count(*) FILTER (WHERE s.resource @@ 'status = "completed"'::jsquery) status_completed
       , count(*) FILTER (WHERE s.resource @@ 'performerInfo.requestStatus = "completed"'::jsquery) request_status_completed
       , count(*)
FROM diagnosticreport d
JOIN servicerequest s ON (s.resource @@ logic_include(d.resource, 'basedOn') OR (s.id = ANY(ARRAY((SELECT (jsonb_path_query(d.resource, '$.basedOn.id') #>> '{}'))))))
    AND s.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI") 
                       and performerInfo.requestStatus = "completed"
                       or status = "completed"'::jsquery
JOIN organization o ON o.resource @@ logic_include(s.resource, 'performer') OR (o.id = ANY(ARRAY((SELECT (jsonb_path_query(s.resource, '$.performer.id') #>> '{}' )))))                       
WHERE d.resource #>> '{effective,dateTime}' BETWEEN '2021-10-01' AND '2021-11-01'
GROUP BY org_name
ORDER BY org_name;

SELECT s.resource #>> '{managingOrganization,display}' org_name
       , count(*) FILTER (WHERE s.resource @@ 'status = "completed"'::jsquery) status_completed
       , count(*) FILTER (WHERE s.resource @@ 'performerInfo.requestStatus = "completed"'::jsquery) request_status_completed
       , count(*)
FROM diagnosticreport d
JOIN servicerequest s ON (s.resource @@ logic_include(d.resource, 'basedOn') OR (s.id = ANY(ARRAY((SELECT (jsonb_path_query(d.resource, '$.basedOn.id') #>> '{}'))))))
    AND (s.resource -> 'performerInfo' @@ 'requestStatus = "completed"'::jsquery OR s.resource ->> 'status' = 'completed')                       
WHERE d.resource #>> '{effective,dateTime}' BETWEEN '2021-10-01' AND '2021-11-01'
    AND d.resource -> 'category' @@ '#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")'::jsquery
GROUP BY org_name
ORDER BY org_name;