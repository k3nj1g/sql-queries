SELECT count(*) AS "all"
       , count(*) FILTER (WHERE s.resource #>> '{performerInfo,requestStatus}' = 'completed') AS "completed"
       , count(*) - count(*) FILTER (WHERE s.resource #>> '{performerInfo,requestStatus}' = 'completed') AS "diff"
FROM servicerequest s 
WHERE s.resource @@ 'managingOrganization.identifier.value="1.2.643.5.1.13.13.12.2.21.10817"  and identifier.#.system="urn:source:icl:ServiceRequest"'::jsquery
    AND s.cts BETWEEN current_date - interval '45 day' AND current_date