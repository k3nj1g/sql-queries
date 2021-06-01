SELECT s.resource #>> '{mainOrganization,display}', count(*)
FROM schedulerule s 
JOIN practitionerrole prr ON prr.id = jsonb_path_query_first(s.resource, '$.actor ? (@.resourceType == "PractitionerRole")') #>> '{id}'
    AND prr.resource #>> '{active}' = 'false'
WHERE immutable_ts(COALESCE ((s.resource #>> '{planningHorizon,end}'), 'infinity')) >= LOCALTIMESTAMP    
GROUP BY s.resource #>> '{mainOrganization,display}'