SELECT sch.resource #>> '{mainOrganization,display}' org, sch.resource #>> '{sector,display}' sector, count(*)
FROM schedulerule sch
JOIN practitionerrole prr ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole")') #>> '{id}'
WHERE immutable_ts(COALESCE ((sch.resource #>> '{planningHorizon,end}'), 'infinity')) >= LOCALTIMESTAMP
    AND sch.resource #>> '{sector,display}' IS NOT NULL 
GROUP BY org, sector
ORDER BY org, sector;

SELECT s.resource #>> '{organization,display}' org, s.resource #>> '{name}' sector, count(sch.id)
FROM sector s
LEFT JOIN schedulerule sch ON s.id = sch.resource #>> '{sector, id}'
    AND immutable_ts(COALESCE ((sch.resource #>> '{planningHorizon,end}'), 'infinity')) >= LOCALTIMESTAMP
WHERE immutable_ts(COALESCE ((s.resource #>> '{period,end}'), 'infinity')) >= LOCALTIMESTAMP
GROUP BY org, sector
ORDER BY org, sector;
