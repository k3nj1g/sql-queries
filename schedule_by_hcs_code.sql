SELECT main_org.resource #>> '{alias,0}' org_name
       , jsonb_path_query_first(main_org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' org_oid
       , sch.resource #>> '{location, display}' cabinet
       , jsonb_path_query_first(loc.resource, '$.identifier ? (@.system == "urn:identity:oid:Location").value') #>> '{}' location_oid
       , jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole").display') #>> '{}'
       , concat(sch.resource #>> '{planningHorizon,start}', ' - ', sch.resource #>> '{planningHorizon,end}')
FROM schedulerule sch
JOIN healthcareservice hcs ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
    AND hcs.resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "999")'::jsquery
LEFT JOIN organization main_org ON main_org.id = sch.resource #>> '{mainOrganization,id}'      
LEFT JOIN "location" loc ON loc.id = sch.resource #>> '{location,id}'
WHERE immutable_ts(COALESCE ((sch.resource #>> '{planningHorizon,end}'), 'infinity')) >= LOCALTIMESTAMP 