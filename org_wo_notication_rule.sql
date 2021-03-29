SELECT DISTINCT o.id
  , o.resource #>> '{alias,0}' short_name
  , jsonb_extract_path_text(jsonb_path_query_first(o.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization")'), 'value') "oid"
FROM organization o 
JOIN organizationinfo oi ON oi.id = o.id
	AND oi.resource @@ 'not notificationDate = *'::jsquery
JOIN schedulerule sch ON sch.resource @@ logic_revinclude(o.resource, o.id, 'mainOrganization') 
	AND tstzrange((sch.resource #>> '{planningHorizon,start}')::timestamp, (sch.resource #>> '{planningHorizon,end}')::timestamp) @> current_timestamp