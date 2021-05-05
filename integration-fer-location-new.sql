--- get sector ---
SELECT
	sec.id AS sector_id
	, org.id AS org_id
	, org.resource @@ 'identifier.#(value in ("1.2.643.5.1.13.13.12.2.21.1558","1.2.643.5.1.13.13.12.2.21.1534","1.2.643.5.1.13.13.12.2.21.1550","1.2.643.5.1.13.13.12.2.21.1530","1.2.643.5.1.13.13.12.2.21.1531","1.2.643.5.1.13.13.12.2.21.1525","1.2.643.5.1.13.13.12.2.21.1529","1.2.643.5.1.13.13.12.2.21.10843") and system="urn:identity:oid:Organization")'::jsquery AS "covid19?"
FROM
	patient pat
INNER JOIN personbinding pb ON
	ref_match(
		pat.resource
		, pb.resource
		, 'urn:source:tfoms:Patient'
		, 'subject'
	)
INNER JOIN sector sec ON
	identifier_match(
		sec.resource
		, pb.resource
		, 'urn:source:tfoms:Sector'
		, 'sector'
	)
INNER JOIN organization org ON
	org.resource @@ CAST(logic_include(sec.resource, 'organization') AS jsquery)
WHERE
	pat.id = 'cae26d4f-c4f1-4791-b870-24d7fa2de4ee'
LIMIT 1;

SELECT *
FROM patient 
WHERE id = 'cae26d4f-c4f1-4791-b870-24d7fa2de4ee';

--- bind-mo-location-sector ---
 SELECT
	DISTINCT location.id
	, location.txid
	, location.ts
	, location.resource_type
	, location.cts
	, location.resource || jsonb_build_object('name', (CASE WHEN (organizationinfo.resource->>'shortName' IS NOT NULL) THEN concat(location.resource->>'name', ' ', organizationinfo.resource->>'shortName') ELSE location.resource->>'name' END)) resource
FROM
	schedulerule sch
INNER JOIN LOCATION LOCATION ON
	(
		location.id = sch.resource #>> '{location,id}'
	)
LEFT JOIN organizationinfo organizationinfo ON
	organizationinfo.id = location.resource #>> '{mainOrganization,id}'
WHERE
	(
		'2021-01-25'::timestamp <= CAST(COALESCE(sch.resource #>> '{planningHorizon,end}', 'infinity') AS timestamp)
		AND sch.resource @@ CAST('availableTime.#.channel.#($ = web) and mainOrganization.id = "81d41979-06de-4f10-a901-db8029b2a671" and sector.id = "186c774e-925a-4d80-97f4-8e81e3e744dc"' AS jsquery)
	)
	
--- covid-19-location ---
--EXPLAIN ANALYSE 
SELECT DISTINCT loc.id
	, loc.txid
	, loc.ts
	, loc.resource_type
	, loc.cts
	, loc.resource || jsonb_build_object('name',  
										(CASE WHEN (org_i.resource ->> 'shortName' IS NOT NULL) THEN concat(loc.resource ->> 'name', ' ', org_i.resource ->> 'shortName') ELSE loc.resource ->> 'name' END)) resource
FROM healthcareservice hcs
JOIN schedulerule sch ON sch.resource #>> '{healthcareService,0,id}' = hcs.id
	AND immutable_ts(coalesce(sch.resource #>> '{planningHorizon,end}', 'infinity')) >= '2021-01-19'
	AND sch.resource -> 'availableTime' @@ '$.#.channel.#($="web")'::jsquery 	
JOIN organization org ON org.id = sch.resource #>> '{mainOrganization,id}'
	AND org.resource @@ 'identifier.#(value in ("1.2.643.5.1.13.13.12.2.21.1558","1.2.643.5.1.13.13.12.2.21.1534","1.2.643.5.1.13.13.12.2.21.1550","1.2.643.5.1.13.13.12.2.21.1530","1.2.643.5.1.13.13.12.2.21.1531","1.2.643.5.1.13.13.12.2.21.1525","1.2.643.5.1.13.13.12.2.21.1529","1.2.643.5.1.13.13.12.2.21.10843") and system="urn:identity:oid:Organization")'::jsquery
JOIN "location" loc ON loc.id = sch.resource #>> '{location,id}'
LEFT JOIN organizationinfo org_i ON
	org_i.id = loc.resource #>> '{mainOrganization,id}'
WHERE hcs.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="4000")'::jsquery

--- female-location ---
--EXPLAIN ANALYSE
   SELECT DISTINCT 
   	      "location".id,
          "location".txid,
          "location".ts,
          "location".resource_type,
          "location".cts,
          "location".resource || jsonb_build_object('name',concat ("location".resource ->> 'name',CASE WHEN org_info.resource ->> 'shortName' IS NOT NULL THEN concat (' ',org_info.resource ->> 'shortName') END,NULL)) AS resource
     FROM schedulerule sch
     JOIN "location" "location" 
       ON ("location".id = sch.resource #>> '{location,id}')
	      AND "location".id NOT IN ('0ea5546d-ce10-4a48-9d10-a74f338ef452', '1b6c6807-62f0-499b-8055-d25715a33778', '1d50b994-efed-4335-936e-2724518d04f7')
     JOIN organization org
       ON (org.id = sch.resource #>> '{mainOrganization,id}'
          AND org.resource @@ 'identifier.#(value = "1.2.643.5.1.13.13.12.2.21.1558" and system="urn:identity:oid:Organization")'::jsquery)
LEFT JOIN organizationinfo org_info 
       ON org_info.id = "location".resource #>> '{mainOrganization,id}'
    WHERE coalesce(sch.resource #>> '{planningHorizon,end}','infinity') >= '2021-04-28' 
    	  AND sch.resource -> 'availableTime' @@ '$.#.channel.#($="web")'::jsquery 
	      AND "location".resource ->> 'name' ILIKE 'Женск%' 	      
	      
SELECT *
FROM pg_indexes
WHERE tablename = 'schedulerule'


SELECT *
FROM schedulerule sch
WHERE 
	immutable_ts(coalesce(sch.resource #>> '{planningHorizon,end}', 'infinity')) >= '2021-01-19'
	AND sch.resource @@ 'availableTime.#.channel.#($ = web)'::jsquery	