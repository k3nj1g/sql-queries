WITH neo AS (
	SELECT concat(p.resource #>> '{name,0,family}', ' ', p.resource #>> '{name,0,given,0}', ' ', p.resource #>> '{name,0,given,1}', ' (', p.resource ->> 'birthDate', ')') patient
	      , jsonb_path_query_first(tmk.resource, '$.reasonCode.coding ? (@.system == "urn:CodeSystem:icd-10")') diagnosis
          , jsonb_path_query_first(tmk.resource, '$.performerInfo.requestActionHistory ? (@.action == "completed").date') "date"
          , jsonb_path_query_first(tmk.resource, '$.performer ? (@.resourceType == "Organization").display') #>> '{}' performer
	      , count(*) OVER (PARTITION BY p.id, jsonb_path_query_first(tmk.resource, '$.reasonCode.coding ? (@.system == "urn:CodeSystem:icd-10")') 
	                       ORDER BY jsonb_path_query_first(tmk.resource, '$.performerInfo.requestActionHistory ? (@.action == "completed")'))
     FROM servicerequest tmk
     JOIN patient p ON p.id = tmk.resource #>> '{subject,id}'
    WHERE tmk.resource @@ 'category.#.coding.#(system = "urn:CodeSystem:servicerequest-category" and code = "TMK")'  
                          'and performerType.coding.#(system = "urn:CodeSystem:health-care-profiles" and code = "55")'  
 		                  'and status = completed'::jsquery
          AND tmk.resource ->> 'authoredOn' BETWEEN '2021-03-01' AND '2021-04-30')
SELECT ROW_NUMBER () OVER ()
       , patient
	   , performer
	   , "date"
       , CASE WHEN "count" = 1 THEN 'первично' WHEN "count" > 1 THEN 'повторно' ELSE '' END tmk
       , concat(diagnosis ->> 'code', ' ', diagnosis ->> 'display')
  FROM neo
  ORDER BY patient, "date"
--  JOIN patient p ON p.id = neo.id
  