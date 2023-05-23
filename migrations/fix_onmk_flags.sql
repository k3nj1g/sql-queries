--EXPLAIN 
SELECT count(*)
FROM flag f
JOIN patient p 
  ON p.id = f.resource #>> '{subject,id}'
JOIN documentreference dr 
  ON dr.resource -> 'subject' @@ logic_revinclude(p.resource, p.id)
  AND knife_extract_min_timestamptz(dr.resource, '[["date"]]') = knife_extract_min_timestamptz(f.resource, '[["period", "start"]]')
WHERE f.resource @@ 'code.coding.#.code = "R01.1" and not period.end = *'::jsquery;

DROP MATERIALIZED VIEW documentrs_onmk;

CREATE MATERIALIZED VIEW documents_onmk AS 
WITH f_p AS (
	SELECT (f.resource #>> '{period,start}')::date f_start
	  , p.id p_id
	  , p.resource p_resource
	FROM flag f
	JOIN patient p 
	  ON p.id = f.resource #>> '{subject,id}'
	WHERE f.resource @@ 'code.coding.#.code = "R01.1" and not period.end = *'::jsquery
)
SELECT dr.*
FROM f_p
JOIN documentreference dr 
  ON dr.resource @@ logic_revinclude(p_resource, p_id, 'subject')
  AND (dr.resource ->> 'date')::date = f_start
  AND jsonb_path_query_first(dr.resource, '$.type.coding ? (@.system == "urn:CodeSystem:documentreference-type").code') #>> '{}' LIKE 'vipis_epikriz%';

SELECT (jsonb_path_query_first(obs.resource,
		'$.component ? (exists(@.code.coding ? (@.system=="urn:CodeSystem:shrm-parameters" && @.code=="shrm"))).value.CodeableConcept.coding ? (@.system=="urn:CodeSystem:shrm-values").code') #>> '{}')::text
		IN ('0', '1')
--   obs.id, array_agg(dr.resource #>> '{context,encounter,0,identifier,value}'), count(*)
--	,jsonb_path_query_first(obs.resource, '$.component ? (exists(@.code.coding ? (@.system=="urn:CodeSystem:shrm-parameters" && @.code=="shrm"))).value.CodeableConcept.coding ? (@.system=="urn:CodeSystem:shrm-values").code') #>> '{}'
FROM documents_onmk dr
JOIN public.observation obs
  ON obs.resource @@ concat('code.coding.#(system="urn:CodeSystem:cardioSigns-type" and code="onmk-rehab") and encounter.identifier.value=', dr.resource #> '{context,encounter,0,identifier,value}')::jsquery
--  AND obs.id = '477a7a73-0f55-4277-a33b-69c9b567a25a'
WHERE COALESCE(dr.resource->>'active','true') = 'true'
  AND dr.resource #>> '{context,encounter,0,identifier,value}' = 'cb8e875e-f927-41ce-8bdb-dcc9ace53380' 
--GROUP BY 1
--HAVING count(*) > 1  

WITH to_update AS (
SELECT 
	f.id
	, jsonb_set_lax(f.resource,
	'{period,end}',
	to_jsonb(((f.resource #>> '{period,start}')::date + concat((CASE
		WHEN shrm.shrm IN ('0', '1') THEN '1'
		ELSE '2'
	END) ,
	' years')::INTERVAL)::date)) resource
FROM
	documents_onmk dr
JOIN LATERAL
  (
	SELECT
		(jsonb_path_query_first(obs.resource, '$.component ? (exists(@.code.coding ? (@.system=="urn:CodeSystem:shrm-parameters" && @.code=="shrm"))).value.CodeableConcept.coding ? (@.system=="urn:CodeSystem:shrm-values").code') #>> '{}')::TEXT shrm
	FROM
		public.observation obs
	WHERE
		obs.resource @@ concat('code.coding.#(system="urn:CodeSystem:cardioSigns-type" and code="onmk-rehab") and encounter.identifier.value=',
		dr.resource #> '{context,encounter,0,identifier,value}')::jsquery) shrm ON
	TRUE
JOIN patient p 
  ON
	p.resource @@ logic_include(dr.resource,
	'subject')
	AND COALESCE(p.resource ->> 'active',
	'true') = 'true'
JOIN flag f 
  ON
	f.resource #>> '{subject,id}' = p.id
	AND f.resource -> 'code' @@ 'coding.#.code = "R01.1" and not period.end = *'::jsquery
WHERE
	COALESCE(dr.resource->>'active',
	'true') = 'true')
UPDATE flag f 
SET resource = tu.resource
FROM to_update tu
WHERE f.id = tu.id
RETURNING f.*
