--EXPLAIN ANALYZE 
SELECT pd.id pd_id
     , pd.resource->>'name' pd_display
     , ad.id ad_id
     , ad.resource#>>'{code,coding,0,display}' ad_display
     , od.id od_id
     , od.resource#>>'{code,coding,0,display}' od_display
   FROM observationdefinition od
   JOIN activitydefinition ad
     ON ad.resource @@ (concat('code.coding.#.system = "urn:CodeSystem:esli-li" and observationResultRequirement.#.id = "', od.id, '"'))::jsquery
   JOIN plandefinition pd
     ON pd.resource @@ (concat('type.coding.#(code="ESLI.TYPE_LI.2" and system="urn:CodeSystem:plandefinition-li-type") and action.#.definition.canonical = "' , 'ActivityDefinition/', ad.id,  '"'))::jsquery
WHERE od.resource @@ 'code.coding.#.system ="urn:CodeSystem:esli-li-test"'::jsquery
  

SELECT pd.id pd_id
     , pd.resource->>'name' pd_display
     , ad.id ad_id
     , ad.resource#>>'{code,coding,0,display}' ad_display
     , od.id od_id
     , od.resource#>>'{code,coding,0,display}' od_display
     , ((count(ad.id) OVER (PARTITION BY ad.id) = count(ad_c_id) OVER (PARTITION BY ad.id)) AND (count(od_c_id) OVER (PARTITION BY ad.id) > 1)) "ad_in_progress"
     , ((count(ad.id) OVER (PARTITION BY ad.id) = count(ad_c_id) OVER (PARTITION BY ad.id)) AND (count(od.id) OVER (PARTITION BY ad.id) = count(od_c_id) OVER (PARTITION BY ad.id))) "ad_completed"
     , od_c_id IS NOT NULL od_completed
FROM plandefinition pd
JOIN LATERAL (
    SELECT id, resource
    , (    SELECT id
            FROM activitydefinition ad_connected WHERE ad_connected.resource @@ concat('jurisdiction.#.coding.#(system="urn:CodeSystem:esli-li" and code = '
                                                                            , jsonb_path_query_first(ad_template.resource, '$.code.coding ? (@.system == "urn:CodeSystem:esli-li").code')
                                                                            ,')')::jsquery
                                                                            LIMIT 1) ad_c_id
    FROM activitydefinition ad_template
    WHERE ad_template.id = any(array(SELECT split_part(jsonb_path_query(pd.resource, '$.action.definition.canonical') #>> '{}', '/', 2)))
) ad ON true
JOIN LATERAL (
    SELECT od_template.id, od_template.resource
    , (SELECT id od_c_id
       FROM observationdefinition od_connected 
       WHERE od_connected.resource @@ concat('category.#.coding.#(system="urn:CodeSystem:esli-li-test" and code = '
                                             , jsonb_path_query_first(od_template.resource, '$.code.coding ? (@.system == "urn:CodeSystem:esli-li-test").code')
                                             ,')')::jsquery
       LIMIT 1) od_c_id
    FROM observationdefinition od_template
    WHERE od_template.id = any(array(SELECT jsonb_path_query(ad.resource, '$.observationResultRequirement.id') #>> '{}'))
) od ON true
WHERE pd.resource @@ 'type.coding.#(code="ESLI.TYPE_LI.2" and system="urn:CodeSystem:plandefinition-li-type")'::jsquery
--    AND (pd.resource->>'name' ILIKE '%кров%' 
--         or od.resource#>>'{code,coding,0,display}' ILIKE '%кров%'
--         or ad.resource#>>'{code,coding,0,display}' ILIKE '%кров%')         

SELECT jsonb_path_query_first(resource, '$.type.coding ? (@.system == "urn:CodeSystem:plandefinition-li-type").code'), count(*)
FROM plandefinition pd
GROUP BY 1

SELECT *
FROM pg_indexes
WHERE tablename = 'plandefinition'
