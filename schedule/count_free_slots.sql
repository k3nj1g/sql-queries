CREATE MATERIALIZED VIEW app_count AS 
WITH filtered AS (
  SELECT s.id sch_id
  FROM schedulerule s
  WHERE s.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
    AND immutable_tsrange(s.resource #>> '{planningHorizon,start}', COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && tsrange('2022-12-05', ('2022-12-05'::date + INTERVAL '4 weeks')::timestamp))
, slots AS (
  SELECT sch_id, jsonb_array_elements((schedule_slots(sch_id, '2022-12-05', ('2022-12-05'::date + INTERVAL '4 weeks')::date))) slot
  FROM filtered
)
SELECT count(*) "all"
  , count(*) FILTER (WHERE jsonb_array_length(slot->'channel') > 1) "many"
  , count(*) FILTER (WHERE jsonb_array_length(slot->'channel') > 1 AND (slot->'channel') ? 'web') "with_web"
FROM slots;

-- Доля слотов для ЕПГУ
WITH schedules AS (
  SELECT DISTINCT ON (sch_id) 
    s.id sch_id
    , CASE 
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '110' THEN 'Врач-терапевт'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '13' THEN 'Акушер-гинеколог'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '49' THEN 'Врач общей практики (Семейный врач)'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '53' THEN 'Оториноларинголог'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '54' THEN 'Офтальмолог'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '59' THEN 'Педиатр участковый'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('83','85') THEN 'Психиатр детский + подростковый'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '87' THEN 'Психиатр-нарколог'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '100' THEN 'Стоматолог'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '101' THEN 'Стоматолог детский'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '103' THEN 'Стоматолог-терапевт'        
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('119','120') THEN 'Фтизиатр'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '122' THEN 'Хирург'
        WHEN jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '28' THEN 'Врачи детские хирурги'
      END AS position
  FROM practitionerrole prr
    JOIN schedulerule s
      ON s.resource @@ concat('actor.#(resourceType="PractitionerRole" and id="',prr.id,'")')::jsquery
     AND immutable_tsrange(s.resource #>> '{planningHorizon,start}', COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && tsrange('2022-12-01', '2022-12-31', '[]')
  WHERE prr.resource @@ 'code.#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("110", "59", "49", "122","13","54","53","100","103","101","83","85","119","120","87","28"))'::jsquery
    AND coalesce(prr.resource -> 'active','true') = 'true')
, slots as (    
  SELECT position
    , sum(jsonb_array_length((schedule_slots(sch_id, '2022-12-01', '2022-12-31', 'web')))) slot_web_12_22
    , sum(jsonb_array_length((schedule_slots(sch_id, '2022-12-01', '2022-12-31')))) slot_all_12_22
    , sum(jsonb_array_length((schedule_slots(sch_id, '2023-01-01', '2023-01-20', 'web')))) slot_web_01_23
    , sum(jsonb_array_length((schedule_slots(sch_id, '2023-01-01', '2023-01-20')))) slot_all_01_23
  FROM schedules
  GROUP BY position)
SELECT position
  , to_char((100 * slot_web_12_22 / slot_all_12_22::float), 'fm99D00%') "12_2022"
  , to_char((100 * slot_web_01_23 / slot_all_01_23::float), 'fm99D00%') "01_2023"
FROM slots;