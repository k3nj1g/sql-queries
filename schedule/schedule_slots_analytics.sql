WITH filtered AS (
  SELECT sch.id sch_id
    , schedule_slots_with_appointment(sch.id, current_date, (current_date + INTERVAL '2 weeks')::date) slots_all
    , jsonb_path_query_first(hcs.resource, '$.type.coding ? (@.system=="urn:CodeSystem:service").code') #>> '{}' service_code
  FROM schedulerule sch
  -- FROM (select * from schedulerule sch WHERE immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && tsrange(current_date, current_date + INTERVAL '2 days', '[]') limit 100) sch
  JOIN healthcareservice hcs
    ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
  WHERE immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) 
         && tsrange(current_date, current_date + INTERVAL '2 weeks', '[]'))
, derived AS (
  SELECT *
    , (SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all) slots(slot) WHERE slot#>>'{appointment,id}' IS NOT NULL) slots_all_busy
    , (SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all) slots(slot) WHERE slot->'channel' ?| '{kc-mo,web,doctor}'::text[]) concurrent_slots
    , (SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all) slots(slot) WHERE slot#>>'{appointment,id}' IS NOT NULL AND slot->'channel' ?| '{kc-mo,web,doctor}'::text[]) slots_all_busy_concurrent
  FROM filtered)
, slots_count AS (
  SELECT *
    , jsonb_array_length(slots_all) slots_all_count
    , jsonb_array_length(concurrent_slots) concurrent_slots_count
    , jsonb_array_length(slots_all_busy) slots_all_busy_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'web')) slots_busy_web_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'kc')) slots_busy_kc_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'kc-mo')) slots_busy_kc_mo_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'reg')) slots_busy_reg_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy) slots(slot) WHERE slot#>>'{appointment,resource,from}' IN ('doctor','fap','medNurse'))) slots_busy_other_count
    , jsonb_array_length(slots_all_busy_concurrent) slots_busy_concurrent_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy_concurrent) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'web')) slots_busy_concurrent_web_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy_concurrent) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'kc')) slots_busy_concurrent_kc_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy_concurrent) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'kc-mo')) slots_busy_concurrent_kc_mo_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy_concurrent) slots(slot) WHERE slot#>>'{appointment,resource,from}' = 'reg')) slots_busy_concurrent_reg_count
    , jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(slots_all_busy_concurrent) slots(slot) WHERE slot#>>'{appointment,resource,from}' IN ('doctor','fap','medNurse'))) slots_busy_concurrent_other_count
  FROM derived)
SELECT sum(slots_all_count) "всего"
  , sum(slots_all_count) FILTER (WHERE service_code='3000') "для диспансеризации"
  , sum(concurrent_slots_count) "конкурентных"
  , sum(concurrent_slots_count) FILTER (WHERE service_code='3000') "конкурентных для диспансеризации"
  , sum(slots_all_busy_count) "занятых"
  , sum(slots_busy_concurrent_count) "занятых конкурентных"
  , sum(slots_busy_concurrent_web_count) "занятых конкурентных с ЕПГУ"
  , 0 as "занятых конкурентных с регпортала"
  , sum(slots_busy_concurrent_kc_count) "занятых конкурентных с единого колл-центра"
  , coalesce(sum(slots_busy_concurrent_kc_mo_count), 0) "занятых конкурентных с колл-центра мо"
  , sum(slots_busy_concurrent_reg_count) "занятых конкурентных с регистратуры"
  , 0 as "занятых конкурентных с инфомата"
  , sum(slots_busy_concurrent_other_count) "занятых конкурентных прочие"
  , sum(slots_all_busy_count) FILTER (WHERE service_code IN ('3000','195)')) "занятых для дисп"
  , sum(slots_busy_web_count) FILTER (WHERE service_code IN ('3000','195)')) "занятых для дисп с ЕПГУ"
  , 0 as "занятых для дисп с регионального портала"
  , coalesce(sum(slots_busy_kc_count) FILTER (WHERE service_code IN ('3000','195)')), 0) "занятых для дисп с единого колл-центра"
  , coalesce(sum(slots_busy_kc_mo_count) FILTER (WHERE service_code IN ('3000','195)')), 0) "занятых для дисп с колл-центра мо"
  , coalesce(sum(slots_busy_reg_count) FILTER (WHERE service_code IN ('3000','195)')), 0) "занятых для дисп с регистратуры"
  , 0 as "занятых для дисп с инфомата"
  , coalesce(sum(slots_busy_other_count) FILTER (WHERE service_code IN ('3000','195)')), 0) "занятых для дисп прочие"
FROM slots_count;