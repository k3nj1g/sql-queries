WITH schedules AS ( 
  SELECT sch.id sch_id
    , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' doctor_position 
  FROM schedulerule sch
  JOIN practitionerrole prr
    ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
  WHERE sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
      AND immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && immutable_tsrange('2024-01-29', '2024-02-04'))
, slots AS (
  SELECT doctor_position
    , schedule_slots_with_appointment(sch_id, '2024-01-29', '2024-02-04') generated_slots 
  FROM schedules sch
)
, slots_test AS (
  SELECT (SELECT jsonb_agg(slot->'appointment') FROM jsonb_array_elements(generated_slots) slots(slot) WHERE slot->'appointment' IS NOT NULL) slot
  FROM slots
)
, slots_count AS (
  SELECT doctor_position
    , COALESCE(jsonb_array_length(generated_slots), 0) slots_count
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE slot->'channel' @> '["kc-mo","web","doctor"]'::jsonb))), 0) concurrent_slots
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE (slot->>'appointment') IS NOT NULL))), 0) slots_with_appointment
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE jsonb_path_exists(slot, '$.appointment.resource ? (@.status != "cancelled" && @.from == "web")')))), 0) slots_with_appointment_web
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE jsonb_path_exists(slot, '$.appointment.resource ? (@.status != "cancelled" && @.from == "kc")')))), 0) slots_with_appointment_kc_mo
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE jsonb_path_exists(slot, '$.appointment.resource ? (@.status != "cancelled" && @.from == "reg")')))), 0) slots_with_appointment_reg
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE jsonb_path_exists(slot, '$.appointment.resource ? (@.status == "cancelled")')))), 0) slots_with_appointment_cancelled
  FROM slots
)
, slots_grouped AS (
  SELECT doctor_position
    , sum(slots_count) slots_count
    , sum(concurrent_slots) concurrent_slots_count
    , sum(slots_with_appointment) slots_with_appointment_count
    , sum(slots_with_appointment_web) slots_with_appointment_web_count
    , sum(slots_with_appointment_kc_mo) slots_with_appointment_kc_mo_count    
    , sum(slots_with_appointment_reg) slots_with_appointment_reg_count
    , sum(slots_with_appointment_cancelled) slots_with_appointment_cancelled_count
  FROM slots_count
  GROUP BY doctor_position
)
--SELECT *
--FROM slots_test; 
SELECT sum(slots_count) FILTER (WHERE doctor_position IN ('110', '13', '49', '53', '54', '59', '83', '85', '87', '100', '101', '103', '119', '120', '122', '28')) column_c
  , sum(slots_count) column_d
  , sum(concurrent_slots_count) FILTER (WHERE doctor_position IN ('110', '13', '49', '53', '54', '59', '83', '85', '87', '100', '101', '103', '119', '120', '122', '28')) column_e
  , sum(concurrent_slots_count) column_f
  , sum(slots_with_appointment_count) FILTER (WHERE doctor_position IN ('110', '13', '49', '53', '54', '59', '83', '85', '87', '100', '101', '103', '119', '120', '122', '28')) column_g
  , sum(slots_with_appointment_count) column_h
  , sum(slots_with_appointment_web_count) FILTER (WHERE doctor_position IN ('110', '13', '49', '53', '54', '59', '83', '85', '87', '100', '101', '103', '119', '120', '122', '28')) column_i
  , sum(slots_with_appointment_web_count) column_j
  , sum(slots_with_appointment_kc_mo_count) FILTER (WHERE doctor_position IN ('110', '13', '49', '53', '54', '59', '83', '85', '87', '100', '101', '103', '119', '120', '122', '28')) column_o
  , sum(slots_with_appointment_kc_mo_count) column_p
  , sum(slots_with_appointment_reg_count) FILTER (WHERE doctor_position IN ('110', '13', '49', '53', '54', '59', '83', '85', '87', '100', '101', '103', '119', '120', '122', '28')) column_q
  , sum(slots_with_appointment_reg_count) column_r
  , sum(slots_with_appointment_cancelled_count) FILTER (WHERE doctor_position IN ('110', '13', '49', '53', '54', '59', '83', '85', '87', '100', '101', '103', '119', '120', '122', '28')) column_u
  , sum(slots_with_appointment_cancelled_count) column_v
FROM slots_grouped;

