-- Clickhouse
WITH with_position_name AS (
  SELECT multiIf(
      cp.c_code = '110', 'врач-терапевт участковый',
      cp.c_code = '49', 'врач общей практики (семейный врач)',
      cp.c_code = '122', 'врач-хирург',
      cp.c_code = '54', 'врач-офтальмолог',
      cp.c_code = '53', 'врач-оториноларинголог',
      cp.c_code = '13', 'врач-акушер-гинеколог',
      cp.c_code = '87', 'врач-психиатр-нарколог',
      cp.c_code IN ('119', '120'), 'врач-фтизиатр',
      cp.c_code = '100', 'врач-стоматолог',
      cp.c_code = '103', 'врач-стоматолог-терапевт',
      cp.c_code = '59', 'врач-педиатр участковый',
      cp.c_code = '28', 'врач-детский хирург',
      cp.c_code = '101', 'врач-стоматолог детский',
      cp.c_code IN ('83', '85'), 'врач-психиатр детский (подростковый)',
      cp.c_code = '109', 'врач-терапевт',
      cp.c_code = '58', 'врач-педиатр',
      cp.c_code IN ('195', '144', '146', '334', '335', '145', '345'), 'фельдшер',
      'other')  position_name
    , ca.c_orig_id id
    , ca.d_start "start"
    , ca.d_meta_createdat "created"
    , ca.c_servicetype_code service
    , ca.c_appointmenttype_code appointment_type
  FROM visiology.cd_appointment ca
    JOIN visiology.cs_practitionerroles cp
      ON cp.c_orig_id = ca.c_participant_practitionerRole
     AND cp.c_code IN ('110', '49', '122', '54', '53', '13', '87', '119', '120', '100', '103', '59', '28', '101', '83', '85', '109', '58', '195', '144', '146', '334', '335', '145', '345')
  WHERE ca.d_start >= '2024-02-12'
    AND ca.d_start < '2024-02-17'
    AND ca.c_status = 'arrived')  
, grouped AS (
  SELECT position_name
    , count(DISTINCT app.id) count_app
    , AVG(((toDateTime(app."start") - toDateTime(app."created")) / 60 / 60 / 24)) avg_day
    , count(DISTINCT app.id) FILTER (WHERE NOT app.service IN ('153', '999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199')) count_app_poly_all
    , count(DISTINCT app.id) FILTER (WHERE NOT app.service IN ('153', '999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND appointment_type IN ('WALKIN', 'EMERGENCY')) count_app_poly_walkin
    , count(DISTINCT app.id) FILTER (WHERE NOT app.service IN ('153', '999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND appointment_type = 'ROUTINE' AND service IN ('184', '185', '186', '187', '188', '189', '199')) count_app_poly_routine
    , count(DISTINCT app.id) FILTER (WHERE app.service IN ('999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199')) count_app_prof_all
    , count(DISTINCT app.id) FILTER (WHERE app.service IN ('999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND service = '3000') count_app_prof_walkin
    , count(DISTINCT app.id) FILTER (WHERE app.service IN ('999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND service IN ('3000', '999')) count_app_prof_routine
    , count(DISTINCT app.id) FILTER (WHERE app.service = '153') count_app_home_all
  FROM with_position_name app
  GROUP BY position_name)
SELECT position_name "Должность"
  , count_app "Столбец C"
  , round(avg_day, 1) "Столбец D"
  , count_app_poly_all "Столбец E"
  , count_app_poly_walkin "Столбец F"
  , count_app_poly_routine "Столбец G"
  , count_app_prof_all "Столбец H"
  , count_app_prof_walkin "Столбец I"
  , count_app_prof_routine "Столбец J"
  , count_app_home_all "Столбец K"
  , count_app_home_all "Столбец L"
FROM grouped
ORDER BY position_name
SETTINGS final = 1;

-- Slots
WITH schedules AS ( 
  SELECT sch.id sch_id
    , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' doctor_position 
  FROM schedulerule sch
  JOIN practitionerrole prr
    ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
  WHERE sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
      AND immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && immutable_tsrange('2024-02-12', '2024-02-17'))
, slots AS (
  SELECT doctor_position
    , schedule_slots_with_appointment(sch_id, '2024-02-12', '2024-02-17') generated_slots 
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
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE jsonb_path_exists(slot, '$.appointment ? (@.status != "cancelled" && @.from == "web")')))), 0) slots_with_appointment_web
  FROM slots
)
, slots_grouped AS (
  SELECT doctor_position
    , sum(slots_count) slots_count
    , sum(concurrent_slots) concurrent_slots_count
    , sum(slots_with_appointment) slots_with_appointment_count
    , sum(slots_with_appointment_web) slots_with_appointment_web_count
  FROM slots_count
  GROUP BY doctor_position
)
SELECT 
  sum(slots_count) FILTER (WHERE doctor_position IN ('110')) "врач-терапевт участковый"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('49')) "врач общей практики (семейный врач)"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('122')) "врач-хирург"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('54')) "врач-офтальмолог"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('53')) "врач-оториноларинголог"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('13')) "врач-акушер-гинеколог"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('87')) "врач-психиатр-нарколог"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('119', '120')) "врач-фтизиатр"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('100')) "врач-стоматолог"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('103')) "врач-стоматолог-терапевт"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('59')) "врач-педиатр участковый"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('28')) "врач-детский хирург"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('101')) "врач-стоматолог детский"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('83','85')) "врач-психиатр детский (подростковый)"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('109')) "врач-терапевт"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('58')) "врач-педиатр"
  , sum(slots_count) FILTER (WHERE doctor_position IN ('195')) "фельдшер"
  , sum(slots_count) "Всего слотов"
FROM slots_grouped;

--- 1 page
WITH schedules AS ( 
  SELECT sch.id sch_id
    , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' doctor_position 
  FROM schedulerule sch
  JOIN practitionerrole prr
    ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
  WHERE sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
      AND immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && immutable_tsrange('2024-02-12', '2024-02-17'))
, slots AS (
  SELECT doctor_position
    , schedule_slots_with_appointment(sch_id, '2024-02-12', '2024-02-17') generated_slots 
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