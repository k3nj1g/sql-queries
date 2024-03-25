WITH schedules AS ( 
  SELECT sch.id sch_id
    , sch.resource #>> '{mainOrganization,display}' mo_name
  FROM schedulerule sch
  JOIN practitionerrole prr
    ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
     AND jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('110', '49', '122', '54', '53', '13', '87', '119', '120', '100', '103', '59', '28', '101', '83', '85', '109', '58', '195', '144', '146', '334', '335', '145', '345')
  WHERE sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
    AND immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && immutable_tsrange('2024-03-04', '2024-03-11')
--    AND sch.resource #>> '{mainOrganization,id}' = (SELECT id FROM organization WHERE identifier_value(resource, 'urn:identity:oid:Organization') = '1.2.643.5.1.13.13.12.2.21.1525')
)
, slots AS (
  SELECT mo_name
    , schedule_slots_with_appointment(sch_id, '2024-03-04', '2024-03-11') generated_slots 
  FROM schedules sch
)
, slots_count AS (
  SELECT mo_name
    , COALESCE(jsonb_array_length(generated_slots), 0) slots_count
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE slot->'channel' @> '["kc-mo","web","doctor"]'::jsonb))), 0) concurrent_slots
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE (slot->>'appointment') IS NOT NULL))), 0) slots_with_appointment
    , COALESCE((jsonb_array_length((SELECT jsonb_agg(slot) FROM jsonb_array_elements(generated_slots) slots(slot) WHERE jsonb_path_exists(slot, '$.appointment ? (@.status != "cancelled" && @.from == "web")')))), 0) slots_with_appointment_web
  FROM slots
)
SELECT mo_name
  , sum(concurrent_slots) concurrent_slots_count
  , sum(slots_with_appointment) slots_with_appointment_count
  , sum(slots_with_appointment_web) slots_with_appointment_web_count
  , sum(slots_count) "Всего слотов"
FROM slots_count
GROUP BY mo_name;