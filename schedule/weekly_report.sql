WITH schedules AS (
    SELECT *
    FROM schedulerule sch
    WHERE
      immutable_ts(COALESCE((sch.resource #>> '{planningHorizon,end}'), 'infinity')) BETWEEN current_date - INTERVAL '1 week' AND current_date + INTERVAL '2 week'
      AND sch.resource @@ 'actor.#.resourceType = "PractitionerRole"'::jsquery
    -- LIMIT 100  
)
, schedules_web AS (
    SELECT *
    FROM schedules
    WHERE resource @@ 'availableTime.#.channel.# = "web"'::jsquery
)
, schedules_not_web AS (
    SELECT *
    FROM schedules
    WHERE NOT resource @@ 'availableTime.#.channel.# = "web"'::jsquery
)
, schedules_all AS (
    SELECT *
    FROM schedules_web
    UNION ALL
    SELECT *
    FROM schedules_not_web
)
, schedule_with_appointments AS (
  SELECT sch.*, app.*
  FROM schedules_all sch
  JOIN LATERAL
    (SELECT count(*) appointment_all
      , count(*) FILTER (WHERE resource ->> 'status' = 'noshow') appointment_no_show
      , count(*) FILTER (WHERE resource ->> 'status' = 'arrived') appointment_arrived
     FROM appointment app
     WHERE ((resource -> 'schedule') ->> 'id') = sch.id
       AND NOT app.resource ->> 'status' = 'cancelled'
       AND immutable_tsrange((app.resource #>> '{start}'), (app.resource #>> '{end}')) && tsrange((current_date - INTERVAL '1 week'), (current_date + INTERVAL '2 week'))) app
    ON true
)
SELECT
    sch.id schedule_id
    , jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole")') #>> '{id}' practitionerrole_id
    , patient_fio(prr.resource -> 'derived') practitionerrole_name
    , to_char(current_date, 'DD.MM.YYYY') "date"
    , jsonb_path_query_first(prr.resource, '$.code ? (exists (@.coding ? (@.system=="urn:CodeSystem:frmr.position")))') #>> '{text}' practitionerrole_position
    , sch.resource @@ 'availableTime.#.channel.# = "web"'::jsquery available_for_web
    , COALESCE((sch.resource @@ 'availableTime.#.channel.# = "web-referral"'::jsquery OR hcs.resource ->> 'reService' = 'true'), false) available_for_requests
    , sch.resource #>> '{minutesDuration}' duration
     , COALESCE(jsonb_array_length(schedule_slots(sch.id, (current_date - INTERVAL '1 week')::date, (current_date + INTERVAL '2 week')::date)), 0) slots_count
    , appointment_all
    , appointment_no_show
    , appointment_arrived
FROM
    schedule_with_appointments sch
JOIN practitionerrole prr
  ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole")') #>> '{id}'
JOIN healthcareservice hcs 
  ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
-- ORDER BY sch.id
;

select * from pg_indexes where tablename='appointment';