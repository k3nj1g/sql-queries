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
SELECT
--    sch.id schedule_id
    'Чувашская Республика - Чувашия' region_name
    , jsonb_path_query_first(main_org.resource, '$.identifier ? (@.system=="urn:identity:oid:Organization")') #>> '{value}' main_organization_oid
    , main_org.resource #>> '{alias,0}' main_organization_name
    , COALESCE (split_part((jsonb_path_query_first(prr.resource, '$.identifier ? (@.system=="urn:identity:frmr:PractitionerRole")') #>> '{value}'), '_', 2)
                , jsonb_path_query_first(org.resource, '$.identifier ? (@.system=="urn:identity:oid:Organization")') #>> '{value}') organization_oid
    , org.resource ->> 'name' organization_name
    , jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole")') #>> '{id}' practitionerrole_id
    , patient_fio(prr.resource -> 'derived') practitionerrole_name
    , to_char(ds.d, 'DD.MM.YYYY') "date"
    , jsonb_path_query_first(prr.resource, '$.code ? (exists (@.coding ? (@.system=="urn:CodeSystem:frmr.position")))') #>> '{text}' practitionerrole_position
    , NULL empty_col1
    , CASE WHEN sch.resource @@ 'availableTime.#.channel.# = "web"'::jsquery THEN 'Доступен' ELSE 'Не доступен' END available_for_web
    , CASE WHEN COALESCE((sch.resource @@ 'availableTime.#.channel.# = "web-referral"'::jsquery OR hcs.resource ->> 'reService' = 'true'), false) THEN 'Да' ELSE 'Нет' END available_for_requests
    , sch.resource #>> '{minutesDuration}' duration
     , COALESCE(jsonb_array_length(schedule_slots(sch.id, ds.d::date, (ds.d + INTERVAL '1 day')::date)), 0) slots_count
    , appointment_all
    , NULL empty_col2
    , appointment_no_show
    , appointment_arrived
from
(
	select d::date as d from (
		select generate_series(ds.ff, ds.to_date, interval '1 day') d
		from (
			select (current_date - interval '1 week') as ff,
			(current_date + interval '2 week') as to_date
		) as ds
	) as dd
) ds
left join lateral
	(
		select *
		from schedules_all sch
	) as sch on true
JOIN LATERAL
    (SELECT count(*) appointment_all
      , count(*) FILTER (WHERE resource ->> 'status' = 'noshow') appointment_no_show
      , count(*) FILTER (WHERE resource ->> 'status' = 'arrived') appointment_arrived
     FROM appointment app
     WHERE ((resource -> 'schedule') ->> 'id') = sch.id
       and (app.resource #>> '{end}') is not null
       AND immutable_tsrange((app.resource #>> '{start}'), (app.resource #>> '{end}')) && tsrange(ds.d, (ds.d + INTERVAL '1 day'))
       ) app
    ON true
JOIN practitionerrole prr
  ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole")') #>> '{id}'
JOIN organization main_org
  ON main_org.id = sch.resource #>> '{mainOrganization,id}'
JOIN organization org
  ON org.resource @@ logic_include(prr.resource, 'organization')
JOIN healthcareservice hcs
  ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
-- where prr.id = '24eaf523-16c7-4107-87d8-fa8afebbeea3'
-- ORDER BY sch.id
;