WITH schedules AS (
    SELECT *
    FROM schedulerule sch
    WHERE
      immutable_ts(COALESCE((sch.resource #>> '{planningHorizon,end}'), 'infinity')) BETWEEN current_date - INTERVAL '2 week' AND current_date + INTERVAL '2 week'
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
    'Чувашская Республика - Чувашия' region
    , jsonb_path_query_first(main_org.resource, '$.identifier ? (@.system=="urn:identity:oid:Organization")') #>> '{value}' mo_oid
    , main_org.resource #>> '{alias,0}' mo_short_name
    , COALESCE (jsonb_path_query_first(org.resource, '$.identifier ? (@.system=="urn:identity:oid:Organization")') #>> '{value}'
                , split_part((jsonb_path_query_first(prr.resource, '$.identifier ? (@.system=="urn:identity:frmr:PractitionerRole")') #>> '{value}'), '_', 2)) sp_oid
    , org.resource ->> 'name' sp_name
    , jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole")') #>> '{id}' mp_id
    , patient_fio(prr.resource -> 'derived') mp_fio
    , to_char(ds.d, 'DD.MM.YYYY') "date_report"
    , prr.id
    , jsonb_path_query_first(prr.resource, '$.code ? (exists (@.coding ? (@.system=="urn:CodeSystem:frmr.position")))') #>> '{text}' mp_dolgnost
    , identifier_value(prr.resource, 'urn:source:frmr2:PractitionerRole') mp_frmr2_id
    , ''  mp_stavka
    , CASE WHEN sch.resource @@ 'availableTime.#.channel.# = "web"'::jsquery THEN 'Доступен для записи через ЕПГУ' ELSE 'Не доступен для записи через ЕПГУ' END slots_type
    , CASE WHEN COALESCE((sch.resource @@ 'availableTime.#.channel.# = "web-referral"'::jsquery OR hcs.resource ->> 'reService' = 'true'), false) THEN 'Да' ELSE 'Нет' END slots_referral
    , sch.resource #>> '{minutesDuration}' slot_length
     , COALESCE(jsonb_array_length(schedule_slots(sch.id, ds.d::date, (ds.d + INTERVAL '1 day')::date)), 0) slots_created
    , slots_booked
    , 0 slots_free_after_booked
    , visits_absence
    , visits_success
from
(
	select d::date as d from (
		select generate_series(ds.ff, ds.to_date, interval '1 day') d
		from (
			select (current_date - interval '2 week') as ff,
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
    (SELECT count(*) slots_booked
      , count(*) FILTER (WHERE resource ->> 'status' = 'noshow') visits_absence
      , count(*) FILTER (WHERE resource ->> 'status' = 'arrived') visits_success
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

SELECT region,
  mo_oid,
  mo_short_name,
  sp_oid,
  sp_name,
  mp_id,
  mp_fio,
  date_report,
  mp_dolgnost,
  pc.rate mp_stavka,
  slots_type,
  slots_referral,
  slot_length::integer,
  slots_created,
  slots_booked,
  slots_free_after_booked,
  visits_absence,
  visits_success
FROM frmr2.weekly_report wr
JOIN frmr2.person_card pc 
  ON pc.person_card_id = wr.mp_frmr2_id::uuid;
