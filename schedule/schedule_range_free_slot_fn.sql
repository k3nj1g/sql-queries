-- DROP FUNCTION schedule_range_free_slot(sch_id TEXT, "start" date, "end" date);

CREATE OR REPLACE FUNCTION schedule_range_free_slot(sch_id TEXT, "start" date, "end" date)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
SELECT (
  WITH global_schedule AS (
    SELECT id,
           jsonb_set(
             jsonb_set(resource
                       ,'{replacement}'::text[]
                       ,coalesce((SELECT jsonb_agg(replacement) AS replacement
                                  FROM jsonb_array_elements(resource -> 'replacement') replacement
                                  WHERE cast(replacement ->> 'date' AS timestamp) >= current_timestamp::timestamp)
                                 ,'[]'))
             ,'{notAvailable}'::text[]
             ,coalesce((SELECT jsonb_agg(not_available) AS not_available
                        FROM jsonb_array_elements(resource -> 'notAvailable') not_available
                        WHERE coalesce(cast(not_available #>> '{during,end}' AS timestamp),'infinity') >= current_timestamp::timestamp)
                       ,'[]')) AS resource
    FROM scheduleruleglobal
    LIMIT 1)
  , not_available AS (
    SELECT jsonb_array_elements(gs.resource->'notAvailable') -> 'during' value
    FROM global_schedule gs
    UNION
    SELECT jsonb_array_elements(s.resource->'notAvailable') -> 'during' value
   )
  , not_available_range as (
    SELECT tsrange(CAST(not_available.value ->> 'start' AS timestamp), CAST((CAST(not_available.value ->> 'end' AS timestamp) + INTERVAL '1 minute') AS timestamp)) AS "range" 
    FROM not_available)
  , init_interval(interval) AS 
    (SELECT tsrange(timezone('Europe/Moscow',current_timestamp), (cast((timezone('Europe/Moscow',current_timestamp) + CAST(concat(CAST(resource #>> '{planningActive,quantity}' AS text), ' week') AS interval)) AS date) + '1 day'::interval)) 
            * tsrange(CAST(resource #>> '{planningHorizon,start}' AS timestamp), CAST(resource #>> '{planningHorizon,end}' AS timestamp))
            * tsrange("start","end")) 
  , series_of_day AS 
  (SELECT cast(generate_series
                (lower((SELECT "interval" FROM init_interval LIMIT 1))
                ,upper((SELECT "interval" FROM init_interval LIMIT 1))
                , interval '1 day') AS date) AS "day")
  , select_day_of_week AS 
  (SELECT ('{mon , tue , wed , thu , fri , sat , sun}'::text[])[extract(ISODOW FROM DAY)] AS day_of_week,
          ('{even,odd}'::text[])[mod(extract(DAY FROM DAY)::integer,2) + 1] AS parity,
         "day"
   FROM series_of_day)
  , av_t AS (
      SELECT v
      FROM jsonb_array_elements(resource->'availableTime') "at"(v))
  , av_t_by_d AS (
      SELECT jsonb_build_object('start', av_t.v->'availableStartTime'
                                , 'end', av_t.v->'availableEndTime'
                                , 'channel', av_t.v->'channel'
                                , 'parity', av_t.v->'parity'
                                , 'day-of-week', dow.v
                                ) sch_day
      FROM av_t, jsonb_array_elements(av_t.v->'daysOfWeek') dow(v))
  , available_time AS (
    SELECT tsrange(("day"."day" + (sch_day ->> 'start')::time), ("day"."day" + (sch_day ->> 'end')::time)) "range"
      , "day".day_of_week
      , sch_day -> 'channel' channel
      , (CASE WHEN (s.resource->>'minutesDuration') IS NOT NULL THEN concat((s.resource->>'minutesDuration'), ' min')::INTERVAL
              ELSE (SELECT concat((hcs.resource->>'minutesDuration'), ' min')::INTERVAL 
                    FROM healthcareservice hcs          
                    WHERE hcs.id = s.resource #>> '{healthcareService,0,id}')
         END) "interval"
    FROM select_day_of_week "day"
      , av_t_by_d
    WHERE (sch_day ->> 'parity' IS NULL OR "day".parity = sch_day ->> 'parity')
      AND "day".day_of_week = sch_day ->> 'day-of-week')
  , series_of_available_time AS (
    SELECT generate_series(lower("range"), upper("range"), "interval") "begin"
      , "range" available_time
      , day_of_week 
      , channel
      , "interval" duration
    FROM available_time)
  , base_slot AS (
    SELECT tsrange("begin", ("begin" + duration)) AS slot
      , day_of_week
      , channel
      , available_time 
    FROM series_of_available_time)
  , checked_slot AS (
    SELECT slot.slot
      , slot.day_of_week
      , slot.channel 
    FROM base_slot slot 
    LEFT JOIN not_available_range not_available 
           ON not_available."range" && slot.slot 
    WHERE not_available IS NULL 
      AND slot.available_time @> slot.slot 
      AND (SELECT "interval" FROM init_interval LIMIT 1) @> slot.slot)
  SELECT row_to_json(s.*) slot
  FROM (SELECT to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "begin"
          , to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "end"
          , slot.day_of_week
          , slot.channel
          , a.id AS appointment 
        FROM checked_slot slot 
          LEFT JOIN appointment a 
                 ON (immutable_tsrange(a.resource#>>'{start}',a.resource#>>'{end}') && slot.slot 
                AND a.resource -> 'schedule' ->> 'id' = sch_id
                AND jsonb_path_query_first(a.resource, '$.appointmentType.coding ? (@.system=="http://terminology.hl7.org/CodeSystem/v2-0276").code') #>> '{}' = 'ROUTINE')
        WHERE a IS NULL
        ORDER BY "begin"
        LIMIT 1) s) c
FROM schedulerule s 
WHERE id = sch_id
$function$;


CREATE OR REPLACE FUNCTION schedule_range_free_slot(sch_id TEXT, "start" date, "end" date, channel_arg text)
 RETURNS text
 LANGUAGE sql
AS $function$
SELECT (
  WITH global_schedule AS (
    SELECT id,
           jsonb_set(
             jsonb_set(resource
                       ,'{replacement}'::text[]
                       ,coalesce((SELECT jsonb_agg(replacement) AS replacement 
                                  FROM jsonb_array_elements(resource -> 'replacement') replacement 
                                  WHERE cast(replacement ->> 'date' AS timestamp) >= current_timestamp::timestamp)
                                 ,'[]'))
             ,'{notAvailable}'::text[]
             ,coalesce((SELECT jsonb_agg(not_available) AS not_available 
                        FROM jsonb_array_elements(resource -> 'notAvailable') not_available 
                        WHERE coalesce(cast(not_available #>> '{during,end}' AS timestamp),'infinity') >= current_timestamp::timestamp)
                       ,'[]')) AS resource
    FROM scheduleruleglobal
    LIMIT 1)
  , not_available AS (
    SELECT jsonb_array_elements(gs.resource->'notAvailable') -> 'during' value
    FROM global_schedule gs
    UNION
    SELECT jsonb_array_elements(s.resource->'notAvailable') -> 'during' value
   )
  , not_available_range as (
    SELECT tsrange(CAST(not_available.value ->> 'start' AS timestamp), CAST((CAST(not_available.value ->> 'end' AS timestamp) + INTERVAL '1 minute') AS timestamp)) AS "range" 
    FROM not_available)
  , init_interval AS 
    (SELECT (tsrange(timezone('Europe/Moscow', "start"), (cast((timezone ('Europe/Moscow', "end") + CAST(concat(CAST(resource #>> '{planningActive,quantity}' AS text), ' week') AS interval)) AS date) + '1 day'::interval)) * 
            tsrange(CAST(resource #>> '{planningHorizon,start}' AS timestamp), CAST(resource #>> '{planningHorizon,end}' AS timestamp))) AS "interval")
  , series_of_day AS 
  (SELECT cast(generate_series
                (lower((SELECT "interval" FROM init_interval LIMIT 1))
                ,upper((SELECT "interval" FROM init_interval LIMIT 1))
                , interval '1 day') AS date) AS "day")
  , select_day_of_week AS 
  (SELECT ('{mon , tue , wed , thu , fri , sat , sun}'::text[])[extract(ISODOW FROM DAY)] AS day_of_week,
          ('{even,odd}'::text[])[mod(extract(DAY FROM DAY)::integer,2) + 1] AS parity,
         "day"
   FROM series_of_day)
  , av_t AS (
      SELECT v
      FROM jsonb_array_elements(resource->'availableTime') "at"(v))
  , av_t_by_d AS (
      SELECT jsonb_build_object('start', av_t.v->'availableStartTime'
                                , 'end', av_t.v->'availableEndTime'
                                , 'channel', av_t.v->'channel'
                                , 'parity', av_t.v->'parity'
                                , 'day-of-week', dow.v
                                ) sch_day
      FROM av_t, jsonb_array_elements(av_t.v->'daysOfWeek') dow(v))
  , available_time AS (
    SELECT tsrange(("day"."day" + (sch_day ->> 'start')::time), ("day"."day" + (sch_day ->> 'end')::time)) "range"
      , "day".day_of_week
      , sch_day -> 'channel' channel
      , (CASE WHEN (s.resource->>'minutesDuration') IS NOT NULL THEN concat((s.resource->>'minutesDuration'), ' min')::INTERVAL
              ELSE (SELECT concat((hcs.resource->>'minutesDuration'), ' min')::INTERVAL 
                    FROM healthcareservice hcs          
                    WHERE hcs.id = s.resource #>> '{healthcareService,0,id}')
         END) "interval"
    FROM select_day_of_week "day"
      , av_t_by_d
    WHERE (sch_day ->> 'parity' IS NULL OR "day".parity = sch_day ->> 'parity')
      AND "day".day_of_week = sch_day ->> 'day-of-week')
  , series_of_available_time AS (
    SELECT generate_series(lower("range"), upper("range"), "interval") "begin"
      , "range" available_time
      , day_of_week 
      , channel
      , "interval" duration
    FROM available_time)
  , base_slot AS (
    SELECT tsrange("begin", ("begin" + duration)) AS slot
      , day_of_week
      , channel
      , available_time 
    FROM series_of_available_time 
  )
  , checked_slot AS (
    SELECT slot.slot
      , slot.day_of_week
      , slot.channel 
    FROM base_slot slot 
    LEFT JOIN not_available_range not_available 
           ON not_available."range" && slot.slot 
    WHERE not_available IS NULL 
      AND slot.available_time @> slot.slot 
      AND (SELECT "interval" FROM init_interval LIMIT 1) @> slot.slot)
  SELECT row_to_json(s.*) slot
  FROM (SELECT to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "begin"
          , to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "end"
          , slot.day_of_week
          , slot.channel
          , a.id AS appointment 
        FROM checked_slot slot 
          LEFT JOIN appointment a 
                 ON (immutable_tsrange(a.resource#>>'{start}',a.resource#>>'{end}') && slot.slot 
                AND a.resource -> 'schedule' ->> 'id' = sch_id
                AND jsonb_path_query_first(a.resource, '$.appointmentType.coding ? (@.system=="http://terminology.hl7.org/CodeSystem/v2-0276").code') #>> '{}' = 'ROUTINE')) s
  WHERE s.appointment IS NULL
    AND s.channel ? channel_arg
  ORDER BY s."begin"
  LIMIT 1
) c
FROM schedulerule s 
WHERE id = sch_id
$function$
;
