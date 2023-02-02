DROP FUNCTION schedule_slots;

CREATE OR REPLACE FUNCTION public.schedule_slots(sch_id text, "start" date, "end" date)
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
             , coalesce((SELECT jsonb_agg(not_available) AS not_available   
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
    FROM select_day_of_week "day"
      , av_t_by_d
    WHERE (sch_day ->> 'parity' IS NULL OR "day".parity = sch_day ->> 'parity')
      AND "day".day_of_week = sch_day ->> 'day-of-week'
  )
  , series_of_available_time AS (
    SELECT generate_series(lower("range"), upper("range"), duration."interval") "begin"
      , "range" available_time
      , day_of_week 
      , channel
      , duration."interval" duration
    FROM available_time
      , (SELECT CASE WHEN (resource->>'minutesDuration') IS NOT NULL THEN concat((resource->>'minutesDuration'), ' min')::INTERVAL 
                     ELSE (SELECT concat((hcs.resource->>'minutesDuration'), ' min')::INTERVAL 
                           FROM healthcareservice hcs          
                           WHERE hcs.id = resource #>> '{healthcareService,0,id}')
      END "interval") duration)  
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
  SELECT jsonb_agg(row_to_json(s.*))
  FROM (SELECT to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "begin"
          , to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "end"
          , slot.day_of_week
          , slot.channel 
        FROM checked_slot slot) s
) c
FROM schedulerule s 
WHERE id = sch_id
$function$
;

CREATE OR REPLACE FUNCTION public.schedule_slots(sch_id text, "start" date, "end" date, channel_arg text)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
  SELECT jsonb_agg(slot.*)
  FROM jsonb_array_elements(schedule_slots(sch_id, "start", "end")) slot
  WHERE slot -> 'channel' ? channel_arg
$function$
;
