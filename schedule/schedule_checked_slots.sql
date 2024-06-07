CREATE OR REPLACE FUNCTION public.schedule_checked_slots(sch_id text, "start" date, "end" date)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
SELECT (
  WITH global_schedule AS (
    SELECT id,
           jsonb_set(
             jsonb_set(sg.resource
                       , '{replacement}'
                       , COALESCE((SELECT jsonb_agg(replacement) AS replacement 
                                   FROM jsonb_array_elements(sg.resource -> 'replacement') replacement 
                                   WHERE CAST(replacement ->> 'date' AS timestamp) >= timezone('Europe/Moscow',current_timestamp))
                                  ,'[]'))
             ,'{notAvailable}'
             , COALESCE((SELECT jsonb_agg(not_available) AS not_available   
                         FROM jsonb_array_elements(sg.resource -> 'notAvailable') not_available 
                         WHERE COALESCE(CAST(not_available #>> '{during,end}' AS timestamp),'infinity') >= timezone('Europe/Moscow',current_timestamp))
                        ,'[]')) AS resource
    FROM scheduleruleglobal sg
    LIMIT 1)
  , not_available AS (
    SELECT jsonb_array_elements(gs.resource->'notAvailable') -> 'during' value
    FROM global_schedule gs
    UNION
    SELECT jsonb_array_elements(s.resource->'notAvailable') -> 'during' value)
  , not_available_range as (
    SELECT range_agg(tsrange(CAST(not_available.value ->> 'start' AS timestamp), CAST((CAST(not_available.value ->> 'end' AS timestamp) + INTERVAL '1 minute') AS timestamp))) AS "range" 
    FROM not_available)
  , init_interval("interval") AS (
    SELECT tsrange('-infinity', (CAST((timezone('Europe/Moscow',current_timestamp) + CAST(concat(CAST(s.resource #>> '{planningActive,quantity}' AS text), ' week') AS interval)) AS date) + '1 day'::INTERVAL)) 
           * tsrange(CAST(s.resource #>> '{planningHorizon,start}' AS timestamp), CAST(s.resource #>> '{planningHorizon,end}' AS timestamp))
           * tsrange("start","end"))
  , series_of_day AS (
    SELECT CAST(generate_series(lower("interval"), upper("interval"), interval '1 day') AS date) AS "day"
    FROM init_interval)
  , select_day_of_week AS (
    SELECT ('{mon , tue , wed , thu , fri , sat , sun}'::TEXT[])[EXTRACT(ISODOW FROM "day")] AS day_of_week
           , ('{even,odd}'::TEXT[])[MOD(EXTRACT(DAY FROM "day")::integer, 2) + 1] AS parity
           , "day"
   FROM series_of_day)
  , av_t AS (
    SELECT value v
    FROM jsonb_array_elements(s.resource->'availableTime'))
  , av_t_by_d AS (
    SELECT jsonb_build_object('start', av_t.v->'availableStartTime'
                              , 'end', av_t.v->'availableEndTime'
                              , 'channel', av_t.v->'channel'
                              , 'parity', av_t.v->'parity'
                              , 'day-of-week', dow.v) sch_day
    FROM av_t
      , jsonb_array_elements(av_t.v->'daysOfWeek') dow(v))
  , available_time AS (
    SELECT tsrange((sdow."day" + (sch_day ->> 'start')::time), (sdow."day" + (sch_day ->> 'end')::time)) "range"
      , sdow.day_of_week
      , sch_day -> 'channel' channel
    FROM select_day_of_week sdow
      , av_t_by_d
    WHERE (sch_day ->> 'parity' IS NULL 
        OR sdow.parity = sch_day ->> 'parity')
      AND sdow.day_of_week = sch_day ->> 'day-of-week')
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
    FROM series_of_available_time)
  SELECT jsonb_agg(jsonb_build_object(
          'slot', slot.slot
          , 'day_of_week', slot.day_of_week
          , 'channel', slot.channel))
  FROM base_slot slot
    , not_available_range not_available
    , init_interval
  WHERE
      slot.available_time @> slot.slot 
      AND "interval" @> slot.slot
      AND (not_available."range" IS NULL OR NOT not_available."range" && slot.slot)
) c
FROM schedulerule s
WHERE id = sch_id
$function$;

SELECT schedule_checked_slots('9a0d217d-db83-45f6-8b84-78a9327cb44a', '2024-04-01', '2024-04-14')