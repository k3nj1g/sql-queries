CREATE OR REPLACE FUNCTION public.schedule_day_activity("date" date, sch_id text)
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
    SELECT daterange(CAST(not_available.value ->> 'start' AS date), CAST((CAST(not_available.value ->> 'end' AS timestamp) + INTERVAL '1 minute') AS date)) AS "range" 
    FROM not_available)
  , not_available_date AS (
    SELECT *
    FROM not_available_range
    WHERE "range" @> "date")
  , init_interval AS 
    (SELECT (tsrange (timezone('Europe/Moscow',current_timestamp), (cast((timezone ('Europe/Moscow',current_timestamp) + CAST(concat(CAST(resource #>> '{planningActive,quantity}' AS text), ' week') AS interval)) AS date) + '1 day'::interval)) * 
            tsrange (CAST(resource #>> '{planningHorizon,start}' AS timestamp), CAST(resource #>> '{planningHorizon,end}' AS timestamp))) AS "interval")
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
    SELECT t
    FROM jsonb_array_elements(resource->'availableTime') t
  )
  , av_t_w_day AS (
    SELECT jsonb_build_object('day-of-week', td, 'parity', t->'parity') days
    FROM av_t, jsonb_array_elements(t->'daysOfWeek') td
  )
  , available_time AS 
  (SELECT array_agg("day") "days"
   FROM select_day_of_week "day",
        av_t_w_day
   WHERE ((days ->> 'parity' IS NULL OR "day".parity = days ->> 'parity') AND "day".day_of_week = days ->> 'day-of-week'))
  SELECT CASE WHEN (av_t."days" @> ARRAY["date"] AND NOT EXISTS (SELECT * FROM not_available_range WHERE "range" @> "date")) THEN 'Да'
              WHEN EXISTS (SELECT * FROM not_available_range WHERE "range" @> "date") THEN 'Неактивно'
              ELSE 'Нет' END
  FROM available_time av_t
) c
FROM schedulerule s 
WHERE id = sch_id
$function$;
