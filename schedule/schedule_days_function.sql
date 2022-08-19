CREATE FUNCTION schedule_days(sch_id text) RETURNS jsonb AS $$
    SELECT (
  WITH init_interval AS 
    (SELECT (tsrange (timezone('Europe/Moscow',current_timestamp), (cast((timezone ('Europe/Moscow',current_timestamp) + '2 week'::interval) AS date) + '1 day'::interval)) * 
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
    SELECT jsonb_build_object('start', t->>'availableStartTime', 'end', t->>'availableEndTime', 'channel', t->'channel', 'day-of-week', td, 'parity', t->'parity') days
    FROM av_t, jsonb_array_elements(t->'daysOfWeek') td
  )
  , available_time AS 
  (SELECT jsonb_build_object('day', "day", 'channel', days->'channel') "day"
   FROM select_day_of_week "day",
        av_t_w_day
   WHERE ((days ->> 'parity' IS NULL OR "day".parity = days ->> 'parity') AND "day".day_of_week = days ->> 'day-of-week'))
  SELECT jsonb_agg("day") FROM available_time
)        c
FROM schedulerule s 
WHERE id = sch_id
$$ LANGUAGE SQL;