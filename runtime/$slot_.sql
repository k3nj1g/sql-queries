--EXPLAIN ANALYZE 
WITH init_interval AS 
  (SELECT (tsrange 
            (timezone('Europe/Moscow',current_timestamp), (cast((timezone ('Europe/Moscow',current_timestamp) + '4 week'::interval) AS date) + '1 day'::interval)) * 
            tsrange ('2020-03-01',NULL) * tsrange ('2022-03-30',NULL)) AS "interval")
, series_of_day AS 
  (SELECT cast(generate_series
                (lower((SELECT "interval" FROM init_interval LIMIT 1))
                ,upper((SELECT "interval" FROM init_interval LIMIT 1))
                , interval '1 day') AS date) AS "day")
, select_day_of_week AS 
  (SELECT coalesce(
            (SELECT replacement ->> 'dayOfWeek' 
             FROM jsonb_array_elements('[]') replacement 
             WHERE cast(replacement ->> 'date' AS date) = cast("day" AS date) 
             LIMIT 1)
            , ('{mon , tue , wed , thu , fri , sat , sun}'::text[])[extract(ISODOW FROM DAY)]) AS day_of_week,
          ('{even,odd}'::text[])[mod(extract(DAY FROM DAY)::integer,2) + 1] AS parity,
         "day"
   FROM series_of_day)
, available_time AS 
  (SELECT tsrange(("day"."day" + cast(available_time ->> 'start' AS time)),("day"."day" + cast(available_time ->> 'end' AS time))) AS "range",
          "day".day_of_week,
          available_time -> 'channel' AS channel
   FROM select_day_of_week "day",
        jsonb_array_elements('[{"start":"07:30:00","end":"08:30:00","channel":["reg","kc","freg"],"day-of-week":"tue","parity":null},{"start":"07:30:00","end":"08:30:00","channel":["reg","kc","freg"],"day-of-week":"wed","parity":null},{"start":"07:30:00","end":"08:30:00","channel":["reg","kc","freg"],"day-of-week":"thu","parity":null}]') available_time
   WHERE ((available_time ->> 'parity' IS NULL OR "day".parity = available_time ->> 'parity') AND "day".day_of_week = available_time ->> 'day-of-week'))
, series_of_available_time AS 
  (SELECT generate_series(lower(available_time."range")
                         ,upper(available_time."range")
                         ,duration."interval") AS "begin",
                         available_time."range" AS available_time,
                         available_time.day_of_week,
                         available_time.channel,
                         duration."interval" AS duration
   FROM available_time available_time,
        (SELECT '10 min'::interval AS "interval") duration)
, base_slot AS 
  (SELECT tsrange("begin", ("begin"+ duration)) AS slot,
          day_of_week,
          channel,
          available_time
   FROM series_of_available_time)
, not_available AS 
  (SELECT tsrange(cast(not_available ->> 'start' AS timestamp)
                 ,cast(not_available ->> 'end' AS timestamp)) AS "range"
   FROM jsonb_array_elements('[]') not_available)
, checked_slot AS 
  (SELECT slot.slot,
          slot.day_of_week,
          slot.channel
   FROM base_slot slot
   LEFT JOIN not_available not_available ON not_available."range" && slot.slot
   WHERE (not_available IS NULL 
     AND slot.available_time @> slot.slot 
     AND (SELECT "interval" FROM init_interval LIMIT 1) @> slot.slot))
SELECT s."type",
       s.day_of_week,
       s.channel,
       s."begin",
       s."end",
       jsonb_agg(s.appointment) AS app,
       'f569a0a9-46e5-4960-be0a-2891736afb26' AS schedule_id
FROM ((SELECT 'ROUTINE' AS "type",
              to_char(lower(slot.slot),'YYYY-MM-DD"T"HH24:MI:SS') AS "begin",
              to_char(upper(slot.slot),'YYYY-MM-DD"T"HH24:MI:SS') AS "end",
              slot.day_of_week,
              slot.channel,
              a.id AS appointment
       FROM checked_slot slot
       LEFT JOIN appointment a
         ON (immutable_tsrange (a.resource #>> '{start}',a.resource #>> '{end}') && slot.slot
           AND a.resource -> 'schedule' ->> 'id' = 'f569a0a9-46e5-4960-be0a-2891736afb26'
           AND a.resource #>> '{start}' >= '2022-03-25'
           AND a.resource ->> 'status' = ANY (ARRAY['pending','booked','fulfilled','arrived','noshow'])
           AND a.resource #> '{appointmentType,coding}' @@ (cast ('#(system = "http://terminology.hl7.org/CodeSystem/v2-0276" and code = ROUTINE)' AS jsquery)))
       WHERE a.id IS NULL
       LIMIT 3)) s
GROUP BY s."type",
         s.day_of_week,
         s.channel,
         s."begin",
         s."end"
ORDER BY s."begin"