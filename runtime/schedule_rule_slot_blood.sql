EXPLAIN ANALYZE 
WITH init_interval AS
(
       SELECT (tsrange(Timezone('Europe/Moscow',CURRENT_TIMESTAMP), (cast((Timezone('Europe/Moscow',CURRENT_TIMESTAMP) + '4 week'::interval) AS date) + '1 day'::interval)) * tsrange('2021-01-11T00:00:00', NULL)) AS interval), series_of_day AS
(
       SELECT cast(generate_series(lower(
              (
                     SELECT interval
                     FROM   init_interval limit 1)), upper(
                                                            (
                                                            SELECT interval
                                                            FROM   init_interval limit 1)), interval '1 day') AS date) AS day), select_day_of_week AS
(
       SELECT COALESCE(
                        (
                        SELECT replacement ->> 'dayOfWeek'
                        FROM   jsonb_array_elements('[]') replacement
                        WHERE  cast(replacement ->> 'date' AS date) = cast(day AS date) limit 1), ('{mon , tue , wed , thu , fri , sat , sun}'::text[])[EXTRACT(ISODOW FROM day)]) AS day_of_week,
              ('{even,odd}'::text[])[MOD(EXTRACT(DAY FROM day)::integer, 2) + 1]                                                                                                   AS parity,
              day
       FROM   series_of_day), available_time AS
(
       SELECT tsrange((day.day + cast(available_time ->> 'start' AS time)), (day.day + cast(available_time ->> 'end' AS time))) AS range,
              day.day_of_week,
              available_time -> 'channel' AS channel
       FROM   select_day_of_week day,
              jsonb_array_elements('[{"start":"07:04:00","end":"09:45:00","channel":["reg","kc-mo"],"day-of-week":"mon","parity":null},{"start":"07:04:00","end":"09:45:00","channel":["reg","kc-mo"],"day-of-week":"tue","parity":null},{"start":"07:04:00","end":"09:45:00","channel":["reg","kc-mo"],"day-of-week":"wed","parity":null},{"start":"07:04:00","end":"09:45:00","channel":["reg","kc-mo"],"day-of-week":"fri","parity":null},{"start":"07:34:00","end":"09:00:00","channel":["reg","kc-mo"],"day-of-week":"sat","parity":null},{"start":"11:00:00","end":"13:30:00","channel":["reg","kc-mo"],"day-of-week":"mon","parity":null},{"start":"11:00:00","end":"13:30:00","channel":["reg","kc-mo"],"day-of-week":"tue","parity":null},{"start":"11:00:00","end":"13:30:00","channel":["reg","kc-mo"],"day-of-week":"wed","parity":null},{"start":"11:00:00","end":"13:30:00","channel":["reg","kc-mo"],"day-of-week":"thu","parity":null},{"start":"11:00:00","end":"13:30:00","channel":["reg","kc-mo"],"day-of-week":"fri","parity":null},{"start":"15:00:00","end":"19:00:00","channel":["reg","kc-mo"],"day-of-week":"mon","parity":null},{"start":"15:00:00","end":"19:00:00","channel":["reg","kc-mo"],"day-of-week":"tue","parity":null},{"start":"15:00:00","end":"19:00:00","channel":["reg","kc-mo"],"day-of-week":"wed","parity":null},{"start":"15:00:00","end":"19:00:00","channel":["reg","kc-mo"],"day-of-week":"thu","parity":null},{"start":"15:00:00","end":"19:00:00","channel":["reg","kc-mo"],"day-of-week":"fri","parity":null},{"start":"07:04:00","end":"09:45:00","channel":["reg","kc-mo"],"day-of-week":"thu","parity":null}]') available_time
       WHERE  ((
                            available_time ->> 'parity' IS NULL
                     OR     day.parity = available_time ->> 'parity')
              AND    day.day_of_week = available_time ->> 'day-of-week')), series_of_available_time AS
(
       SELECT generate_series(lower(available_time.range), upper(available_time.range), duration.interval) AS BEGIN,
              available_time.range                                                                         AS available_time,
              available_time.day_of_week,
              available_time.channel,
              duration.interval AS duration
       FROM   available_time available_time,
              (
                     SELECT '4 min'::interval AS interval) duration), base_slot AS
(
       SELECT tsrange(BEGIN, (BEGIN + duration)) AS slot,
              day_of_week,
              channel,
              available_time
       FROM   series_of_available_time), not_available AS
(
       SELECT tsrange(cast(not_available ->> 'start' AS timestamp), cast(not_available ->> 'end' AS timestamp)) AS range
       FROM   jsonb_array_elements('[{"end":"2020-11-03T20:00:00","start":"2020-11-03T18:00:00"},{"end":"2021-01-10T23:59:00","start":"2020-12-31T00:00:00"},{"end":"2021-02-23T23:59:00","start":"2021-02-22T00:00:00"},{"end":"2021-05-01T19:00:00","start":"2021-04-30T15:00:00"},{"end":"2021-06-12T23:59:00","start":"2021-06-11T15:00:00"},{"end":"2021-06-14T23:59:00","start":"2021-06-14T00:00:00"},{"end":"2021-06-23T19:00:00","start":"2021-06-23T15:00:00"},{"end":"2021-06-24T23:59:00","start":"2021-06-24T00:00:00"},{"end":"2021-11-04T23:59:00","start":"2021-11-03T15:00:00"},{"end":"2021-11-05T19:00:00","start":"2021-11-05T11:00:00"},{"end":"2022-01-09T23:59:00","start":"2021-12-30T15:00:00"},{"end":"2022-02-22T19:00:00","start":"2022-02-22T17:00:00"},{"end":"2022-03-07T07:28:00","start":"2022-03-07T07:04:00"},{"end":"2022-03-08T23:59:00","start":"2022-03-07T09:04:00"}]') not_available), checked_slot AS
(
          SELECT    slot.slot,
                    slot.day_of_week,
                    slot.channel
          FROM      base_slot slot
          LEFT JOIN not_available not_available
          ON        not_available.range && slot.slot
          WHERE     (
                              not_available IS NULL
                    AND       slot.available_time @> slot.slot
                    AND
                              (
                                     SELECT interval
                                     FROM   init_interval limit 1) @> slot.slot))
SELECT   s.type,
         s.day_of_week,
         s.channel,
         s.BEGIN,
         s.END,
         jsonb_agg(s.appointment) AS app
FROM     (
         (
                   SELECT    'ROUTINE'                                              AS type,
                             to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS BEGIN,
                             to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS
         END,
         slot.day_of_week,
         slot.channel,
         a.id AS appointment
FROM      checked_slot slot
LEFT JOIN appointment a
ON        (
                    immutable_tsrange(a.resource#>>'{start}',a.resource#>>'{end}') && slot.slot
          AND       a.resource -> 'schedule' ->> 'id' = '7dc6a9c9-ecc4-4f2e-81ad-4c82e4aaac29'
          AND       a.resource#>>'{start}' >= '2022-03-10'
          AND       a.resource->>'status' = ANY(array['pending', 'booked', 'fulfilled', 'arrived', 'noshow'])
          AND       a.resource#>'{appointmentType,coding}' @@ (cast('#(system = "http://terminology.hl7.org/CodeSystem/v2-0276" and code = ROUTINE)' AS jsquery))))) s
GROUP BY s.type,
         s.day_of_week,
         s.channel,
         s.BEGIN,
         s.END
ORDER BY s.BEGIN