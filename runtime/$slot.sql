WITH init_interval AS (
	SELECT (tsrange(timezone('Europe/Moscow', current_timestamp), (CAST((timezone('Europe/Moscow', current_timestamp) + '4 week'::INTERVAL) AS date) + '1 day'::INTERVAL)) 
			* tsrange('2019-12-30T00:00:00', '2020-12-30T23:59:59') * tsrange('2020-12-26', NULL)) AS "interval"),
series_of_day AS (
	SELECT
		CAST(generate_series(lower((SELECT INTERVAL FROM init_interval LIMIT 1)), upper((SELECT INTERVAL FROM init_interval LIMIT 1)), INTERVAL '1 day') AS date) AS DAY),
	select_day_of_week AS (
	SELECT
		COALESCE((SELECT replacement ->> 'dayOfWeek' FROM jsonb_array_elements('[]') replacement WHERE CAST(replacement ->> 'date' AS date) = CAST(DAY AS date) LIMIT 1), ('{mon , tue , wed , thu , fri , sat , sun}'::TEXT[])[EXTRACT(ISODOW FROM DAY)]) AS day_of_week,
		('{even,odd}'::TEXT[])[MOD(EXTRACT(DAY FROM DAY)::integer, 2) + 1] AS parity,
		DAY
	FROM
		series_of_day),
available_time AS (
	SELECT
		tsrange((day.day + CAST(available_time ->> 'start' AS time)),
		(day.day + CAST(available_time ->> 'end' AS time))) AS RANGE,
		day.day_of_week,
		available_time -> 'channel' AS channel
	FROM
		select_day_of_week DAY,
		jsonb_array_elements('[{"start":"08:00:00","end":"08:20:00","channel":["reg"],"day-of-week":"wed","parity":null}]') available_time
	WHERE
		((available_time ->> 'parity' IS NULL
		OR day.parity = available_time ->> 'parity')
		AND day.day_of_week = available_time ->> 'day-of-week')),
series_of_available_time AS (
	SELECT
		generate_series(lower(available_time.range), upper(available_time.range), duration.interval) AS BEGIN,
		available_time.range AS available_time,
		available_time.day_of_week,
		available_time.channel,
		duration.interval AS duration
	FROM
		available_time available_time,
		(
		SELECT
			'20 min'::INTERVAL AS INTERVAL) duration),
base_slot AS (
	SELECT
		tsrange(BEGIN,
		(BEGIN + duration)) AS slot,
		day_of_week,
		channel,
		available_time
	FROM
		series_of_available_time),
not_available AS (
	SELECT
		tsrange(CAST(not_available ->> 'start' AS timestamp),
		CAST(not_available ->> 'end' AS timestamp)) AS RANGE
	FROM
		jsonb_array_elements('[]') not_available),
checked_slot AS (
	SELECT
		slot.slot,
		slot.day_of_week,
		slot.channel
	FROM
		base_slot slot
	LEFT JOIN not_available not_available ON
		not_available.range && slot.slot
	WHERE
		(not_available IS NULL
		AND slot.available_time @> slot.slot
		AND (
		SELECT
			INTERVAL
		FROM
			init_interval
		LIMIT 1) @> slot.slot)),
walkin_day AS (
	SELECT
		DISTINCT ON
		(lower(slot)::date) lower(slot)::date AS DAY,
		day_of_week,
		'[]'::jsonb AS channel
	FROM
		checked_slot),
walkin_slot AS (
	SELECT
		tsrange(DAY, (DAY + '1 day'::INTERVAL)) AS slot,
		day_of_week,
		channel
	FROM
		walkin_day)
SELECT
	s.type,
	s.day_of_week,
	s.channel,
	s.begin,
	s.end,
	jsonb_agg(s.appointment) AS app
FROM
	((
	SELECT
		'ROUTINE' AS TYPE,
		to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS BEGIN,
		to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS
	END,
	slot.day_of_week,
	slot.channel,
	a.id AS appointment
FROM
	checked_slot slot
LEFT JOIN appointment a ON
	(immutable_tsrange(a.resource#>>'{start}',
	a.resource#>>'{end}') && slot.slot
	AND a.resource -> 'schedule' ->> 'id' = '9b7f23b2-0856-447d-8e90-b89723d921bc'
	AND a.resource#>>'{start}' >= '2020-11-06'
	AND a.resource->>'status' = ANY(ARRAY['pending','booked','fulfilled','arrived'])
	AND a.resource#>'{appointmentType,coding}' @@ (CAST('#(system = "http://terminology.hl7.org/CodeSystem/v2-0276" and code = ROUTINE)' AS jsquery))))
UNION ALL (
SELECT
	'WALKIN' AS TYPE,
	to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "begin",
	to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') AS "end",
	slot.day_of_week,
	slot.channel,
	a.id AS appointment
FROM walkin_slot slot
LEFT JOIN appointment a ON (CAST(a.resource #>> '{start}' AS date) = lower(slot.slot)
	AND a.resource#>>'{schedule,id}' = '9b7f23b2-0856-447d-8e90-b89723d921bc'
	AND a.resource#>>'{start}' >= '2020-11-06'
	AND a.resource->>'status' = ANY(ARRAY['pending','booked','fulfilled','arrived'])
	AND a.resource#>'{appointmentType,coding}' @@ (CAST('#(system = "http://terminology.hl7.org/CodeSystem/v2-0276" and code = WALKIN)' AS jsquery))))) s
GROUP BY s.type, s.day_of_week, s.channel, s.begin, s.end
ORDER BY s.BEGIN
		
