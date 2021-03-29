with init_interval as 
		(select (tsrange(timezone('Europe/Moscow', current_timestamp), (cast((timezone('Europe/Moscow', current_timestamp) + '14 week'::interval) as date) + '1 day'::interval)) * tsrange('2020-03-01', null) * tsrange('2020-06-26', null)) as interval), 
	series_of_day as 
		(select cast(generate_series(lower((select interval from init_interval limit 1)), upper((select interval from init_interval limit 1)), interval '1 day') as date) as day), 
	select_day_of_week as 
		(select coalesce((select replacement ->> 'dayOfWeek' from jsonb_array_elements('[]') replacement where cast(replacement ->> 'date' as date) = cast(day as date) limit 1), ('{mon , tue , wed , thu , fri , sat , sun}'::text[])[extract(ISODOW from day)]) as day_of_week, ('{even,odd}'::text[])[mod(extract(day from day)::integer, 2) + 1] as parity, day from series_of_day), 
	available_time as 
		(select tsrange((day.day + cast(available_time ->> 'start' as time)), (day.day + cast(available_time ->> 'end' as time))) as range, day.day_of_week, available_time -> 'channel' as channel
		from select_day_of_week day, jsonb_array_elements('[{"start":"08:00:00", "end":"12:00:00", "channel":["reg"], "day-of-week":"mon", "parity":null}, {"start":"08:00:00", "end":"12:00:00", "channel":["reg"], "day-of-week":"tue", "parity":null}, {"start":"08:00:00", "end":"12:00:00", "channel":["reg"], "day-of-week":"wed", "parity":null}, {"start":"08:00:00", "end":"12:00:00", "channel":["reg"], "day-of-week":"thu", "parity":null}, {"start":"08:00:00", "end":"12:00:00", "channel":["reg"], "day-of-week":"fri", "parity":null}, {"start":"08:00:00", "end":"12:00:00", "channel":["reg"], "day-of-week":"sat", "parity":null}]') available_time
		where ((available_time ->> 'parity' is null
			or day.parity = available_time ->> 'parity')
			and day.day_of_week = available_time ->> 'day-of-week')), 
	series_of_available_time as 
		(select generate_series(lower(available_time.range), upper(available_time.range), duration.interval) as begin, available_time.range as available_time, available_time.day_of_week, available_time.channel, duration.interval as duration
		from available_time available_time, (select '5 min'::interval as interval) duration), 
	base_slot as 
		(select tsrange(begin, (begin + duration)) as slot, day_of_week, channel, available_time
		from series_of_available_time), 
	not_available as 
		(select tsrange(cast(not_available ->> 'start' as timestamp), cast(not_available ->> 'end' as timestamp)) as range
		from jsonb_array_elements('[{"end":"2020-06-19T23:59:00", "start":"2020-06-19T00:00:00" }]') not_available), 
	checked_slot as 
		(select slot.slot, slot.day_of_week, slot.channel
		from base_slot slot
			left join not_available not_available on not_available.range && slot.slot
		where (not_available is null
			and slot.available_time @> slot.slot
			and (select interval
				from init_interval
				limit 1) @> slot.slot)), 
	walkin_day as 
		(select distinct on (lower(slot)::date) lower(slot)::date as day, day_of_week, '[]'::jsonb as channel
		from checked_slot), 
	walkin_slot as 
		(select tsrange(day, (day + '1 day'::interval)) as slot, day_of_week, channel
		from walkin_day) 
select s.type, s.day_of_week, s.channel, s.begin, s.end, jsonb_agg(s.appointment) as app
from ((select 'ROUTINE' as type, to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') as begin, to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') as end, slot.day_of_week, slot.channel, a.id as appointment
from checked_slot slot
	left join appointment a on (immutable_tsrange(a.resource#>>'{start}', a.resource#>>'{end}') && slot.slot
		and a.resource #>> '{schedule, id}' = '7c13f3c8-4f9d-44c3-9c0d-664a344ddffc'
		and a.resource#>>'{start}' >= '2020-06-25'
		and a.resource->>'status' = any(array['pending', 'booked', 'fulfilled'])
		and a.resource#>'{appointmentType,coding}' @@ (cast('#(system = "http://terminology.hl7.org/CodeSystem/v2-0276" and code = ROUTINE)' as jsquery))))
union all 
	(select 'WALKIN' as type, to_char(lower(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') as begin, to_char(upper(slot.slot), 'YYYY-MM-DD"T"HH24:MI:SS') as end, slot.day_of_week, slot.channel, a.id as appointment
	from walkin_slot slot
		left join appointment a on (cast(a.resource #>> '{start}' as date) = lower(slot.slot)
			and a.resource #>> '{schedule, id}' = '7c13f3c8-4f9d-44c3-9c0d-664a344ddffc'
			and a.resource#>>'{start}' >= '2020-06-25'
			and a.resource->>'status' = any(array['pending', 'booked', 'fulfilled'])
			and a.resource#>'{appointmentType,coding}' @@ (cast('#(system = "http://terminology.hl7.org/CodeSystem/v2-0276" and code = WALKIN)' as jsquery))))) s
group by s.type, s.day_of_week, s.channel, s.begin, s.end
order by s.begin