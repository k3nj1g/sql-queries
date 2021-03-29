select
	episodeofcare.resource, ra.id, (with risks as (
				select *
				from jsonb_array_elements(ra.resource -> 'prediction'))
			select 	value -> 'probability' ->> 'decimal'
			from risks
			where value -> 'when' -> 'Range' -> 'low' ->> 'value' = (
				select max(value -> 'when' -> 'Range' -> 'low' ->> 'value')
				from risks))
from
	episodeofcare
inner join riskassessment ra on
	ra.resource @@ logic_revinclude(episodeofcare.resource, episodeofcare.id, 'episodeOfCare') and
	ra.resource @@ 'code.coding.#.system = "urn:CodeSystem:pregnancy-risk-type"'::jsquery
where
	((episodeofcare.resource @> '{"status":"active"}'
	or episodeofcare.resource @> '{"status":"onhold"}'
	or episodeofcare.resource @> '{"status":"waitlist"}')
	and episodeofcare.resource @> '{"type":[{"coding":[{"system":"urn:CodeSystem:episodeofcare-type","code":"PregnantCard"}]}]}')
	and
	cast((with risks as (
				select *
				from jsonb_array_elements(ra.resource -> 'prediction'))
			select 	value -> 'probability' ->> 'decimal'
			from risks
			where value -> 'when' -> 'Range' -> 'low' ->> 'value' = (
				select max(value -> 'when' -> 'Range' -> 'low' ->> 'value')
				from risks)) as integer) is null
				
