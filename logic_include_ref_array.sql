select *
from episodeofcare eoc
JOIN careteam ct ON ct.resource @@ concat('identifier.#("system"="', eoc.resource #>> '{team,0,identifier,system}', '" and value="', eoc.resource #>> '{team,0,identifier,value}', '")')::jsquery
where eoc.id = '44243b8a-be4e-4546-9108-2e344745c72b'

SELECT ct.resource #>> '{participant,0,period,start}', ct.resource #>> '{participant,-1,period,end}', jsonb_array_length(ct.resource -> 'participant')
FROM careteam ct 


SELECT concat('identifier.#("system"="', eoc.resource #>> '{team,0,identifier,system}', '" and value="', eoc.resource #>> '{team,0,identifier,value}', '"')
from episodeofcare eoc
where eoc.id = '44243b8a-be4e-4546-9108-2e344745c72b'
