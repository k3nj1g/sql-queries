SELECT *
from patient_history 
where id = '30eff82c-72e7-410d-b654-c5ee737405c8'
order by ts DESC;

select * 
from patient
where resource @> '{"identifier":[{"value":"2152240842000188"}]}'

select * 
from integrationqueue
where resource @> '{"payload":{"identifier":[{"value":"2152240842000188"}]}}'

SELECT i.*
FROM patient p 
JOIN integrationqueue i 
  ON i.resource @@ concat('payload.identifier.#.value in (', (SELECT  string_agg((value->'value')::text, ',') FROM jsonb_array_elements(p.resource->'identifier')), ')')::jsquery
where p.id = 'd5651d38-df91-4b72-a07e-ca36a363116d'
order BY i.ts  DESC;

select * 
from integrationqueue
where resource @> '{"payload":{"identifier":[{"value":"2152240842000188"}]}}'