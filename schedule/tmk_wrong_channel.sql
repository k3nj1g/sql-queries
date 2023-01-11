WITH schedules AS
(
  SELECT DISTINCT ON (sch.id)
         sch.id
         , sch.resource #>> '{mainOrganization,display}'
         , jsonb_array_elements((sch.resource -> 'availableTime')) -> 'channel' chan
  FROM schedulerule sch
    JOIN healthcareservice hcs
      ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
     AND hcs.resource @@ 'type.#.coding.#.code="2001"'::jsquery
  WHERE immutable_tsrange(sch.resource #>> '{planningHorizon,start}',coalesce((sch.resource #>> '{planningHorizon,end}'::text[]),'infinity'::text)) && tsrange(current_timestamp::timestamp,'infinity')
)
SELECT *
FROM schedules
WHERE NOT chan @@ '#:($=tmk-online)'::jsquery