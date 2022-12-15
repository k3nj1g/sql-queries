CREATE MATERIALIZED VIEW app_count AS 
WITH filtered AS (
  SELECT s.id sch_id
  FROM schedulerule s
  WHERE s.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
    AND immutable_tsrange(s.resource #>> '{planningHorizon,start}', COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && tsrange('2022-12-05', ('2022-12-05'::date + INTERVAL '4 weeks')::timestamp))
, slots AS (
  SELECT sch_id, jsonb_array_elements((schedule_slots(sch_id, '2022-12-05', ('2022-12-05'::date + INTERVAL '4 weeks')::date))) slot
  FROM filtered
)
SELECT count(*) "all"
  , count(*) FILTER (WHERE jsonb_array_length(slot->'channel') > 1) "many"
  , count(*) FILTER (WHERE jsonb_array_length(slot->'channel') > 1 AND (slot->'channel') ? 'web') "with_web"
FROM slots