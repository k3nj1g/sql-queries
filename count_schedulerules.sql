WITH sch_2019 AS (
  SELECT count(sch.*) cnt
    FROM schedulerule sch
   WHERE sch.resource @@ 'availableTime.#.channel.# = "web" and actor.#.resourceType = "PractitionerRole"'::jsquery
         AND tsrange((sch.resource #>> '{planningHorizon,start}')::timestamp, (sch.resource #>> '{planningHorizon,end}')::timestamp) && tsrange('2019-01-01', '2020-01-01'))
, sch_2020 AS (
  SELECT count(sch.*) cnt
    FROM schedulerule sch
   WHERE sch.resource @@ 'availableTime.#.channel.# = "web" and actor.#.resourceType = "PractitionerRole"'::jsquery
         AND tsrange((sch.resource #>> '{planningHorizon,start}')::timestamp, (sch.resource #>> '{planningHorizon,end}')::timestamp) && tsrange('2020-01-01', '2021-01-01'))
, sch_2021 AS (
  SELECT count(sch.*) cnt
    FROM schedulerule sch
   WHERE sch.resource @@ 'availableTime.#.channel.# = "web" and actor.#.resourceType = "PractitionerRole"'::jsquery
         AND tsrange((sch.resource #>> '{planningHorizon,start}')::timestamp, (sch.resource #>> '{planningHorizon,end}')::timestamp) && tsrange('2021-01-01', current_timestamp::timestamp))         
SELECT sch_2019.cnt cnt_2019, sch_2020.cnt cnt_2020, sch_2021.cnt cnt_2021
FROM sch_2019, sch_2020, sch_2021
            