WITH schedules AS (
  SELECT sch.resource #>> '{mainOrganization,id}' mo
    , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' frmr_position
    , array_agg(sch.id) sch_ids
  FROM practitionerrole prr
  JOIN schedulerule sch ON sch.resource @@ logic_revinclude(prr.resource, prr.id, 'actor.#')
   AND immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && tsrange(current_timestamp::timestamp, 'infinity')
  JOIN healthcareservice hcs
    ON hcs.id = sch.resource #>> '{healthcareService,0,id}'
   AND NOT jsonb_path_query_first(hcs.resource, '$.type.coding ? (@.system=="urn:CodeSystem:service").code') #>> '{}' = '153'
  WHERE prr.resource @@ 'code.#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("54", "53", "110", "49", "13"))'::jsquery
  GROUP BY mo, frmr_position)
, pre AS (
  SELECT o.resource #>> '{alias,0}' "МО"
    , c.resource->>'display' "Специальность"
    , (SELECT ((schedule_range_free_slot(id, current_date, (current_date + INTERVAL '4 weeks')::date)) ->> 'begin')::date AS "begin"
       FROM unnest(sch_ids) ids(id)
       ORDER BY "begin"
       LIMIT 1) - current_date "Минимальный срок ожидания"
  FROM schedules
  JOIN organization o
    ON o.id = mo
  JOIN concept c
    ON (c.resource#>>'{system}') = 'urn:CodeSystem:frmr.position'
   AND (c.resource#>>'{code}') = frmr_position)
SELECT *
FROM pre
ORDER BY "МО", "Специальность";


select schedule_range_free_slot('69082f7e-1d3f-49c0-8f17-bfa83363e067', current_date, (current_date + INTERVAL '4 weeks')::date);