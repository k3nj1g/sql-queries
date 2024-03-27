WITH tasks AS
(
  SELECT t.*
  FROM task AS t
  WHERE (t.resource @@ 'code.coding.#(system="urn:CodeSystem:chu-task-code" and code="waitingList") and owner(resourceType="Organization" and id="1150e915-f639-4234-a795-1767e0a0be5f") and businessStatus.coding.#(system="urn:CodeSystem:chu-task-business-status" and code in ("in-progress","expired","run-out"))'::jsquery)
  ORDER BY t.resource ->> 'authoredOn' ASC
  LIMIT 50
  OFFSET 0
)
SELECT t.id, t.ts, t.cts, t.resource_type, t.status
     , JSONB_SET_LAX(resource,'{slot}'
                       ,(SELECT TO_JSONB(TRUE) 
                         FROM schedulerule AS sch 
                         WHERE (sch.resource @@ CAST(CONCAT ('actor.#.resourceType="PractitionerRole" and mainOrganization.id="1150e915-f639-4234-a795-1767e0a0be5f" and ','healthcareService.#(id=',(t.resource #> '{focus,id}'),')') AS jsquery)) 
                           AND (IMMUTABLE_TSRANGE ((sch.resource #>> '{planningHorizon,start}'),COALESCE((sch.resource #>> '{planningHorizon,end}'),'infinity')) @> CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) 
                           AND schedule_has_free_slot(sch.id,'{doctor,reg,kc-mo}') 
                         LIMIT 1)) -> 'slot' AS resource
      , ROW_NUMBER() OVER () AS row_num
FROM tasks t;

WITH tasks AS
(
  SELECT t.*
  FROM task AS t
  WHERE (t.resource @@ 'code.coding.#(system="urn:CodeSystem:chu-task-code" and code="waitingList") and owner(resourceType="Organization" and id="1150e915-f639-4234-a795-1767e0a0be5f") and businessStatus.coding.#(system="urn:CodeSystem:chu-task-business-status" and code in ("in-progress","expired","run-out"))'::jsquery)
  ORDER BY JSONB_EXTRACT_PATH_TEXT(t.resource,'authoredOn') ASC LIMIT '25' OFFSET 0
)
SELECT t.id,
       t.ts,
       t.cts,
       t.txid,
       t.status,
       t.resource_type,
       JSONB_SET_LAX(t.resource,'{slot}',(SELECT TO_JSONB(TRUE) FROM schedulerule AS sch WHERE (sch.resource @@ CAST(CONCAT ('actor.#.resourceType="PractitionerRole" and mainOrganization.id="1150e915-f639-4234-a795-1767e0a0be5f" and ','healthcareService.#(id=',(t.resource #>> '{focus,id}'),')') AS jsquery)) AND (IMMUTABLE_TSRANGE ((sch.resource #>> '{planningHorizon,start}'),COALESCE((sch.resource #>> '{planningHorizon,end}'),'infinity')) @> CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) AND SCHEDULE_HAS_FREE_SLOT (sch.id,'{doctor,reg,kc-mo}') LIMIT 1)) AS resource,
       ROW_NUMBER() OVER () AS row_num
FROM tasks AS t

WITH tasks AS
(
  SELECT t.*
  FROM task AS t
  WHERE (t.resource @@ 'code.coding.#(system="urn:CodeSystem:chu-task-code" and code="waitingList") and owner(resourceType="Organization" and id="1150e915-f639-4234-a795-1767e0a0be5f") and businessStatus.coding.#(system="urn:CodeSystem:chu-task-business-status" and code in ("in-progress","expired","run-out"))'::jsquery)
    AND EXISTS (SELECT 1
                FROM schedulerule AS sch 
                WHERE (sch.resource @@ CAST(CONCAT ('actor.#.resourceType="PractitionerRole" and mainOrganization.id="1150e915-f639-4234-a795-1767e0a0be5f" and ','healthcareService.#(id=',(t.resource #> '{focus,id}'),')') AS jsquery)) 
                  AND (IMMUTABLE_TSRANGE ((sch.resource #>> '{planningHorizon,start}'),COALESCE((sch.resource #>> '{planningHorizon,end}'),'infinity')) @> CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) 
                  AND schedule_has_free_slot(sch.id,'{doctor,reg,kc-mo}') 
                 LIMIT 1)
  ORDER BY t.resource ->> 'authoredOn' ASC
  LIMIT 50
  OFFSET 0
)
SELECT t.id, t.ts, t.cts, t.resource_type, t.status
     , JSONB_SET_LAX(resource,'{slot}', to_jsonb(TRUE)) resource
      , ROW_NUMBER() OVER () AS row_num
FROM tasks t;