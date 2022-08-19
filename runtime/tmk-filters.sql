WITH sr_with_task AS
(
  SELECT sr.*,
         ((SELECT to_jsonb(t.*) AS t
  FROM task AS t
  WHERE (t.resource @@ LOGIC_REVINCLUDE(sr.resource,sr.id,'focus',' and code.coding.#(system="urn:CodeSystem:chu-task-code" and code="misStatistics")')))) AS t FROM servicerequest AS sr WHERE (sr.resource @@ 'category.#.coding.#(code="TMK" and system="urn:CodeSystem:servicerequest-category") and requester(resourceType="Organization" and id="ff0f409e-ce00-4707-9e44-d8e493cde996")'::jsquery) AND ((sr.resource #>> '{performerInfo,requestStatus}') IN ('draft','new','in-progress','suspended','expanded','completed'))
  ORDER BY cast((sr.resource ->> 'authoredOn') AS date) DESC,
           ARRAY_POSITION(ARRAY['suspended','draft','new','in-progress','expanded','completed'],(sr.resource #>> '{performerInfo,requestStatus}')) ASC,
           ARRAY_POSITION(ARRAY['asap','urgent','routine'],(sr.resource #>> '{priority}')) ASC,
           cast((sr.resource ->> 'authoredOn') AS timestamp) DESC LIMIT 100
)
SELECT *
FROM sr_with_task AS sr
WHERE (((SELECT ((SELECT (value ->> 'action') FROM jsonb_array_elements((sr.resource #> '{performerInfo,requestActionHistory}')) WHERE (value ->> 'action') IN ('payment-requested','payment-failed') ORDER BY cast((value ->> 'date') AS timestamp) DESC LIMIT 1)))) IS NULL)
AND   CASE WHEN (sr.t -> 'resource') IS NOT NULL THEN ((sr.t -> 'resource') @@ 'not status="completed"'::jsquery) ELSE TRUE END