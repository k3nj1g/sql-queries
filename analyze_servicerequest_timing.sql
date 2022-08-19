--EXPLAIN ANALYZE
WITH sr_s AS (
    SELECT 
        sr.resource #>> '{managingOrganization,display}' org_name,
        (SELECT (actions ->> 'date')::timestamp
         FROM jsonb_array_elements(sr.resource #> '{performerInfo,requestActionHistory}') actions
         WHERE actions ->> 'action' = 'draft')
         - (sr.resource ->> 'authoredOn')::timestamp AS draft_authored,
        (s.resource ->> 'receivedTime')::timestamp - 
        (SELECT (actions ->> 'date')::timestamp
         FROM jsonb_array_elements(sr.resource #> '{performerInfo,requestActionHistory}') actions
         WHERE actions ->> 'action' = 'draft') received_draft
        , (s.resource ->> 'receivedTime')::timestamp received
        ,null::timestamp AS effective
--        ,(SELECT (actions ->> 'date')::timestamp
--         FROM jsonb_array_elements(sr.resource #> '{performerInfo,requestActionHistory}') actions
--         WHERE actions ->> 'action' = 'draft')
    FROM servicerequest sr
    --  JOIN diagnosticreport dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'basedOn.#')
    --  ON knife_extract_text(dr.resource, '[["basedOn", {"resourceType": "ServiceRequest"}, "id"]]') && ARRAY[sr.id]
    --     OR knife_extract_text(dr.resource, '[["basedOn", {"type": "ServiceRequest"}, "identifier", "value"]]') <@ knife_extract_text(sr.resource, '[["identifier", "value"]]')
      JOIN specimen s ON s.resource @@ logic_include (sr.resource,'specimen')
    WHERE sr.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") 
                          and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")'::jsquery
        AND sr.resource ->> 'authoredOn' BETWEEN '2021-11-22' AND '2021-11-29'
),
sr_dr AS (
    SELECT 
        sr.resource #>> '{managingOrganization,display}' org_name,
        NULL::interval AS draft_authored,
        NULL::INTERVAL AS received_draft,
        NULL::timestamp AS received,
        (dr.resource #>> '{effective,dateTime}')::timestamp AS effective
    FROM servicerequest sr
      JOIN diagnosticreport dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'basedOn.#')
    WHERE sr.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") 
                          and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")'::jsquery
        AND sr.resource ->> 'authoredOn' BETWEEN '2021-11-22' AND '2021-11-29'
),
unioned AS 
(SELECT *
FROM sr_s
UNION 
SELECT *
FROM sr_dr)
SELECT 
    unioned.org_name
    , avg(unioned.draft_authored) column2
    , avg(unioned.received_draft) column3
    , avg(unioned.received - unioned.effective) column4
FROM unioned
GROUP BY 1
ORDER BY 1

WITH joined AS (
  SELECT sr.resource #>> '{managingOrganization,display}' org_name,
        sr.id sr_id,
        (SELECT (actions ->> 'date')::timestamp
         FROM jsonb_array_elements(sr.resource #> '{performerInfo,requestActionHistory}') actions
         WHERE actions ->> 'action' = 'draft') AS draft,
        (SELECT (actions ->> 'date')::timestamp
         FROM jsonb_array_elements(sr.resource #> '{performerInfo,requestActionHistory}') actions
         WHERE actions ->> 'action' = 'draft')
         - (sr.resource ->> 'authoredOn')::timestamp AS draft_authored,
        (s.resource ->> 'receivedTime')::timestamp - 
        (SELECT (actions ->> 'date')::timestamp
         FROM jsonb_array_elements(sr.resource #> '{performerInfo,requestActionHistory}') actions
         WHERE actions ->> 'action' = 'draft') received_draft
        , (s.resource ->> 'receivedTime')::timestamp received,
        (dr.resource #>> '{effective,dateTime}')::timestamp AS effective
  FROM servicerequest sr
    JOIN diagnosticreport dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'basedOn.#')
    JOIN specimen s ON s.resource @@ logic_include (sr.resource,'specimen')
  WHERE sr.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") 
                          and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")'::jsquery
    AND sr.resource ->> 'authoredOn' BETWEEN '2021-11-22' AND '2021-11-29')
SELECT joined.org_name
  , avg(joined.draft_authored) column2
  , avg(joined.received_draft) column3
  , avg(joined.effective - joined.received) column4
  , count(DISTINCT joined.sr_id) FILTER (WHERE joined.draft IS NULL) column5
  , count(DISTINCT joined.sr_id) FILTER (WHERE joined.received IS NULL) column6
  , count(DISTINCT joined.sr_id) all_sr
FROM joined
GROUP BY 1
ORDER BY 1    