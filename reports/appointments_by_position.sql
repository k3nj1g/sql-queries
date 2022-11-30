WITH appointments AS (
  SELECT app.cts created
    , app.resource ->> 'start' started
    , DATE_PART('day', (app.resource ->> 'start')::timestamp - app.cts) day_diff
    , app.resource #>> '{mainOrganization,id}' main_org
    , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position")') #>> '{code}' prr_position
    , app.resource--count(*)
  FROM appointment app
  JOIN practitionerrole prr ON prr.id = (jsonb_path_query_first(app.resource, '$.participant.actor ? (@.resourceType=="PractitionerRole")') #>> '{id}')
    AND prr.resource @@ 'code.#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("110", "59", "49", "122","13","54","53","100","103","101","83","85","119","120","87","28"))'::jsquery
  WHERE app.resource ->> 'start' BETWEEN '2022-01-01' AND '2022-11-30'
    AND app.resource @@ 'participant.#.actor.resourceType="PractitionerRole"'::jsquery)
, grouped AS (
  SELECT prr_position
    , round(avg(day_diff)::numeric, 0) average
    , min(day_diff) minimum
    , array_agg(jsonb_build_object('diff', day_diff, 'main-org', main_org, 'res', resource)) diffs
  FROM appointments 
  GROUP BY prr_position)
SELECT c.resource ->> 'display'
  , average
  , minimum
  , maximum.diff maximum
  , org.resource #>> '{alias,0}'
FROM grouped
JOIN concept c ON c.resource #>> '{system}' = 'urn:CodeSystem:frmr.position'
  AND c.resource #>> '{code}' = prr_position
JOIN LATERAL 
  (SELECT DISTINCT ON (v ->> 'diff')
           v ->> 'diff' diff
           , v ->> 'main-org' main_org
           , v ->> 'res' res
         FROM UNNEST(diffs) d(v) 
         ORDER BY (v ->> 'diff') DESC 
         LIMIT 5) maximum ON TRUE
JOIN organization org ON org.id = maximum.main_org;