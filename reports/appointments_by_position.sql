DROP MATERIALIZED VIEW appointments_year;

CREATE MATERIALIZED VIEW appointments_year AS 
WITH appointments AS (
  SELECT app.cts created
    , app.resource ->> 'start' started
    , DATE_PART('day', (app.resource ->> 'start')::timestamp - app.cts) day_diff
    , app.resource #>> '{mainOrganization,id}' main_org
    , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position")') #>> '{code}' prr_position
    , prr.resource #>> '{derived,orgid}' division
    , app.resource
  FROM appointment app
  JOIN practitionerrole prr ON prr.id = (jsonb_path_query_first(app.resource, '$.participant.actor ? (@.resourceType=="PractitionerRole")') #>> '{id}')
    AND prr.resource @@ 'code.#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("110", "59", "49", "122","13","54","53","100","103","101","83","85","119","120","87","28"))'::jsquery
  WHERE app.resource ->> 'start' BETWEEN '2022-01-01' AND '2022-12-05'
    AND app.resource @@ 'participant.#.actor.resourceType="PractitionerRole"'::jsquery)
, grouped AS (
  SELECT prr_position
    , round(avg(day_diff)::numeric, 0) average
    , min(day_diff) minimum
    , max(day_diff) maximum
    , array_agg(jsonb_build_object('diff', day_diff
                                   , 'main-org', main_org
                                   , 'res', resource
                                   , 'division', division)) diffs
  FROM appointments 
  GROUP BY prr_position)
SELECT c.resource ->> 'display' position
  , average
  , minimum
  -- , maximum
  , maximum.diff maximum
  , main_org.resource #>> '{alias,0}' main_org
  , org.resource ->> 'name' org_name
  , identifier_value(org.resource, 'urn:identity:oid:Organization') org_oid
  , jsonb_path_query_first(org.resource, '$.address ? (@.type == "physical" || @.type == "postal")') #>> '{text}' address
FROM grouped
JOIN concept c ON c.resource #>> '{system}' = 'urn:CodeSystem:frmr.position'
  AND c.resource #>> '{code}' = prr_position
JOIN LATERAL 
  (WITH unnested AS (
     SELECT (v ->> 'diff')::integer diff
       , v ->> 'main-org' main_org
       , v ->> 'division' division
     FROM UNNEST(diffs) d(v))
   , grouped AS (
     SELECT main_org 
       , division
       , max(diff) diff
     FROM unnested
     GROUP BY main_org, division)    
   , distincted AS (
     SELECT DISTINCT ON (diff) *
     FROM grouped
     ORDER BY diff)
   , numbered AS (
     SELECT *
       , ROW_NUMBER() OVER (PARTITION BY main_org, division ORDER BY diff DESC) AS r
     FROM distincted)
   SELECT *
   FROM numbered
   WHERE r = 1
   ORDER BY diff DESC
   LIMIT 5) maximum ON TRUE
JOIN organization main_org ON main_org.id = maximum.main_org
JOIN organization org ON org.id = maximum.division;

select * from appointments_year;