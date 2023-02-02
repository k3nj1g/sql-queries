drop function slots_with_50_percent_rule;

CREATE OR REPLACE FUNCTION fast_slots_with_50_percent_rule(sch_id text, planning_week int, tomorrow date, slots_init jsonb)
RETURNS jsonb
AS $$ 
  WITH RECURSIVE r AS (
    WITH slots(slot) as (
      SELECT *
      FROM jsonb_array_elements(slots_init)
    )
    SELECT 1 as week
      , COALESCE(
          (SELECT jsonb_agg(slot) 
           FROM (SELECT * 
                 FROM jsonb_array_elements(slots_agg) s(slot)
                 WHERE slot ->> 'app' IS NULL
                 ORDER BY slot -> 'begin' 
                 LIMIT 3) s)
          , jsonb_build_array()) as slot_result
    FROM (
      SELECT jsonb_agg(slot || jsonb_build_object('app', a.id))
      FROM slots
      LEFT JOIN appointment a 
        ON (
          immutable_tsrange(a.resource#>>'{start}', a.resource#>>'{end}') && immutable_tsrange(slot->>'begin', slot->>'end')
          AND a.resource->'schedule'->>'id' = sch_id
          AND jsonb_path_query_first(
            a.resource,
            '$.appointmentType.coding ? (@.system=="http://terminology.hl7.org/CodeSystem/v2-0276").code') #>>'{}' = 'ROUTINE')
      WHERE (slot->>'begin')::date < tomorrow + format('%s week', 1)::interval
      ) slots(slots_agg)
    UNION
    SELECT
      1 + week as week
      , COALESCE(slot_result || 
        CASE 
          WHEN count_all_web_slots = 0 THEN jsonb_build_array()
          WHEN week < 2 OR count_busy_web_slots / count_all_web_slots::float < 0.5 
          THEN (SELECT jsonb_agg(slot) 
                FROM (SELECT * 
                      FROM jsonb_array_elements(slots_agg) s(slot)
                      WHERE slot ->> 'app' IS NULL
                      ORDER BY slot -> 'begin' 
                      LIMIT (3 - jsonb_array_length(slot_result))) s)
          ELSE (SELECT jsonb_agg(slot) 
                FROM (SELECT * 
                      FROM jsonb_array_elements(slots_agg) s(slot)
                      WHERE slot ->> 'app' IS NULL
                        AND NOT ((slot->'channel') @> '["web"]') 
                      ORDER BY slot -> 'begin'
                      LIMIT (3 - jsonb_array_length(slot_result))) s)
        END, jsonb_build_array()) as slot_result
    FROM r
      JOIN LATERAL (
        SELECT count(slot) FILTER (WHERE ((slot->'channel') @> '["web"]'))
          , count(slot) FILTER (WHERE ((slot->'channel') @> '["web"]')
                                  AND a.id IS NOT NULL)
          , jsonb_agg(slot || jsonb_build_object('app', a.id))
        FROM slots
        LEFT JOIN appointment a ON (
            immutable_tsrange(a.resource#>>'{start}', a.resource#>>'{end}') && immutable_tsrange(slot->>'begin', slot->>'end')
            AND a.resource->'schedule'->>'id' = sch_id
            AND jsonb_path_query_first(
              a.resource,
              '$.appointmentType.coding ? (@.system=="http://terminology.hl7.org/CodeSystem/v2-0276").code') #>>'{}' = 'ROUTINE')
        WHERE (slot->>'begin')::date >= tomorrow + format('%s week', week)::interval 
          AND (slot->>'begin')::date < tomorrow + format('%s week', week+1)::interval
      ) slots_with_app(count_all_web_slots, count_busy_web_slots, slots_agg) ON true
    WHERE jsonb_array_length(slot_result) < 3 and week <= planning_week
  )
  SELECT slot_result
  FROM r
  ORDER BY week DESC
  LIMIT 1$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION fast_slots_with_50_percent_rule(sch_id text, planning_week int, slots_init jsonb)
RETURNS jsonb
AS $$ 
  SELECT fast_slots_with_50_percent_rule(sch_id, planning_week, 'tomorrow'::date, slots_init)
  $$
LANGUAGE sql;
