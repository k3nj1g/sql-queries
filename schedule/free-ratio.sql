WITH RECURSIVE r AS (
  WITH slots(slot) as (
    SELECT *
    FROM jsonb_array_elements(
        schedule_slots(
          'sch-rule-50',
          '2023-02-01',
          '2023-03-02'
        )
      )
  )
  SELECT 1 as week
    , cnt
    , cnt_app
    , tsrange('tomorrow'::date, 'yesterday'::date + interval '1 week') dr
    , COALESCE(
        (SELECT jsonb_agg(slot) 
        FROM (SELECT * 
              FROM jsonb_array_elements(slots_agg) s(slot)
              WHERE slot ->> 'app' IS NULL
              ORDER BY slot -> 'begin' 
              LIMIT 3) s)
        , jsonb_build_array()) as slot_result
  FROM (
    SELECT 
    count(slot) filter (
      where ((slot->'channel') @> '["web"]')
    ) as cnt,
    count(slot) filter (
      where ((slot->'channel') @> '["web"]')
        and a is not NULL
    ) as cnt_app
    , jsonb_agg(slot || jsonb_build_object('app', a.id))
    FROM slots
    LEFT JOIN appointment a ON (
      immutable_tsrange(a.resource#>>'{start}', a.resource#>>'{end}') && immutable_tsrange(slot->>'begin', slot->>'end')
      AND a.resource->'schedule'->>'id' = 'sch-rule-50'
      AND jsonb_path_query_first(
        a.resource,
        '$.appointmentType.coding ? (@.system=="http://terminology.hl7.org/CodeSystem/v2-0276").code'
      )#>>'{}' = 'ROUTINE')
    WHERE (slot->>'begin')::date >= 'yesterday'::date 
      AND (slot->>'begin')::date < 'yesterday'::date + format('%s week', 1)::interval
    ) slots(cnt, cnt_app, slots_agg)
  UNION
  SELECT -- (cnt + count(slot) filter (where ((slot -> 'channel') @> '["web"]'))) as cnt,
    1 + week as week
    , next_cnt::integer as cnt
    , next_cnt_app::integer as cnt_app
    , tsrange('yesterday'::date + format('%s week', week)::interval, 'yesterday'::date + format('%s week', week+1)::interval) dr
    , COALESCE(slot_result || 
      CASE 
        WHEN next_cnt = 0 THEN jsonb_build_array()
        WHEN week < 2 OR next_cnt_app / next_cnt::float < 0.5 
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
      END, jsonb_build_array())as slot_result
  FROM r
    JOIN LATERAL (
      SELECT count(slot) filter (
          where ((slot->'channel') @> '["web"]')
        ) as cnt,
        count(slot) filter (
          where ((slot->'channel') @> '["web"]')
            and a.id is not NULL
        ) as cnt_app
        , jsonb_agg(slot || jsonb_build_object('app', a.id))
      FROM slots
        LEFT JOIN appointment a ON (
          immutable_tsrange(a.resource#>>'{start}', a.resource#>>'{end}') 
          && immutable_tsrange(slot->>'begin', slot->>'end')
          AND a.resource->'schedule'->>'id' = 'sch-rule-50'
          AND jsonb_path_query_first(
            a.resource,
            '$.appointmentType.coding ? (@.system=="http://terminology.hl7.org/CodeSystem/v2-0276").code'
          )#>>'{}' = 'ROUTINE'
        )
      WHERE (slot->>'begin')::date >= 'yesterday'::date + format('%s week', week)::interval 
        AND (slot->>'begin')::date < 'yesterday'::date + format('%s week', week+1)::interval
    ) slots_with_app(next_cnt, next_cnt_app, slots_agg) ON true
  WHERE jsonb_array_length(slot_result) < 3 and week <= 5
)
SELECT week, dr, slot_result
      FROM r
      ORDER BY week DESC
-- SELECT jsonb_array_elements(slot_result)
-- FROM (SELECT slot_result
--       FROM r
--       ORDER BY week DESC
--       LIMIT 1) s
;

SELECT resource
from appointment
order by resource->>'start';
--where id = 'sch-rule-50';


SELECT *
FROM jsonb_array_elements(
        schedule_slots(
          'sch-rule-50',
          '2023-02-01',
          '2023-03-02'
        )
      ) s(slot)
ORDER BY slot->>'begin' desc;

SELECT fast_slots_with_50_percent_rule('sch-rule-50', 5, '2023-02-02'::date, schedule_slots(
          'sch-rule-50',
          '2023-02-01',
          '2023-03-02'
        ));
