SELECT s.resource,
    jsonb_set(
        s.resource,
        '{availableTime}',
        (
            SELECT jsonb_agg(
                    jsonb_set(
                        x,
                        '{channel}',
                        jsonb_remove_array_element(x->'channel', '"tmk-online"')
                    )
                )
            FROM jsonb_array_elements(s.resource->'availableTime') av_t(x)
        )
    )
FROM schedulerule s
    JOIN healthcareservice h ON h.id = s.resource#>>'{healthcareService,0,id}'
    AND NOT h.resource#>>'{type,0,coding,0,code}' = '2001'
WHERE s.resource @@ 'availableTime.#.channel.# = "tmk-online"'::jsquery;

-- WITH to_update AS (
--     SELECT s.id,
--       jsonb_set(
--           s.resource,
--           '{availableTime}',
--           (
--               SELECT jsonb_agg(
--                       jsonb_set(
--                           x,
--                           '{channel}',
--                           jsonb_remove_array_element(x->'channel', '"tmk-online"')
--                       )
--                   )
--               FROM jsonb_array_elements(s.resource->'availableTime') av_t(x)
--           )
--       ) resource
--   FROM schedulerule s
--       JOIN healthcareservice h ON h.id = s.resource#>>'{healthcareService,0,id}'
--       AND NOT h.resource#>>'{type,0,coding,0,code}' = '2001'
--   WHERE s.resource @@ 'availableTime.#.channel.# = "tmk-online"'::jsquery)
-- UPDATE schedulerule
-- SET resource = to_update.resource
-- FROM to_update
-- WHERE to_update.id = schedulerule.id
-- RETURNING *;

SELECT *
FROM schedulerule
WHERE id = '3d8b6a57-8941-4c3e-a7bf-8c4731dc7d00';

CREATE OR REPLACE FUNCTION jsonb_remove_array_element(arr jsonb, element jsonb) 
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
  DECLARE _idx integer;
  DECLARE _result jsonb;
  BEGIN
    _idx := (SELECT ordinality - 1 FROM jsonb_array_elements(arr) WITH ordinality WHERE value = element);
    IF _idx IS NOT NULL 
    THEN 
      _result := arr - _idx;
    ELSE
      _result := arr;
    END IF;
    RETURN _result;
  END;
$$;
