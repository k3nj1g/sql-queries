SELECT *
FROM patientflag f 
WHERE resource::text ILIKE '%V01%'
LIMIT 10

SELECT count(*)
FROM episodeofcare e 
JOIN patient p ON p.resource @@ LOGIC_INCLUDE(e.resource, 'patient') OR (p.id = ANY(ARRAY((SELECT (JSONB_PATH_QUERY(e.resource, '$.patient.id') #>> '{}')))))
JOIN LATERAL (
    SELECT *
    FROM patientflag pf
    WHERE pf.resource #>> '{subject,id}' = p.id
        AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements(pf.resource -> 'flag') flag_item
            WHERE immutable_tsrange(e.resource #>> '{period,start}', e.resource #>> '{period,end}') && immutable_tsrange(flag_item #>> '{period,start}', flag_item #>> '{period,end}')
--                AND jsonb_path_query_first(flag_item, '$.code.coding ? (@.system == "urn:CodeSystem:r21.tag")') ->> 'code' LIKE 'V01.19%'
        )
) patient_flag ON true
WHERE e.resource @@ 'type.#.coding.#.code = "PregnantCard"'::jsquery

--EXPLAIN ANALYSE 
SELECT count(*)
FROM episodeofcare e 
JOIN patient p ON p.resource @@ LOGIC_INCLUDE(e.resource, 'patient') OR (p.id = ANY(ARRAY((SELECT (JSONB_PATH_QUERY(e.resource, '$.patient.id') #>> '{}')))))
JOIN flag f ON f.resource #>> '{subject,id}' = p.id 
WHERE e.resource @@ 'type.#.coding.#.code = "PregnantCard"'::jsquery
    AND immutable_tsrange_with_ts((e.resource #>> '{period,start}'), (e.resource #>> '{period,end}')) @> '2021-01-01'::timestamp
    AND e.resource #>> '{period,start}' < COALESCE(e.resource #>> '{period,end}', 'infinity')
    
CREATE FUNCTION immutable_tsrange_with_ts (_start timestamp, _end timestamp) 
RETURNS tsrange 
LANGUAGE plpgsql IMMUTABLE
AS
$function$ 
    BEGIN 
        RETURN (tsrange (_start, _end));
    END;
$function$;

    
CREATE INDEX episodeofcare_resource_pregnant_period_range__gist 
  ON episodeofcare  
  USING gist (immutable_tsrange_with_ts((resource #>> '{period,start}')::timestamp, (COALESCE(resource #>> '{period,end}', ((resource #>> '{period,start}')::timestamp + '9 month'::INTERVAL)))))  
WHERE resource @@ 'type.#.coding.#.code = "PregnantCard"'::jsquery
    AND resource #>> '{period,start}' < COALESCE(resource #>> '{period,end}', 'infinity')
  
    
    
VACUUM ANALYSE episodeofcare;

DROP INDEX episodeofcare_resource_period_range__gist;

SELECT *
FROM pg_indexes
WHERE tablename = 'flag'

SELECT *
FROM episodeofcare e 
WHERE e.resource @@ 'type.#.coding.#.code = "PregnantCard"'::jsquery
    AND e.resource #>> '{period,start}' > COALESCE(e.resource #>> '{period,end}', 'infinity')
    
SELECT resource -> 'period', resource -> 'status'
FROM flag f 
WHERE resource @@ 'not period.start = * and status = active'::jsquery