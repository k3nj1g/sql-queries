WITH to_update AS (
  SELECT id, 
  (WITH g_idfs AS (
     SELECT jsonb_build_object('system', idf->>'system', 'value', idf->>'value', 'assigner', idf#>>'{assigner,identifier,value}') v
           , jsonb_agg(idf) agg
     FROM jsonb_array_elements(resource->'identifier') idfs(idf)
     GROUP BY v)
   , c_idfs AS (
     SELECT idf.agg agg
     FROM g_idfs idf
     WHERE jsonb_array_length(idf.agg) = 1
     UNION ALL
     SELECT jsonb_agg(
       (SELECT idf
       FROM jsonb_array_elements(idf.agg) idfs(idf)
       LIMIT 1)) agg
     FROM g_idfs idf
     WHERE jsonb_array_length(agg) > 1
       AND agg @@ '$.#:(not period = *)'::jsquery
     UNION ALL
     SELECT (
       SELECT jsonb_agg(idf)
       FROM jsonb_array_elements(idf.agg) idfs(idf)
       WHERE idf->'period' IS NOT NULL) agg
     FROM g_idfs idf
     WHERE jsonb_array_length(agg) > 1
       AND agg @@ '$.#(period = *)'::jsquery)
   , r_idfs AS (
     SELECT jsonb_array_elements(agg) idf
     FROM c_idfs)
   SELECT jsonb_agg(idf)
   FROM r_idfs
  ) idfs
FROM patient
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT jsonb_build_object('system', idf->>'system', 'value', idf->>'value', 'assigner', idf#>>'{assigner,identifier,value}') v 
      , jsonb_agg(idf->'period') periods
    FROM jsonb_array_elements(resource->'identifier') idfs(idf)
    WHERE idf->>'system' = 'urn:identity:insurance-gov:Patient'
      AND idf->'assigner' IS NOT NULL 
    GROUP BY v
    HAVING count(idf) > 1) idfs
  WHERE idfs.periods @> 'null'))
UPDATE patient p
SET resource = jsonb_set_lax(p.resource, '{identifier}', tu.idfs)
FROM to_update tu
WHERE p.id = tu.id;