CREATE MATERIALIZED VIEW pat_idf_dup AS 
SELECT patient.id,
    ( SELECT d.idf_value
           FROM ( SELECT (idfs.idf ->> 'value'::text) AS idf_value
                   FROM jsonb_array_elements((patient.resource -> 'identifier'::text)) idfs(idf)
                  WHERE (((idfs.idf ->> 'system'::text) = 'urn:identity:insurance-gov:Patient'::text) AND ((idfs.idf #>> '{period,end}'::text[]) IS NULL))
                  GROUP BY (idfs.idf ->> 'value'::text)
                 HAVING (count(*) > 1)) d
         LIMIT 1) AS idf_value
FROM patient
WHERE (EXISTS ( SELECT 1
           FROM jsonb_array_elements((patient.resource -> 'identifier'::text)) idfs(idf)
          WHERE (((idfs.idf ->> 'system'::text) = 'urn:identity:insurance-gov:Patient'::text) AND ((idfs.idf #>> '{period,end}'::text[]) IS NULL))
          GROUP BY (idfs.idf ->> 'value'::text)
         HAVING (count(*) > 1)));
        
REFRESH MATERIALIZED VIEW pat_idf_dup;

UPDATE patient p 
SET resource = jsonb_set_lax(p.resource, '{identifier}', (
  WITH idfs AS (
    SELECT idf
    FROM jsonb_array_elements(p.resource->'identifier') idfs(idf))
  , idf_insurance AS (
    SELECT idf
    FROM idfs
    WHERE idf->>'system'='urn:identity:insurance-gov:Patient'
      AND idf->>'value'=pid.idf_value
      AND idf#>>'{period,end}' IS NULL
    ORDER BY idf#>>'{period,start}' NULLS LAST 
    LIMIT 1)
  , idf_others AS (
    SELECT idf
    FROM idfs
    WHERE NOT (
      idf->>'system'='urn:identity:insurance-gov:Patient'
      AND idf->>'value'=pid.idf_value
      AND idf#>>'{period,end}' IS NULL))
  , idf_all AS (
    SELECT idf
    FROM idf_insurance
    UNION ALL
    SELECT *
    FROM idf_others)
  SELECT jsonb_agg(idf)
  FROM idf_all)) 
FROM pat_idf_dup pid
WHERE p.id = pid.id;
