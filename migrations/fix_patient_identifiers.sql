CREATE MATERIALIZED VIEW pat_idf_dup AS 
SELECT patient.id,
    ( SELECT d.idf_value
           FROM ( SELECT (idfs.idf ->> 'value'::text) AS idf_value
                   FROM jsonb_array_elements((patient.resource -> 'identifier'::text)) idfs(idf)
                  WHERE ((idfs.idf ->> 'system'::text) IN ('urn:identity:insurance-gov:Patient','urn:identity:insurance-gov-legacy:Patient') AND ((idfs.idf #>> '{period,end}'::text[]) IS NULL))
                  GROUP BY (idfs.idf ->> 'value'::text)
                 HAVING (count(*) > 1)) d
         LIMIT 1) AS idf_value
FROM patient
WHERE (EXISTS (SELECT 1
           FROM jsonb_array_elements((patient.resource -> 'identifier'::text)) idfs(idf)
          WHERE ((idfs.idf ->> 'system'::text) in ('urn:identity:insurance-gov:Patient','urn:identity:insurance-gov-legacy:Patient') AND ((idfs.idf #>> '{period,end}'::text[]) IS NULL))
          GROUP BY (idfs.idf ->> 'value'::text)
         HAVING (count(*) > 1)));

REFRESH MATERIALIZED VIEW pat_idf_dup;

SELECT *
FROM pat_idf_dup;

DROP MATERIALIZED VIEW pat_idf_dup;

UPDATE patient p 
SET resource = jsonb_set_lax(p.resource, '{identifier}', (
  WITH idfs AS (
    SELECT idf
    FROM jsonb_array_elements(p.resource->'identifier') idfs(idf))
  , idf_insurance_gov AS (
    SELECT idf
    FROM idfs
    WHERE idf->>'system' = 'urn:identity:insurance-gov:Patient'
      AND idf->>'value' = pid.idf_value
      AND idf#>>'{period,end}' IS NULL
    ORDER BY idf#>>'{period,start}' NULLS LAST 
    LIMIT 1)
  , idf_insurance_legacy AS (
    SELECT idf
    FROM idfs
    WHERE idf->>'system'='urn:identity:insurance-gov-legacy:Patient'
      AND idf->>'value'=pid.idf_value
      AND idf#>>'{period,end}' IS NULL
    ORDER BY idf#>>'{period,start}' NULLS LAST 
    LIMIT 1)
  , idf_insurance AS (
    SELECT COALESCE((SELECT idf FROM idf_insurance_gov), (SELECT idf FROM idf_insurance_legacy)) idf)
  , idf_others AS (
    SELECT idf
    FROM idfs
    WHERE NOT (
      idf->>'system' IN ('urn:identity:insurance-gov:Patient','urn:identity:insurance-gov-legacy:Patient')
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
--RETURNING p.*


SELECT p.id, jsonb_set_lax(p.resource, '{identifier}', (
  WITH idfs AS (
    SELECT idf
    FROM jsonb_array_elements(p.resource->'identifier') idfs(idf))
  , idf_insurance_gov AS (
    SELECT idf
    FROM idfs
    WHERE idf->>'system' = 'urn:identity:insurance-gov:Patient'
      AND idf->>'value' = pid.idf_value
      AND idf#>>'{period,end}' IS NULL
    ORDER BY idf#>>'{period,start}' NULLS LAST 
    LIMIT 1)
  , idf_insurance_legacy AS (
    SELECT idf
    FROM idfs
    WHERE idf->>'system'='urn:identity:insurance-gov-legacy:Patient'
      AND idf->>'value'=pid.idf_value
      AND idf#>>'{period,end}' IS NULL
    ORDER BY idf#>>'{period,start}' NULLS LAST 
    LIMIT 1)
  , idf_insurance AS (
    SELECT COALESCE((SELECT idf FROM idf_insurance_gov), (SELECT idf FROM idf_insurance_legacy)) idf
  )
  , idf_others AS (
    SELECT idf
    FROM idfs
    WHERE NOT (
      idf->>'system' IN ('urn:identity:insurance-gov:Patient','urn:identity:insurance-gov-legacy:Patient')
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
FROM (SELECT * FROM pat_idf_dup pid LIMIT 100) pid
JOIN patient p
  ON p.id = pid.id
--WHERE pid.id = 'b80594b8-8aec-4250-a2d2-83bf3cfdde8d'

SELECT jsonb_select_keys(idf, '{system,value,period}')
FROM (SELECT jsonb_array_elements(resource->'identifier') idf
FROM patient
WHERE id = '2d367a3d-909f-4aed-acd4-6d2c68907a27') idfs
ORDER BY idf->>'value'

SELECT resource,  ((EXISTS ( SELECT 1
           FROM jsonb_array_elements((p.resource -> 'identifier'::text)) idfs(idf)
          WHERE (((idfs.idf ->> 'system'::text) = 'urn:identity:insurance-gov:Patient'::text) AND ((idfs.idf #>> '{period,end}'::text[]) IS NULL))
          GROUP BY (idfs.idf ->> 'value'::text)
         HAVING (count(*) > 1))))
FROM patient p
WHERE p.id = '92f6f066-afa5-4760-ac16-602186efb6ba'

SELECT *
FROM pat_idf_dup pid 
WHERE pid.id = 'b80594b8-8aec-4250-a2d2-83bf3cfdde8d'
