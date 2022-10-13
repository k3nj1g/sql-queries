CREATE TABLE patient_idf_fix(id TEXT, polis TEXT , status TEXT);

WITH patients AS (
  SELECT *
  FROM patient p 
  WHERE resource @@ 'identifier.#(system = "urn:identity:insurance-gov:Patient" and assigner.identifier.value in ("21001", "21002")) and identifier.@# > 1'::jsquery)
, idfs AS (
  SELECT id, jsonb_array_elements(resource -> 'identifier') idfs
  FROM patients
)
, insurance AS (
  SELECT *
  FROM idfs
  WHERE idfs.idfs ->> 'system' = 'urn:identity:insurance-gov:Patient'
    AND idfs.idfs #>> '{assigner,identifier,value}' IN ('21001', '21002'))
, broken AS (
  SELECT id
  , idfs ->> 'value' polis
  FROM insurance
  GROUP BY 1,2
  HAVING count(*) > 1
    AND (array_agg(CASE WHEN idfs ->> 'period' IS NOT NULL THEN TRUE ELSE FALSE END) @> ARRAY[FALSE]))
--SELECT *
--FROM broken
INSERT INTO patient_idf_fix
(SELECT id, polis, 'pending'
 FROM broken);
  
CREATE INDEX patient_idf_fix_id ON patient_idf_fix (id);
CREATE INDEX patient_idf_fix_status ON patient_idf_fix (status);
VACUUM ANALYZE patient_idf_fix; 

SELECT p.id, (resource->'identifier'), (jsonb_set(
                                  p.resource
                                  , '{identifier}'
                                  , (SELECT jsonb_path_query_array(
                                              jsonb_agg(CASE WHEN value ->> 'system' = 'urn:identity:insurance-gov:Patient' AND value ->> 'value' = polis AND value ->> 'period' IS NULL THEN NULL
                                                          ELSE value
                                                        END)
                                              , '$ ? (@ != null)') 
                                     FROM jsonb_array_elements(p.resource->'identifier')))
                                     -> 'identifier')
FROM patient_idf_fix pf
JOIN patient p ON p.id = pf.id

-- Фикс идентификаторов пациента
WITH part AS (
  SELECT *
  FROM patient_idf_fix pf
  WHERE status = 'pending'
  LIMIT 3000)
, fix AS (
  UPDATE patient p
  SET resource = jsonb_set_lax(
                   p.resource
                   , '{identifier}'
                   , (SELECT jsonb_path_query_array(
                               jsonb_agg(CASE 
                                           WHEN value ->> 'system' = 'urn:identity:insurance-gov:Patient' AND value ->> 'value' = polis AND value ->> 'period' IS NULL THEN NULL
                                           ELSE value
                                         END)
                               , '$ ? (@ != null)') 
                      FROM jsonb_array_elements(p.resource->'identifier')))
  FROM part j
  WHERE p.id = j.id)
UPDATE patient_idf_fix pf
SET status = 'completed'
FROM part p
WHERE pf.id = p.id
RETURNING p.id, p.polis

SELECT count(*)
FROM patient_idf_fix
WHERE status = 'pending'

-- Фикс идентификаторов пациента в цикле
DO LANGUAGE plpgsql
$do$
BEGIN 
   FOR i IN 1..10 LOOP
     WITH part AS (
       SELECT *
       FROM patient_idf_fix pf
       WHERE status = 'pending'
       LIMIT 3000)
     , fix AS (
       UPDATE patient p
       SET resource = jsonb_set_lax(
                        p.resource
                        , '{identifier}'
                        , (SELECT jsonb_path_query_array(
                                    jsonb_agg(CASE 
                                                WHEN value ->> 'system' = 'urn:identity:insurance-gov:Patient' AND value ->> 'value' = polis AND value ->> 'period' IS NULL THEN NULL
                                                ELSE value
                                              END)
                                    , '$ ? (@ != null)') 
                           FROM jsonb_array_elements(p.resource->'identifier')))
       FROM part j
       WHERE p.id = j.id)
     UPDATE patient_idf_fix pf
     SET status = 'completed'
     FROM part p
     WHERE pf.id = p.id;
   END LOOP;
END
$do$;

DROP TABLE patient_idf_fix;

 

                