CREATE TABLE patient_snils (id SERIAL PRIMARY KEY, enp TEXT, bd text, snils TEXT, patient_id TEXT);

DROP TABLE patient_snils;

UPDATE patient_snils 
SET bd = to_date (bd,'DD.mm.YYYY')::TEXT;

--ALTER TABLE patient_snils ADD COLUMN patient_id TEXT;

--BEGIN;
--EXPLAIN ANALYZE
WITH pat_snils AS (
    SELECT ps.id ps_id, p.id patient_id
    FROM (SELECT enp,
                 bd,
                 snils,
                 max(id) id
          FROM patient_snils
          GROUP BY enp, bd, snils
          HAVING count(*) = 1) ps
    JOIN patient p
      ON p.resource @@ concat ('identifier.#(system = "urn:identity:insurance-gov:Patient" and value = "',ps.enp,'")')::jsquery
        AND NOT p.resource -> 'identifier' @@ '$.#.system = "urn:identity:snils:Patient"'::jsquery
        AND p.resource #>> '{birthDate}' = bd) 
UPDATE patient_snils
SET patient_id = pat_snils.patient_id
FROM pat_snils
WHERE id = pat_snils.ps_id;

---
WITH pat_snils AS (
    SELECT patient_id, snils
    FROM patient_snils
    WHERE patient_id IS NOT NULL
)
UPDATE patient p
SET resource = jsonb_insert(p.resource
                            ,array_append('{identifier}',jsonb_array_length(resource -> 'identifier')::text)
                            ,jsonb_build_object('type',jsonb_build_object('coding',jsonb_build_array(jsonb_build_object('code','SNILS','system','http://hl7.org/fhir/ValueSet/identifier-type')))
                            ,'value',pat_snils.snils
                            ,'system','urn:identity:snils:Patient'))
FROM pat_snils
WHERE p.id = pat_snils.patient_id
RETURNING p.id;

VACUUM ANALYZE patient;