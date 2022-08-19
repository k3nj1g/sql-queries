WITH to_update AS (
  SELECT p.id, jsonb_set(p.resource, '{extension}', COALESCE ((
    SELECT jsonb_agg(exts.ext)
    FROM (
      SELECT ext
      FROM jsonb_array_elements(p.resource -> 'extension') ext
      WHERE ext @@ 'not (url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery
    ) exts), '[]'::jsonb)) resource
  FROM patient p
  JOIN patientbinding pb ON pb.resource #>> '{patient,id}' = p.id
  WHERE p.resource @@ 'extension.#(url="urn:extension:patient-type" and valueCode=newborn)'::jsquery)
--SELECT *
--FROM to_update tu
UPDATE patient
SET resource = to_update.resource
FROM to_update
WHERE patient.id = to_update.id
RETURNING *w