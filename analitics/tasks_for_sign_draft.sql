--EXPLAIN ANALYZE 
SELECT 
  ((knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performingOrganization"}]}}, "value", "Reference", "display"]]$$))[1])
  , (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"signature"}]}}, "value", "Signature", "who", "id"]]$$))
  , ARRAY (
    SELECT perf_id
    FROM UNNEST(knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$)) perf(perf_id)
    LEFT JOIN (
      SELECT *
      FROM UNNEST(knife_extract_text(resource, $$[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "who", "id"]]$$))
    ) sig(sig_id) ON sig_id = perf_id
    WHERE sig_id IS NULL 
  )
FROM task t
--WHERE resource @@ 'code.coding.#(system="urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522" and code="7")'::jsquery
WHERE resource @@ 'code.coding.#(system="urn:CodeSystem:chu-task-code" and code="documentSignature")'::jsquery
  AND resource #>> '{executionPeriod,start}' >= '2022-05-01'
  AND resource #>> '{executionPeriod,start}' < '2022-05-05'

CREATE TABLE tasks_for_sign_array (
  org_id TEXT
  , "86" text[]
  , "37" text[]
  , "5" text[]
  , "7" text[]
  , "6" text[]
  , "3" text[]
  , signed text[]
  , signed_all text[]
  , not_signed text[]
);

DROP TABLE tasks_for_sign_test;

SELECT * FROM tasks_for_sign_test;

TRUNCATE tasks_for_sign_test;

-- 2 + 10

DO
$do$
BEGIN
   FOR i IN 0..10 LOOP
     RAISE NOTICE 'Calculate loop %', i;  
     WITH tasks AS (
       SELECT * 
       FROM task t 
       WHERE resource @@ 'code.coding.#(system="urn:CodeSystem:chu-task-code" and code="documentSignature")'::jsquery
         AND resource #>> '{executionPeriod,start}' >= (('2022-01-01'::date + '0 weeks'::INTERVAL)::date + (i::text || ' weeks')::INTERVAL)::text
         AND resource #>> '{executionPeriod,start}' < (('2022-01-01'::date +  '0 weeks'::INTERVAL)::date + ((i+1)::text || ' weeks')::INTERVAL)::text)
     , calculated AS (
       SELECT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performingOrganization"}]}}, "value", "Reference", "id"]]$$))[1] org_id
         , array_agg(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '86' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "86"
         , array_agg(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '37' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "37"
         , array_agg(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '5' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "5"
         , array_agg(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '7' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "7"
         , array_agg(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '6' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "6"
         , array_agg(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '3' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "3"
         , array_agg(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL AND (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] in ('86','37','5','7','6','3')) signed
         , array_agg(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL signed_all
         , array_agg(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"signature"}]}}, "value", "Signature", "who", "id"]]$$))[1]) signers
         , array_agg(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NULL) not_signed  
     FROM tasks t
     GROUP BY 1)
     INSERT INTO tasks_for_sign_test
       SELECT t.org_id 
         , t."86"
         , t."37"
         , t."5"
         , t."7"
         , t."6"
         , t."3"
         , t.signed
         , t.not_signed
       FROM calculated t;
   END LOOP;
END;
$do$;

CREATE OR REPLACE AGGREGATE array_concat_agg (anycompatiblearray)(SFUNC = array_cat,STYPE = anycompatiblearray);

WITH grouped AS (
SELECT t.org_id
    , array_concat_agg("86") "86"
    , array_concat_agg("37") "37"
    , array_concat_agg("5") "5"
    , array_concat_agg("7") "7"
    , array_concat_agg("6") "6"
    , array_concat_agg("3") "3"
    , array_concat_agg(signed) signed
    , array_concat_agg(not_signed) not_signed
FROM tasks_for_sign_test t
GROUP BY 1)
SELECT org_id
  , (SELECT count(DISTINCT v) FROM unnest("86") a(v)) "86"
  , (SELECT count(DISTINCT v) FROM unnest("37") a(v)) "37"
  , (SELECT count(DISTINCT v) FROM unnest("5") a(v)) "5"
  , (SELECT count(DISTINCT v) FROM unnest("7") a(v)) "7"
  , (SELECT count(DISTINCT v) FROM unnest("6") a(v)) "6"
  , (SELECT count(DISTINCT v) FROM unnest("3") a(v)) "3"
  , (SELECT count(DISTINCT v) FROM unnest(signed) a(v)) signed
  , (SELECT count(DISTINCT v) FROM unnest(not_signed) a(v)) not_signed  
FROM grouped;

SELECT * FROM tasks_for_sign_test;


       FROM calculated t;  