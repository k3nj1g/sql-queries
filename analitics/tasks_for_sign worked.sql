CREATE TABLE tasks_for_sign (
  org_id TEXT
  , "86" text[]
  , "37" text[]
  , "5" text[]
  , "7" text[]
  , "6" text[]
  , "3" text[]
  , signed text[]
  , not_signed text[]
)

WITH tasks AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' >= '2022-01-01'
    AND resource #>> '{executionPeriod,start}' < '2022-07-01')
, calculated AS (
  SELECT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performingOrganization"}]}}, "value", "Reference", "id"]]$$))[1] org_id
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '86' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "86"
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '37' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "37"
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '5' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "5"
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '7' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "7"
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '6' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "6"
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '3' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "3"
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT null and (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] in ('86','37','5','7','6','3')) signed
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NULL) not_signed  
FROM tasks t
GROUP BY 1)
INSERT INTO tasks_for_sign 
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

SELECT *
FROM tasks_for_sign;
 
WITH doctors AS (
  SELECT main_org.id main_org_id
    , main_org.resource #>> '{alias,0}' main_org_name
    , count(DISTINCT pr.*) doctors_count
  FROM practitionerrole prr
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
  JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
  JOIN organization main_org ON main_org.resource @@ logic_include(org.resource, 'mainOrganization')
    AND main_org.resource -> 'partOf' IS NULL AND COALESCE (main_org.resource ->> 'active', 'true') = 'true'
  WHERE prr.resource @@ 'active=true and roleCategory.code in ("doctor", "administrative-managerial-personnel")'::jsquery 
  GROUP BY 1
)  
SELECT main_org_name
  , "86"
  , round(100 * (t."86"::numeric / d.doctors_count)::numeric, 2) "86_percent"
  , "37"
  , round(100 * (t."37"::numeric / d.doctors_count)::numeric, 2) "37_percent"
  , "5"    
  , round(100 * (t."5"::numeric / d.doctors_count)::numeric, 2) "5_percent"
  , "7"    
  , round(100 * (t."7"::numeric / d.doctors_count)::numeric, 2) "7_percent"
  , "6"
  , round(100 * (t."6"::numeric / d.doctors_count)::numeric, 2) "6_percent"
  , "3"  
  , round(100 * (t."3"::numeric / d.doctors_count)::numeric, 2) "3_percent"
  , signed  
  , round(100 * (t.signed::numeric / d.doctors_count)::numeric, 2) "signed_percent"
  , not_signed
  , doctors_count
FROM tasks_for_sign t  
JOIN doctors d ON d.main_org_id = t.org_id
ORDER BY 1;