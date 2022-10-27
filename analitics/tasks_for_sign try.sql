WITH doctors AS (
  SELECT main_org.id org_id, count(DISTINCT pr.*)
  FROM practitionerrole prr
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
  JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
  JOIN organization main_org ON main_org.resource @@ logic_include(org.resource, 'mainOrganization')
    AND main_org.resource -> 'partOf' IS NULL AND COALESCE (main_org.resource ->> 'active', 'true') = 'true'
    AND jsonb_path_query_first(main_org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' SIMILAR TO '1.2.643.5.1.13.13.12.2.21.15\d{2}'
  WHERE prr.resource @@ 'active=true'::jsquery  
  GROUP BY 1
)
, jan AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-01-01' AND '2022-01-11'
)
, feb AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-11-01' AND '2022-01-21'
)
, evrthg AS (
  SELECT *
  FROM jan
  UNION 
  SELECT *
  FROM feb)
SELECT ((knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performingOrganization"}]}}, "value", "Reference", "display"]]$$))[1]) org
  , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '86' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "86"
  , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '37' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "37"
  , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '5' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "5"
  , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '8' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "8"
  , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '6' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "6"
  , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '3' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "3"
  , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT null and (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] in ('86','37','5','8','6','3')) signed
  , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NULL) not_signed  
FROM evrthg t
JOIN doctors d ON d.org_id = (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performingOrganization"}]}}, "value", "Reference", "id"]]$$))[1]
GROUP BY 1


--- таски за год
WITH doctors AS MATERIALIZED (
  SELECT main_org.id org_id, count(DISTINCT pr.*) doctors_count
  FROM practitionerrole prr
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
  JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
  JOIN organization main_org ON main_org.resource @@ logic_include(org.resource, 'mainOrganization')
    AND main_org.resource -> 'partOf' IS NULL AND COALESCE (main_org.resource ->> 'active', 'true') = 'true'
--    AND jsonb_path_query_first(main_org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' SIMILAR TO '1.2.643.5.1.13.13.12.2.21.15\d{2}'
  WHERE prr.resource @@ 'active=true'::jsquery  
  GROUP BY 1
)
, jan AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-01-01' AND '2022-02-01'
)
, feb AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-02-01' AND '2022-03-01'
)
, mar AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-03-01' AND '2022-04-01'
)
, apr AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-04-01' AND '2022-05-01'
)
, may AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-05-01' AND '2022-06-01'
)
, jun AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-06-01' AND '2022-07-01'
)
, jul AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-07-01' AND '2022-08-01'
)
, aug AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-08-01' AND '2022-09-01'
)
, sep AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-09-01' AND '2022-10-01'
)
, oct AS (
  SELECT * 
  FROM task t 
  WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] IS NOT NULL 
    AND resource #>> '{executionPeriod,start}' between '2022-10-01' AND '2022-11-01'
)
, evrthg AS (
  SELECT *
  FROM jan
  UNION 
  SELECT *
  FROM feb
  UNION 
  SELECT *
  FROM mar
  UNION 
  SELECT *
  FROM apr
  UNION 
  SELECT *
  FROM may
  UNION 
  SELECT *
  FROM jun
  UNION 
  SELECT *
  FROM jul
  UNION 
  SELECT *
  FROM aug
  UNION 
  SELECT *
  FROM sep
  UNION 
  SELECT *
  FROM oct)
, calculated AS (
  SELECT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performingOrganization"}]}}, "value", "Reference", "id"]]$$))[1] org_id
    , ((knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performingOrganization"}]}}, "value", "Reference", "display"]]$$))[1]) org_name
    , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '86' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "86"
    , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '37' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "37"
    , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '5' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "5"
    , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '8' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "8"
    , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '6' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "6"
    , count(distinct (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] = '3' AND (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT NULL) AS "3"
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NOT null and (knife_extract_text(resource, '[["code", "coding", {"system": "urn:CodeSystem:ramd-1.2.643.5.1.13.13.11.1522"}, "code"]]'::jsonb))[1] in ('86','37','5','8','6','3')) signed
    , count(DISTINCT (knife_extract_text(t.resource, $$[["input", {"type":{"coding":[{"system":"urn:CodeSystem:task-input-type","code":"performerDoctorPractitioner"}]}}, "value", "Reference", "id"]]$$))[1]) FILTER (WHERE (knife_extract_text(resource, '[["input", {"type": {"coding": [{"code": "signature", "system": "urn:CodeSystem:task-input-type"}]}}, "value", "Signature", "data"]]'::jsonb))[1] IS NULL) not_signed  
FROM evrthg t
GROUP BY 1,2)
SELECT t.org_id
  , t.org_name 
  , t."86"
  , round(100 * (t."86"::numeric / d.doctors_count)::numeric, 2) "86_percent"
  , t."37"
  , round(100 * (t."37"::numeric / d.doctors_count)::numeric, 2) "37_percent"
  , t."5"
  , round(100 * (t."5"::numeric / d.doctors_count)::numeric, 2) "5_percent"
  , t."8"
  , round(100 * (t."8"::numeric / d.doctors_count)::numeric, 2) "8_percent"
  , t."6"
  , round(100 * (t."6"::numeric / d.doctors_count)::numeric, 2) "6_percent"
  , t."3"
  , round(100 * (t."3"::numeric / d.doctors_count)::numeric, 2) "3_percent"
  , t.signed
  , round(100 * (t.signed::numeric / d.doctors_count)::numeric, 2) "signed_percent"
  , t.not_signed
FROM calculated t 
LEFT JOIN doctors d ON d.org_id = t.org_id 