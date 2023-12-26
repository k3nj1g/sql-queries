CREATE MATERIALIZED VIEW patient_with_eye_problems_with_age AS 
SELECT count(DISTINCT p.*) FILTER (WHERE age(to_date(p.resource->>'birthDate','YYYY-MM-DD')) < '18 years') children
  , count(DISTINCT p.*) FILTER (WHERE age(to_date(p.resource->>'birthDate','YYYY-MM-DD')) >= '18 years') adult
  , count(DISTINCT p.*) FILTER (WHERE ((knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] >= 'H40' AND (knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] < 'H42')) glaukoma
  , count(DISTINCT p.*) FILTER (WHERE ((knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] >= 'H52' AND (knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] < 'H53')) h52_all
  , count(DISTINCT p.*) FILTER (WHERE (knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] = 'H52.0') h52_0
  , count(DISTINCT p.*) FILTER (WHERE (knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] = 'H52.1') h52_1
  , count(DISTINCT p.*) FILTER (WHERE (knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] = 'H52.2') h52_2
-- FROM "condition" c
FROM (SELECT * FROM "condition" c LIMIT 10000) c
JOIN patient p 
  ON p.resource @@ logic_include(c.resource, 'subject')
WHERE ((knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] >= 'H00' AND (knife_extract_text(c.resource, '[["code", "coding", {"system": "urn:CodeSystem:icd-10"}, "code"]]'::jsonb))[1] < 'H60')
GROUP BY p.id;
