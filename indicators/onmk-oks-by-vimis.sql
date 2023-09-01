CREATE INDEX CONCURRENTLY encounter_period_start_main_diagnosis__btree 
  ON public.encounter (((resource #>> '{period,start}')), main_diagnosis_code(resource));
 
 
 CREATE TEMP TABLE IF NOT EXISTS onmk_oks_encounter AS
SELECT enc.*
FROM encounter AS enc
WHERE (MAIN_DIAGNOSIS_CODE(enc.resource) BETWEEN 'I60' AND 'I65'
    OR ((MAIN_DIAGNOSIS_CODE(enc.resource) >= 'I69') AND (MAIN_DIAGNOSIS_CODE(enc.resource) < 'I70'))
    OR ((MAIN_DIAGNOSIS_CODE(enc.resource) > 'G45') AND (MAIN_DIAGNOSIS_CODE(enc.resource) < 'G47')))
  AND ((enc.resource#>>'{period,start}') >= '2023-08-01');

WITH onmk_oks AS (
  SELECT DISTINCT ON (p.*)
    p.id p_id
    , pb.resource #>> '{organization,id}' org_id
    , pb.resource #>> '{practitioner,id}' pr_id
  FROM onmk_oks_encounter AS enc
  JOIN patient AS p
    ON ((p.resource @@ LOGIC_INCLUDE(enc.resource,'subject'))
        AND JSONB_PATH_EXISTS(p.resource, CAST(CONCAT('$.identifier ? (@.system == '
                                                      , (enc.resource #> '{subject,identifier,system}')
                                                      , ' && @.value == '
                                                      , (enc.resource #> '{subject,identifier,value}')
                                                      , ' && (!exists(@.period.end) || @.period.end.datetime() > "',CURRENT_DATE,'".datetime()))') AS jsonpath)))
      OR (p.id = ANY (ARRAY((SELECT (JSONB_PATH_QUERY(enc.resource,'$.subject.id') #>> '{}')))))
  JOIN patientbinding pb
    ON pb.resource #>> '{patient,id}' = p.id)
, onmk_oks_vimis AS (
  SELECT *
  FROM onmk_oks AS oo
  WHERE EXISTS (
    SELECT 1
    FROM flag AS f
    WHERE ((f.resource #>> '{subject,id}') = oo.p_id)
      AND ((KNIFE_EXTRACT_TEXT(f.resource, '[["code","coding",{"system":"urn:CodeSystem:r21.tag"},"code"]]'))[1] = 'A04.VIMIS')
      AND (COALESCE(CAST((f.resource #>> '{period,end}') AS DATE),'infinity') > CURRENT_DATE)
    LIMIT 1))
, grouped AS (
  SELECT org_id
         , pr_id
         , COUNT(oo.*) AS onmk_oks_count
         , COUNT(oov.*) AS onmk_oks_vimis_count
  FROM onmk_oks AS oo
    LEFT JOIN onmk_oks_vimis AS oov USING (org_id,pr_id,p_id)
  GROUP BY org_id,
           pr_id)
SELECT g.*
       , IDENTIFIER_VALUE(org.resource,'urn:identity:oid:Organization') AS org_oid
       , (org.resource #>> '{alias,0}') AS org_name
       , PATIENT_FIO(pr.resource) AS pr_name
       , COALESCE(ROUND((100.0*onmk_oks_vimis_count) / NULLIF(onmk_oks_count,0),2),0) AS hypertense_dn_ratio
FROM grouped AS g
  INNER JOIN organization AS org ON org.id = org_id
  INNER JOIN practitioner AS pr ON pr.id = pr_id
ORDER BY org_name ASC,
         pr_name ASC;