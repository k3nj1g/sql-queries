--EXPLAIN ANALYZE 
CREATE TEMP TABLE patient_hypertense AS 
WITH hypertense AS (
  SELECT *
  FROM encounter enc
  WHERE (main_diagnosis_code(enc.resource) BETWEEN 'I10' AND 'I16')
    AND enc.resource #>> '{period,start}' > '2023-08-20')
SELECT DISTINCT ON (p.*)
  pb.resource #>> '{organization,id}' org_id
  , pb.resource #>> '{practitioner,id}' pr_id
  , p.id p_id
  , jsonb_select_keys(p.resource, '{identifier}') p_idf
FROM hypertense enc
JOIN LATERAL 
  (SELECT *
   FROM patient p
   WHERE (p.resource @@ logic_include(enc.resource, 'subject') OR (p.id = ANY (ARRAY ((SELECT (JSONB_PATH_QUERY(enc.resource,'$.subject.id') #>> '{}'))))))
     AND NOT EXISTS 
     (SELECT 1
      FROM flag f
      WHERE f.resource #>> '{subject,id}' = p.id
        AND jsonb_path_query_first(resource, '$."code"."coding"?(@."system" == "urn:CodeSystem:r21.tag")."code"'::jsonpath) #>> '{}' IN ('A04.15.01','A04.15.02'))) p
   ON TRUE
JOIN patientbinding pb
  ON pb.resource #>> '{patient,id}' = p.id;
 
WITH hypertense AS (
  SELECT * FROM patient_hypertense AS ph),
hypertense_dn AS (
  SELECT *
  FROM hypertense AS h
  WHERE EXISTS (SELECT 1
                FROM episodeofcare AS eoc
                WHERE REFERENCE_VALUE(eoc.resource,'patient') = ANY (IDENTIFIER_REFERENCE_VALUE(h.p_id,h.p_idf)))),
hypertense_dn_vimis AS (
  SELECT *
  FROM hypertense_dn AS hd
  WHERE EXISTS (SELECT 1
                FROM flag AS f
                WHERE ((f.resource #>> '{subject,id}') = hd.p_id)
                AND   ((JSONB_PATH_QUERY_FIRST(f.resource,'$.code.coding ? (@.system==\"urn:CodeSystem:r21.tag\").code') #>> '{}') = 'A11.VIMIS')
                AND   (COALESCE(CAST((f.resource #>> '{period,end}') AS DATE),'infinity') > CURRENT_DATE))),
grouped AS (
  SELECT org_id,
         pr_id,
         COUNT(h.*) AS hypertense_count,
         COUNT(hd.*) AS hypertense_dn_count,
         COUNT(hdv.*) AS hypertense_dn_vimis_count
  FROM hypertense AS h
    LEFT JOIN hypertense_dn AS hd USING (org_id,pr_id,p_id)
    LEFT JOIN hypertense_dn_vimis AS hdv USING (org_id,pr_id,p_id)
  GROUP BY org_id,
           pr_id)
SELECT g.*,
       IDENTIFIER_VALUE(org.resource,'urn:identity:oid:Organization') AS org_oid,
       (org.resource #>> '{alias,0}') AS org_name,
       PATIENT_FIO(pr.resource) AS pr_name,
       COALESCE(ROUND((100.0*hypertense_dn_count) / NULLIF(hypertense_count,0),2),0) AS hypertense_dn_ratio,
       COALESCE(ROUND((100.0*hypertense_dn_vimis_count) / NULLIF(hypertense_dn_count,0),2),0) AS hypertense_dn_vimis_ratio
FROM grouped AS g
  INNER JOIN organization AS org ON org.id = org_id
  INNER JOIN practitioner AS pr ON pr.id = pr_id
ORDER BY org_name ASC,
         pr_name ASC