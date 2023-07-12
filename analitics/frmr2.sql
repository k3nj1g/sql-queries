DELETE
FROM practitionerrole 
WHERE resource #>> '{code,0,text}' SIMILAR TO 'Уборщик%|Буфетчик%|Администратор%|Буфетчица%|Заведующий складом%|Бухгалтер%|Уборщик производственных и служебных помещений%|Машинистка%|Заведующая складом%|Начальник%|Сестра-хозяйка%|Директор%';

SELECT count(DISTINCT prr.*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:source:frmr2:PractitionerRole"'::jsquery) frmr2
  , count(DISTINCT prr.*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:identity:frmr:PractitionerRole"'::jsquery) frmr
FROM organization main_org
JOIN organization org ON org.resource @@ logic_revinclude(main_org.resource, main_org.id, 'mainOrganization') 
JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(org.resource, org.id, 'organization')
JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
WHERE main_org.resource @@ 'identifier.#.value = "1.2.643.5.1.13.13.12.2.21.1537"'::jsquery;

SELECT count(*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:source:frmr2:PractitionerRole"'::jsquery) frmr2
  , count(*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:identity:frmr:PractitionerRole"'::jsquery) frmr
  , count(*) prr_all
FROM practitionerrole prr
WHERE COALESCE (prr.resource #>> '{period,end}', 'infinity')::date > current_date;

SELECT *, prr - prr_frmr2, round((pr_frmr2::DECIMAL / pr) * 100.0, 2) pr_percent, round((prr_frmr2::DECIMAL / prr) * 100.0, 2) prr_percent
FROM (SELECT 
  main_org.resource #>> '{alias,0}' org_name
  , jsonb_path_query_first(main_org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' org_oid
  , count(DISTINCT pr.*) FILTER (WHERE pr.resource @@ 'identifier.#.system="urn:source:frmr2:Practitioner"  '::jsquery) pr_frmr2
  , count(DISTINCT pr.*) pr
  , count(DISTINCT prr.*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:source:frmr2:PractitionerRole"'::jsquery) prr_frmr2
  , count(DISTINCT prr.*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:identity:frmr:PractitionerRole"'::jsquery) prr_frmr
  , count(DISTINCT prr.*) prr
FROM organization main_org
JOIN organization org ON org.resource @@ logic_revinclude(main_org.resource, main_org.id, 'mainOrganization') 
JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(org.resource, org.id, 'organization')
  AND COALESCE (prr.resource #>> '{period,end}', 'infinity')::date > current_date
JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
GROUP BY 1,2) c
--WHERE main_org.resource @@ 'identifier.#.value = "1.2.643.5.1.13.13.12.2.21.1540"'::jsquery;
ORDER BY prr_percent DESC;

SELECT *, prr - prr_frmr2 diff, round((prr_frmr2::DECIMAL / prr) * 100.0, 2) prr_percent
FROM (SELECT 
  main_org.resource #>> '{alias,0}' org_name
  , jsonb_path_query_first(main_org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}'
  , count(DISTINCT prr.*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:source:frmr2:PractitionerRole"'::jsquery) prr_frmr2
  , count(DISTINCT prr.*) FILTER (WHERE prr.resource @@ 'identifier.#.system="urn:identity:frmr:PractitionerRole"'::jsquery) prr_frmr
  , count(DISTINCT prr.*) FILTER (WHERE NOT prr.resource @@ 'employment.code = *'::jsquery AND jsonb_path_query_first(prr.resource, '$.identifier ? (@.system == "urn:source:frmr2:PractitionerRole").value') IS NULL) prr_no_employment
  , count(DISTINCT prr.*) FILTER (WHERE jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') IS NULL AND jsonb_path_query_first(prr.resource, '$.identifier ? (@.system == "urn:source:frmr2:PractitionerRole").value') IS NULL) prr_no_position
  , count(DISTINCT prr.*) FILTER (WHERE prr.resource #>> '{period,start}' IS NULL) prr_no_start
  , count(DISTINCT prr.*) FILTER (WHERE prr.resource #>> '{period,start}' IS NULL) prr_not_matched  
  , count(DISTINCT prr.*) prr
FROM organization main_org
JOIN organization org ON org.resource @@ logic_revinclude(main_org.resource, main_org.id, 'mainOrganization') 
JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(org.resource, org.id, 'organization')
  AND COALESCE (prr.resource #>> '{period,end}', 'infinity')::date > current_date
JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
WHERE main_org.resource @@ 'identifier.#.value = "1.2.643.5.1.13.13.12.2.21.1537"'::jsquery
GROUP BY 1,2) c
ORDER BY prr_percent DESC;

SELECT DISTINCT prr.*
FROM organization main_org
JOIN organization org ON org.resource @@ logic_revinclude(main_org.resource, main_org.id, 'mainOrganization') 
JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(org.resource, org.id, 'organization')
  AND COALESCE (prr.resource #>> '{period,end}', 'infinity')::date > current_date
  AND NOT prr.resource @@ 'identifier.#.system="urn:identity:frmr:PractitionerRole"'::jsquery
  AND prr.resource @@ 'identifier.#.system="urn:source:frmr2:PractitionerRole"'::jsquery
JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
  -- AND NOT pr.resource @@ 'identifier.#.system="urn:source:frmr2:Practitioner"'::jsquery
WHERE main_org.resource @@ 'identifier.#.value = "1.2.643.5.1.13.13.12.2.21.1523"'::jsquery;

SELECT prr.resource ->> 'active', prr.resource -> 'period'
FROM practitioner pr
JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(pr.resource, pr.id, 'practitioner')
WHERE pr.resource @@ 'identifier.#.value = "010-757-465 31"'::jsquery;

SELECT org_name, main_org_oid, pr_snils, prr_text, frmr1, pr_name, prr_id
FROM (SELECT DISTINCT ON (prr.id) main_org.resource #>> '{alias,0}' org_name,
       jsonb_path_query_first(main_org.resource,'$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' main_org_oid,
       jsonb_path_query_first(org.resource,'$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' org_oid,
       jsonb_path_query_first(pr.resource,'$.identifier? (@.system == "urn:identity:snils:Practitioner").value') #>> '{}' pr_snils
       , patient_fio(pr.resource) pr_name
       , prr.resource #>> '{code,0,text}' prr_text
       , jsonb_path_query_first(prr.resource, '$.identifier ? (@.system == "urn:identity:frmr:PractitionerRole").value') frmr1
       , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') frmr_position
       , prr.resource #>> '{period,start}' "start"
       , prr.id prr_id
FROM organization main_org
  JOIN organization org ON org.resource @@ logic_revinclude (main_org.resource,main_org.id,'mainOrganization')
  JOIN practitionerrole prr
    ON prr.resource @@ logic_revinclude (org.resource,org.id,'organization')
   AND coalesce (prr.resource #>> '{period,end}','infinity')::date> current_date
  JOIN practitioner pr ON pr.resource @@ logic_include (prr.resource,'practitioner')
WHERE 
  -- jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') IS NULL
  -- AND 
  NOT prr.resource @@ 'identifier.#.system="urn:source:frmr2:PractitionerRole"'::jsquery
  and prr.resource @@ 'identifier.#.system="urn:identity:frmr:PractitionerRole"'::jsquery) s;
-- ORDER BY 1,4
-- WHERE main_org.resource @@ 'identifier.#.value = "1.2.643.5.1.13.13.12.2.21.1530"'::jsquery;

SELECT main_org.resource #>> '{alias,0}' org_name,
       JSONB_PATH_QUERY_FIRST(main_org.resource,'$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' org_oid,
       jsonb_path_query_first(org.resource,'$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' dept_oid,
       patient_fio(pr.resource) fio,
       jsonb_path_query_first(pr.resource,'$.identifier? (@.system == "urn:identity:snils:Practitioner").value') #>> '{}' pr_snils, 
       prr.resource #>> '{code,0,text}' prr_text, 
       jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' frmr_position,
       prr.resource #>> '{employment,code}' employment,
       prr.resource #>> '{period,start}' "start"
FROM practitionerrole prr
LEFT JOIN practitioner pr 
  ON pr.resource @@ logic_include (prr.resource,'practitioner')
LEFT JOIN organization org 
  ON org.resource @@ logic_include(prr.resource, 'organization')
LEFT JOIN organization main_org 
  ON main_org.resource @@ logic_include(org.resource, 'mainOrganization')
WHERE COALESCE (prr.resource #>> '{period,end}','infinity')::DATE> CURRENT_DATE
   AND NOT prr.resource @@ 'identifier.#.system in ("urn:identity:frmr:PractitionerRole","urn:source:frmr2:PractitionerRole")'::jsquery
ORDER BY 1, 2;   