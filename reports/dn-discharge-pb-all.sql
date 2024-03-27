SELECT org.resource #>> '{alias,0}' AS "Мед. организация",
       (enc.resource #>> '{serviceProvider,display}') AS "МО, лечившая пациента",
       (enc.resource #>> '{hospitalDepartment,display}') AS "Отделение",
       CAST((enc.resource #>> '{period,start}') AS DATE) AS "Дата поступления",
       PATIENT_FIO(p.resource) AS "ФИО пациента",
       CAST((p.resource #>> '{birthDate}') AS DATE) AS "Дата рождения",
       (JSONB_PATH_QUERY_FIRST(p.resource,'$.address ? (@.type=="physical").text') #>> '{}') AS "Адрес проживания",
       (JSONB_PATH_QUERY_FIRST(p.resource,'$.address ? (@.type=="both").text') #>> '{}') AS "Адрес регистрации",
       CAST((enc.resource #>> '{period,end}') AS DATE) AS "Дата выписки",
       (JSONB_PATH_QUERY_FIRST(enc.resource,'$.clinicalResult ? (@.system=="urn:CodeSystem:clinical-result").display') #>> '{}') AS "Результат",
       main_diagnosis(enc.resource) AS "Диагноз"
FROM encounter AS enc
JOIN patient AS p
  ON p.resource @@ LOGIC_INCLUDE (enc.resource,'subject')
    AND (COALESCE (JSONB_EXTRACT_PATH_TEXT (p.resource,'active'),'true') = 'true')
JOIN patientbinding AS pb
  ON (pb.resource #>> '{patient,id}') = p.id
JOIN organization AS org
  ON org.id = pb.resource #>> '{organization,id}'
WHERE (enc.resource #>> '{period,end}') BETWEEN '2024-01-01' AND '2024-04-01'
  AND ((enc.resource -> 'class') @@ 'system="http://terminology.hl7.org/CodeSystem/v3-ActCode" and code="IMP"'::jsquery)
  AND main_diagnosis_code(enc.resource) > 'J12'
  AND main_diagnosis_code(enc.resource) < 'J19'
ORDER BY "Мед. организация";

CREATE OR REPLACE FUNCTION public.main_diagnosis(resource jsonb) RETURNS TEXT AS $$
DECLARE 
  diag jsonb := jsonb_path_query_first(resource, '$.contained ? (exists(@.code.coding ? (@.system == "urn:CodeSystem:icd-10"))) ? (@.id == $diagnosis).code.coding ? (@.system == "urn:CodeSystem:icd-10")', jsonb_build_object('diagnosis', jsonb_path_query_first(resource,'$.diagnosis ? (@.use.coding.code=="1").condition.localRef')));
BEGIN
  RETURN (diag ->> 'code') || ' ' || (diag ->> 'display');
END
$$
LANGUAGE plpgsql;