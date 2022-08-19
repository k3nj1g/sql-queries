CREATE OR REPLACE FUNCTION main_diagnosis_code(resource jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 AS $function$
   SELECT jsonb_path_query_first(resource, '$.contained ? (exists(@.code.coding ? (@.system == "urn:CodeSystem:icd-10"))) ? (@.id == $diagnosis).code.coding ? (@.system == "urn:CodeSystem:icd-10").code', jsonb_build_object('diagnosis', jsonb_path_query_first(resource,'$.diagnosis ? (@.use.coding.code=="1").condition.localRef'))) #>> '{}';
 $function$;

CREATE INDEX encounter_discharged_covid19_period__gist 
  ON encounter USING gist(immutable_tsrange((resource #>> '{period,start}'), (coalesce(resource #>> '{period,end}', 'infinity'))))
    WHERE (resource #>> '{period,start}') < COALESCE((resource #>> '{period,end}'), 'infinity')
        AND resource -> 'clinicalResult' @@ '(system="urn:CodeSystem:clinical-result" and code in ("101","201","107","207","302","108","208","308"))'::jsquery
        AND main_diagnosis_code(resource) IN ('U07.1', 'U07.2')

SELECT to_jsonb(enc.*) AS encounter,
       to_jsonb(p.*) AS patient
FROM encounter AS enc
  INNER JOIN patient AS p
          ON (p.resource @@ LOGIC_INCLUDE (enc.resource,'subject'))
         AND ( (p.resource #>> '{deceased,dateTime}') IS NULL)
  INNER JOIN patientbinding AS pb
          ON ( (pb.resource #>> '{patient,id}') = p.id)
         AND ( (pb.resource #>> '{organization,id}') = '1150e915-f639-4234-a795-1767e0a0be5f')
WHERE (IMMUTABLE_TSRANGE((enc.resource #>> '{period,start}'),coalesce((enc.resource #>> '{period,end}'),'infinity')) && IMMUTABLE_TSRANGE('2021-01-01','2021-02-01'))
AND   ((enc.resource #>> '{period,start}') <coalesce((enc.resource #>> '{period,end}'),'infinity'))
AND   ((enc.resource -> 'clinicalResult') @@ 'system="urn:CodeSystem:clinical-result" and code in ("101","201","107","207","302","108","208","308")'::jsquery)
AND   (MAIN_DIAGNOSIS_CODE(enc.resource) IN ('U07.1','U07.2'))