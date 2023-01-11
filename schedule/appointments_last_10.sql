WITH appointments AS MATERIALIZED (
  SELECT --DISTINCT ON (jsonb_path_query_first(app.resource, '$.participant.actor?(@.resourceType == "Patient")') #>> '{}')
    jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' frmr_position
    , app.resource #>> '{mainOrganization,display}' "МО"
    , jsonb_path_query_first(prr.resource, '$.code.text') #>> '{}' "Специальность врача"
    , patient_fio((prr.resource #> '{derived}')) "ФИО врача"
    , to_char((app.resource ->> 'start')::timestamp, 'DD.MM.YYYY HH24:MI') "Время приема записи"
    , jsonb_path_query_first(app.resource, '$.participant.actor?(@.resourceType == "Patient")') #>> '{id}' patient_id
    , to_char(app.ts, 'DD.MM.YYYY HH24:MI') "Дата закрытия апоинтмента"
    , app.resource ->> 'start' "start"
    , app.resource ->> 'status' "status"
    , prr.resource prr_resource
  FROM appointment app
  JOIN practitionerrole prr
    ON prr.id = jsonb_path_query_first(app.resource, '$.participant.actor?(@.resourceType == "PractitionerRole")') #>> '{id}'
    AND prr.resource -> 'code' @@ '#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("110", "59", "49", "122","13","54","53","100","103","101","83","85","119","120","87","28"))'::jsquery
  WHERE app.resource #>> '{mainOrganization,id}' IN ('70042e7c-cf2d-4a22-9d6a-c89444857b07', '1150e915-f639-4234-a795-1767e0a0be5f', '0be4ac6b-2556-4da4-80d5-0d7c4e7926ce')
   AND (app.resource ->> 'start') >= '2022-10-01'
   AND app.resource ->> 'from' in ('doctor','nurse')
   AND app.resource ->> 'status' = 'arrived')
, "110" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '110')
, "59" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '59'
  ORDER BY "start" DESC)
, "49" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '49'
  ORDER BY "start" DESC)
, "122" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '122'
  ORDER BY "start" DESC)
, "13" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '13'
  ORDER BY "start" DESC)
, "54" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '54'
  ORDER BY "start" DESC)
, "53" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '53'
  ORDER BY "start" DESC)
, "100" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '100'
  ORDER BY "start" DESC)
, "103" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '103'
  ORDER BY "start" DESC)
, "101" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '101'
  ORDER BY "start" DESC)
, "83" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '83'
  ORDER BY "start" DESC)
, "85" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '85'
  ORDER BY "start" DESC)
, "119/120" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position IN ('119','120')
  ORDER BY "start" DESC)
, "87" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '87'
  ORDER BY "start" DESC)
, "28" AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "МО" ORDER BY "start" DESC) row_num
  FROM appointments app
  WHERE frmr_position = '28'
  ORDER BY "start" DESC)
, "all" AS (
  SELECT * FROM "110" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "59" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "49" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "122" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "13" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "54" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "53" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "100" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "103" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "101" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "83" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "85" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "119/120" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "87" WHERE row_num <= 10
  UNION ALL
  SELECT * FROM "28" WHERE row_num <= 10)
SELECT  "МО"
  , row_num
  , "Специальность врача"
  , "ФИО врача"
  , "Время приема записи"
  , patient_fio(p.resource) "ФИО пациента"
  , identifier_value(p.resource, 'urn:identity:enp:Patient') "Номер полиса ОМС"
  , "Дата закрытия апоинтмента"
FROM "all"
JOIN patient p
  ON p.id = patient_id
ORDER BY "МО", "Специальность врача", row_num