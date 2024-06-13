WITH appointments AS (
  SELECT resource #>> '{mainOrganization,id}' AS mo_id
    , jsonb_path_exists(resource, '$ ? (@.from=="web" && @.basedOn.resourceType=="ServiceRequest")') AS type_1
    , jsonb_path_exists(resource, '$ ? (@.from!="web" && @.basedOn.resourceType=="ServiceRequest")') AS type_2
    , EXTRACT(MONTH FROM CAST((resource #>> '{start}') AS date)) AS month
  FROM appointment
  WHERE ((resource #>> '{start}') >= '2024-01-01'
    AND ((resource #>> '{start}') < '2024-05-01')))
, grouped AS (
  SELECT mo_id
    , count(*) FILTER (WHERE type_1 AND month = 1) AS epgu_1
    , count(*) FILTER (WHERE type_1 AND month = 2) AS epgu_2
    , count(*) FILTER (WHERE type_1 AND month = 3) AS epgu_3
    , count(*) FILTER (WHERE type_1 AND month = 4) AS epgu_4
    , count(*) FILTER (WHERE type_2 AND month = 1) AS total_1
    , count(*) FILTER (WHERE type_2 AND month = 2) AS total_2
    , count(*) FILTER (WHERE type_2 AND month = 3) AS total_3
    , count(*) FILTER (WHERE type_2 AND month = 4) AS total_4
  FROM appointments
  GROUP BY mo_id)
, resolve_org_name AS (
  SELECT org.resource #>> '{alias,0}' AS "Наименование МО"
    , epgu_1::text AS "Запись через ЕПГУ(1)" 
    , epgu_2::text AS "Запись через ЕПГУ(2)" 
    , epgu_3::text AS "Запись через ЕПГУ(3)"
    , epgu_4::text AS "Запись через ЕПГУ(4)"
    , total_1::text AS "Всего записей(1)" 
    , total_2::text AS "Всего записей(2)" 
    , total_3::text AS "Всего записей(3)"
    , total_4::text AS "Всего записей(4)"
  FROM grouped
  JOIN organization org
    ON org.id = mo_id
  ORDER BY "Наименование МО")
SELECT 'Месяц' AS "Наименование МО"
  , 'январь' AS "Запись через ЕПГУ(1)" 
  , 'февраль' AS "Запись через ЕПГУ(2)" 
  , 'март' AS "Запись через ЕПГУ(3)"
  , 'апрель' AS "Запись через ЕПГУ(4)"
  , 'январь' AS "Всего записей(1)" 
  , 'февраль' AS "Всего записей(2)" 
  , 'март' AS "Всего записей(3)"
  , 'апрель' AS "Всего записей(4)"
UNION ALL  
SELECT *
FROM resolve_org_name;