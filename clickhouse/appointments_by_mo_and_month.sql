--- 1
WITH "totals" AS
(
  SELECT m.c_name_short AS mo_name
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 1) AS epgu_1
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 2) AS epgu_2
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 3) AS epgu_3
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 4) AS epgu_4
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_1
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_2
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_3
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_4
  FROM visiology.cd_appointment a
  JOIN visiology.cs_mo m ON m.zdravbox_id = a.c_mainorganization
  WHERE (d_start >= CAST('2024-01-01' AS datetime))
  AND   (d_start < CAST('2024-05-01T00:00:00' AS datetime))
  AND   (c_servicetype_code = '4000')
  GROUP BY mo_name)
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
SELECT mo_name AS "Наименование МО"
  , toString(epgu_1) AS "Запись через ЕПГУ(1)" 
  , toString(epgu_2) AS "Запись через ЕПГУ(2)" 
  , toString(epgu_3) AS "Запись через ЕПГУ(3)"
  , toString(epgu_4) AS "Запись через ЕПГУ(4)"
  , toString(total_1) AS "Всего записей(1)" 
  , toString(total_2) AS "Всего записей(2)" 
  , toString(total_3) AS "Всего записей(3)"
  , toString(total_4) AS "Всего записей(4)"
FROM "totals"
ORDER BY "Наименование МО";

--- 3
WITH "totals" AS
(
  SELECT m.c_name_short AS mo_name
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 1) AS epgu_1
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 2) AS epgu_2
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 3) AS epgu_3
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 4) AS epgu_4
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_1
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_2
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_3
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_4
  FROM visiology.cd_appointment a
  JOIN visiology.cs_mo m ON m.zdravbox_id = a.c_mainorganization
  WHERE (d_start >= CAST('2024-01-01' AS datetime))
  AND   (d_start < CAST('2024-05-01T00:00:00' AS datetime))
  AND   (c_servicetype_code IN ('184', '185', '186', '187', '188', '189', '199'))
  GROUP BY mo_name)
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
SELECT mo_name AS "Наименование МО"
  , toString(epgu_1) AS "Запись через ЕПГУ(1)" 
  , toString(epgu_2) AS "Запись через ЕПГУ(2)" 
  , toString(epgu_3) AS "Запись через ЕПГУ(3)"
  , toString(epgu_4) AS "Запись через ЕПГУ(4)"
  , toString(total_1) AS "Всего записей(1)" 
  , toString(total_2) AS "Всего записей(2)" 
  , toString(total_3) AS "Всего записей(3)"
  , toString(total_4) AS "Всего записей(4)"
FROM "totals"
--- 1
WITH "totals" AS
(
  SELECT m.c_name_short AS mo_name
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 1) AS epgu_1
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 2) AS epgu_2
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 3) AS epgu_3
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web' AND toMonth(a.d_start) = 4) AS epgu_4
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_1
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_2
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_3
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE toMonth(a.d_start) = 4) AS total_4
  FROM visiology.cd_appointment a
  JOIN visiology.cs_mo m ON m.zdravbox_id = a.c_mainorganization
  WHERE (d_start >= CAST('2024-01-01' AS datetime))
  AND   (d_start < CAST('2024-05-01T00:00:00' AS datetime))
  AND   (c_servicetype_code = '4000')
  GROUP BY mo_name)
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
SELECT mo_name AS "Наименование МО"
  , toString(epgu_1) AS "Запись через ЕПГУ(1)" 
  , toString(epgu_2) AS "Запись через ЕПГУ(2)" 
  , toString(epgu_3) AS "Запись через ЕПГУ(3)"
  , toString(epgu_4) AS "Запись через ЕПГУ(4)"
  , toString(total_1) AS "Всего записей(1)" 
  , toString(total_2) AS "Всего записей(2)" 
  , toString(total_3) AS "Всего записей(3)"
  , toString(total_4) AS "Всего записей(4)"
FROM "totals"
ORDER BY "Наименование МО";