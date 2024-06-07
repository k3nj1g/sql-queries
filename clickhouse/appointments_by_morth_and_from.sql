WITH "totals" AS
(
  SELECT toMonth(a.d_start) AS "month"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'medNurse') AS "Медсестра"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'doctor') AS "Врач"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'fap') AS "ФАП/ФП"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'reg') AS "Регистратура"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'kc') AS "Колл центр"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'web') AS "ЕПГУ"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_from = 'tmk-online') AS "ТМК Онлайн"
    , COUNT(DISTINCT c_orig_id) FILTER(WHERE c_mainorganization <> c_authororganization AND NOT c_from = 'web') AS "Др. МО"
  FROM visiology.cd_appointment a
  JOIN visiology.cs_practitionerroles p 
    ON p.c_orig_id = a.c_participant_practitionerRole
   AND p.c_code IN ('66','121','114','89','81','57','31','368','367','366','100','62','37','999','408','407','406','381','130','129','128','127','126','125','124','123','122','120','119','118','117','116','115','113','112','111','110','109','108','107','106','105','104','103','102','101','99','98','97','96','95','94','93','92','91','90','88','87','86','85','84','83','82','80','79','78','77','76','75','74','73','72','71','70','69','68','67','65','64','63','61','60','59','58','56','55','54','53','52','51','50','49','48','47','46','45','44','43','42','41','40','39','38','36','35','34','33','32','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13')
   AND a.c_from IS NOT NULL 
  WHERE (d_start >= CAST('2024-01-01' AS datetime))
  AND   (d_start < CAST('2024-06-01T00:00:00' AS datetime))
  GROUP BY "month")
SELECT multiIf(
        "month" = 1, 'Январь',
        "month" = 2, 'Февраль',
        "month" = 3, 'Март',
        "month" = 4, 'Апрель',
        "month" = 5, 'Май',
        '-') AS "Месяц"
  , t."Медсестра"
  , t."Врач"
  , t."ФАП/ФП"
  , t."Регистратура"
  , t."Колл центр"
  , t."ЕПГУ"
  , t."ТМК Онлайн"
  , t."Др. МО"
  , IFNULL(t."Медсестра" + t."Врач" + t."ФАП/ФП" + t."Регистратура" + t."Колл центр" + t."ЕПГУ" + t."Др. МО", 0) AS "Всего"
FROM "totals" t;