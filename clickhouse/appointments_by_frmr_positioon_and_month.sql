WITH "totals" AS
(
  SELECT multiIf(
  c_mainorganization <> c_authororganization AND NOT c_from = 'web', 'other',
  c_from) "from"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '110') "врач-терапевт участковый"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '49') "врач общей практики (семейный врач)"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '122') "врач-хирург"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '54') "врач-офтальмолог"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '53') "врач-оториноларинголог"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '13') "врач-акушер-гинеколог"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '87') "врач-психиатр-нарколог"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '119') "врач-фтизиатр"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '100') "врач-стоматолог"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '103') "врач-стоматолог-терапевт"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '59') "врач-педиатр участковый"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '28') "врач-детский хирург"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '101') "врач-стоматолог детский"
    , COUNT(DISTINCT a.c_orig_id) FILTER (WHERE p.c_code = '83') "врач-психиатр детский (подростковый)"
    , COUNT(DISTINCT a.c_orig_id) "Всего"
  FROM visiology.cd_appointment a
  JOIN visiology.cs_practitionerroles p 
    ON p.c_orig_id = a.c_participant_practitionerRole
   AND p.c_code IN ('13', '49', '53', '54', '59', '83', '87', '100', '101', '103', '110', '119', '122', '28')
   AND a.c_from IS NOT NULL 
  WHERE (d_start >= CAST('2024-01-01' AS datetime))
  AND   (d_start < CAST('2024-06-01T00:00:00' AS datetime))
  GROUP BY "from"
) 
, with_sum AS (
SELECT *
FROM "totals"
UNION ALL 
SELECT 'Всего' "from"
  , SUM("врач-терапевт участковый") "врач-терапевт участковый"
  , SUM("врач общей практики (семейный врач)") "врач общей практики (семейный врач)"
  , SUM("врач-хирург") "врач-хирург"
  , SUM("врач-офтальмолог") "врач-офтальмолог"
  , SUM("врач-оториноларинголог") "врач-оториноларинголог"
  , SUM("врач-акушер-гинеколог") "врач-акушер-гинеколог"
  , SUM("врач-психиатр-нарколог") "врач-психиатр-нарколог"
  , SUM("врач-фтизиатр") "врач-фтизиатр"
  , SUM("врач-стоматолог") "врач-стоматолог"
  , SUM("врач-стоматолог-терапевт") "врач-стоматолог-терапевт"
  , SUM("врач-педиатр участковый") "врач-педиатр участковый"
  , SUM("врач-детский хирург") "врач-детский хирург"
  , SUM("врач-стоматолог детский") "врач-стоматолог детский"
  , SUM("врач-психиатр детский (подростковый)") "врач-психиатр детский (подростковый)"
  , SUM("Всего") "Всего"
FROM "totals")
SELECT multiIf(
      "from" = 'doctor', 'Врач',
      "from" = 'fap', 'ФАП/ФП',
      "from" = 'kc', 'Колл-центр',
      "from" = 'medNurse', 'Медсестра',
      "from" = 'reg', 'Регистратура',
      "from" = 'tmk-online', 'ТМК',
      "from" = 'web', 'ЕПГУ',
      "from" = 'other', 'Др. МО',
      'Всего') as "Источник"
  , "врач-терапевт участковый"
  , "врач общей практики (семейный врач)"
  , "врач-хирург"
  , "врач-офтальмолог"
  , "врач-оториноларинголог"
  , "врач-акушер-гинеколог"
  , "врач-психиатр-нарколог"
  , "врач-фтизиатр"
  , "врач-стоматолог"
  , "врач-стоматолог-терапевт"
  , "врач-педиатр участковый"
  , "врач-детский хирург"
  , "врач-стоматолог детский"
  , "врач-психиатр детский (подростковый)"
  , "Всего"  
FROM with_sum
ORDER BY "from";