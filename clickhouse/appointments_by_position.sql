WITH with_position_name AS (
  SELECT multiIf(
      cp.c_code = '110', 'врач-терапевт участковый',
      cp.c_code = '49', 'врач общей практики (семейный врач)',
      cp.c_code = '122', 'врач-хирург',
      cp.c_code = '54', 'врач-офтальмолог',
      cp.c_code = '53', 'врач-оториноларинголог',
      cp.c_code = '13', 'врач-акушер-гинеколог',
      cp.c_code = '87', 'врач-психиатр-нарколог',
      cp.c_code IN ('119', '120'), 'врач-фтизиатр',
      cp.c_code = '100', 'врач-стоматолог',
      cp.c_code = '103', 'врач-стоматолог-терапевт',
      cp.c_code = '59', 'врач-педиатр участковый',
      cp.c_code = '28', 'врач-детский хирург',
      cp.c_code = '101', 'врач-стоматолог детский',
      cp.c_code IN ('83', '85'), 'врач-психиатр детский (подростковый)',
      cp.c_code = '109', 'врач-терапевт',
      cp.c_code = '58', 'врач-педиатр',
      cp.c_code IN ('195', '144', '146', '334', '335', '145', '345'), 'фельдшер',
      'other')  position_name
    , ca.c_orig_id id
    , ca.d_start "start"
    , ca.d_meta_createdat "created"
    , ca.c_servicetype_code service
    , ca.c_appointmenttype_code appointment_type
  FROM visiology.cd_appointment ca
    JOIN visiology.cs_practitionerroles cp
      ON cp.c_orig_id = ca.c_participant_practitionerRole
     AND cp.c_code IN ('110', '49', '122', '54', '53', '13', '87', '119', '120', '100', '103', '59', '28', '101', '83', '85', '109', '58', '195', '144', '146', '334', '335', '145', '345')
  WHERE ca.d_start >= '2024-01-29'
    AND ca.d_start < '2024-02-05'
    AND ca.c_status = 'arrived')  
, grouped AS (
  SELECT position_name
    , count(DISTINCT app.id) count_app
    , AVG(((toDateTime(app."start") - toDateTime(app."created")) / 60 / 60 / 24)) avg_day
    , count(DISTINCT app.id) FILTER (WHERE NOT app.service IN ('153', '999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199')) count_app_poly_all
    , count(DISTINCT app.id) FILTER (WHERE NOT app.service IN ('153', '999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND appointment_type IN ('WALKIN', 'EMERGENCY')) count_app_poly_walkin
    , count(DISTINCT app.id) FILTER (WHERE NOT app.service IN ('153', '999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND appointment_type = 'ROUTINE' AND service IN ('184', '185', '186', '187', '188', '189', '199')) count_app_poly_routine
    , count(DISTINCT app.id) FILTER (WHERE app.service IN ('999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199')) count_app_prof_all
    , count(DISTINCT app.id) FILTER (WHERE app.service IN ('999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND service = '3000') count_app_prof_walkin
    , count(DISTINCT app.id) FILTER (WHERE app.service IN ('999', '3000', '173', '195', '184', '185', '186', '187', '188', '189', '199') AND service IN ('3000', '999')) count_app_prof_routine
    , count(DISTINCT app.id) FILTER (WHERE app.service = '153') count_app_home_all
  FROM with_position_name app
  GROUP BY position_name)
SELECT position_name "Должность"
  , count_app "Столбец C"
  , round(avg_day, 1) "Столбец D"
  , count_app_poly_all "Столбец E"
  , count_app_poly_walkin "Столбец F"
  , count_app_poly_routine "Столбец G"
  , count_app_prof_all "Столбец H"
  , count_app_prof_walkin "Столбец I"
  , count_app_prof_routine "Столбец J"
  , count_app_home_all "Столбец K"
  , count_app_home_all "Столбец L"
FROM grouped
ORDER BY position_name
SETTINGS final = 1;