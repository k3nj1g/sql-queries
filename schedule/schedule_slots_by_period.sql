WITH filtered AS (
  SELECT DISTINCT ON (sch_id) 
    s.id sch_id
    , s.resource sch_resource
    , prr.resource prr_resource
  FROM practitionerrole prr
    JOIN schedulerule s
      ON s.resource @@ concat('actor.#(resourceType="PractitionerRole" and id="',prr.id,'")')::jsquery
     AND immutable_tsrange(s.resource #>> '{planningHorizon,start}', COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && tsrange('2022-12-05', '2022-12-18', '[]')
  WHERE prr.resource @@ 'code.#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("110", "59", "49", "122","13","54","53","100","103","101","83","85","119","120","87","28"))'::jsquery
    AND coalesce(prr.resource -> 'active','true') = 'true')
, "all" AS (
  SELECT 
    count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '13') AS "Акушер-гинеколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '28') AS "Детский хирург"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '49') AS "Врач общей практики (семейный)"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '53') AS "Оториноларинголог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '54') AS "Офтальмолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '59') AS "Педиатр участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('83','85')) AS "Психиатр детский + подростковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '87') AS "Психиатр-нарколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '100') AS "Стоматолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '101') AS "Стоматолог детский"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '103') AS "Стоматолог-терапевт"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '110') AS "Терапевт участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('119','120')) AS "Фтизиатр"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '122') AS "Хирург"
    , count(*) "all"
  FROM filtered
)
, "all_epgu" AS (
  SELECT 
    count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '13') AS "Акушер-гинеколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '28') AS "Детский хирург"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '49') AS "Врач общей практики (семейный)"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '53') AS "Оториноларинголог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '54') AS "Офтальмолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '59') AS "Педиатр участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('83','85')) AS "Психиатр детский + подростковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '87') AS "Психиатр-нарколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '100') AS "Стоматолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '101') AS "Стоматолог детский"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '103') AS "Стоматолог-терапевт"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '110') AS "Терапевт участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('119','120')) AS "Фтизиатр"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '122') AS "Хирург"
    , count(*) "all"
  FROM filtered
  WHERE sch_resource @@ 'availableTime.#.channel.#($="web")'::jsquery    
)
, "free" AS (
  SELECT 
    count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '13') AS "Акушер-гинеколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '28') AS "Детский хирург"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '49') AS "Врач общей практики (семейный)"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '53') AS "Оториноларинголог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '54') AS "Офтальмолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '59') AS "Педиатр участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('83','85')) AS "Психиатр детский + подростковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '87') AS "Психиатр-нарколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '100') AS "Стоматолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '101') AS "Стоматолог детский"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '103') AS "Стоматолог-терапевт"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '110') AS "Терапевт участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('119','120')) AS "Фтизиатр"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '122') AS "Хирург"
    , count(*) "all"
  FROM filtered
  WHERE schedule_range_free_slot(sch_id, '2022-12-05', '2022-12-18') IS NOT NULL
)
, "free_today" AS (
  SELECT 
    count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '13') AS "Акушер-гинеколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '28') AS "Детский хирург"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '49') AS "Врач общей практики (семейный)"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '53') AS "Оториноларинголог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '54') AS "Офтальмолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '59') AS "Педиатр участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('83','85')) AS "Психиатр детский + подростковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '87') AS "Психиатр-нарколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '100') AS "Стоматолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '101') AS "Стоматолог детский"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '103') AS "Стоматолог-терапевт"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '110') AS "Терапевт участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('119','120')) AS "Фтизиатр"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '122') AS "Хирург"
    , count(*) "all"
  FROM filtered
  WHERE schedule_range_free_slot(sch_id, '2022-12-05', '2022-12-05') IS NOT NULL
)
, "free_web" AS (
  SELECT
    count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '13') AS "Акушер-гинеколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '28') AS "Детский хирург"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '49') AS "Врач общей практики (семейный)"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '53') AS "Оториноларинголог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '54') AS "Офтальмолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '59') AS "Педиатр участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('83','85')) AS "Психиатр детский + подростковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '87') AS "Психиатр-нарколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '100') AS "Стоматолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '101') AS "Стоматолог детский"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '103') AS "Стоматолог-терапевт"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '110') AS "Терапевт участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('119','120')) AS "Фтизиатр"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '122') AS "Хирург"
    , count(*) "all"
  FROM filtered
  WHERE schedule_range_free_slot(sch_id, '2022-12-05', '2022-12-18', 'web') IS NOT NULL
)
, "free_web_today" AS (
  SELECT
    count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '13') AS "Акушер-гинеколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '28') AS "Детский хирург"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '49') AS "Врач общей практики (семейный)"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '53') AS "Оториноларинголог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '54') AS "Офтальмолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '59') AS "Педиатр участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('83','85')) AS "Психиатр детский + подростковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '87') AS "Психиатр-нарколог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '100') AS "Стоматолог"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '101') AS "Стоматолог детский"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '103') AS "Стоматолог-терапевт"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '110') AS "Терапевт участковый"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' IN ('119','120')) AS "Фтизиатр"
    , count(*) FILTER (WHERE jsonb_path_query_first(prr_resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' = '122') AS "Хирург"
    , count(*) "all"
  FROM filtered
  WHERE schedule_range_free_slot(sch_id, '2022-12-05', '2022-12-05', 'web') IS NOT NULL
)
, unioned as (
  SELECT 'Количество врачей, на которых заведено расписание в ГИСЗ субъекта РФ (по всем источникам) в течении 14 дней' "Показатель", *, 1 AS "order"
  FROM "all"
  UNION ALL
  SELECT 'Количество врачей, у которых заведено расписание в ГИСЗ субъекта РФ для записи через ЕПГУ в течение 14 дней' "Показатель", *, 2 AS "order"
  FROM "all_epgu"
  UNION ALL
  SELECT 'Количество врачей, у которых есть свободные слоты по всем источникам в течение 14 дней' "Показатель", *, 3 AS "order"
  FROM "free"
  UNION ALL
  SELECT 'Количество врачей, у которых есть свободные слоты по всем источникам в день обращения' "Показатель", *, 4 AS "order"
  FROM "free_today"
  UNION ALL
  SELECT 'Количество врачей, у которых есть свободные слоты для записи через ЕПГУ в течение 14 дней' "Показатель", *, 5 AS "order"
  FROM "free_web"
  UNION ALL
  SELECT 'Количество врачей, у которых есть свободные слоты для записи через ЕПГУ в день обращения' "Показатель", *, 6 AS "order"
  FROM "free_web_today")
SELECT *
    -- sum("Акушер-гинеколог") "Акушер-гинеколог"
    -- , sum("Детский хирург") "Детский хирург"
    -- , sum("Врач общей практики (семейный)") "Врач общей практики (семейный)"
    -- , sum("Оториноларинголог") "Оториноларинголог"
    -- , sum("Офтальмолог") "Офтальмолог"
    -- , sum("Педиатр участковый") "Педиатр участковый"
    -- , sum("Психиатр детский + подростковый") "Психиатр детский + подростковый"
    -- , sum("Психиатр-нарколог") "Психиатр-нарколог"
    -- , sum("Стоматолог") "Стоматолог"
    -- , sum("Стоматолог детский") "Стоматолог детский"
    -- , sum("Стоматолог-терапевт") "Стоматолог-терапевт"
    -- , sum("Терапевт участковый") "Терапевт участковый"
    -- , sum("Фтизиатр") "Фтизиатр"
    -- , sum("Хирург") "Хирург"
    -- , sum("all") "all"
FROM unioned
ORDER BY "order";