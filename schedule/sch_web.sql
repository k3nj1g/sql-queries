WITH grouped AS (
  SELECT prr.resource #>> '{derived,moshort}' org_name
    , prr.resource #>> '{code,0,text}' "role"   
    , concat(pr.resource#>>'{name,0,family}', ' ', pr.resource#>>'{name,0,given,0}', ' ', pr.resource#>>'{name,0,given,1}') doctor 
    , jsonb_build_object((current_date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb(current_date))) > 0 THEN 'Да' ELSE 'Нет' END
                        ,((current_date + INTERVAL '1 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '1 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END
                        ,((current_date + INTERVAL '2 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '2 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '3 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '3 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '4 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '4 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '5 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '5 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '6 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '6 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '7 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '7 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '8 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '8 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '9 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '9 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '10 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '10 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '11 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '11 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '12 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '12 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END 
                        ,((current_date + INTERVAL '13 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '13 day')::date))) > 0 THEN 'Да' ELSE 'Нет' END                        
                        ) activity
  FROM practitionerrole prr  
  LEFT JOIN schedulerule s ON s.resource @@ concat('availableTime.#.channel.#($="web") and actor.#(resourceType="PractitionerRole" and id="', prr.id, '")')::jsquery
    AND immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
--  JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization', 'and type.#.coding.#(system="urn:CodeSystem:frmo.structure-type" and code="1")')
  WHERE prr.resource @@ 'code.#.text in ("Заведующий фельдшерско-акушерским пунктом - акушерка","Заведующий фельдшерско-акушерским пунктом - медицинская сестра", "Заведующий фельдшерско-акушерским пунктом - фельдшер","Фельдшер","Врач общей практики (семейный врач)","Врач-педиатр участковый","Врач-терапевт участковый","Врач-акушер-гинеколог","Врач-дерматовенеролог","Врач-колопроктолог","Врач-оториноларинголог","Врач-офтальмолог","Врач-психиатр участковый","Врач-психиатр детский участковый","Врач-психиатр подростковый участковый","Врач-травматолог-ортопед","Врач-уролог","Врач-хирург","Врач-детский хирург","Врач-стоматолог","Врач-стоматолог детский","Врач-стоматолог-терапевт")'::jsquery 
    AND COALESCE(prr.resource->'active', 'true') = 'true'
  GROUP BY 1,2,3)
SELECT g.org_name "Название МО", 'запись к врачу через ЕПГУ' "Услуга", g."role" "Должность", g.doctor "Врач/кабинет", g.activity "Активность"
FROM grouped g 
ORDER BY 1,4;