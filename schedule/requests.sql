﻿---- 1 ----
WITH grouped AS (
  SELECT s.resource #>> '{mainOrganization,id}' org_id
    , prr.resource #>> '{code,0,text}' "role"
    , concat(pr.resource#>>'{name,0,family}', ' ', regexp_replace((pr.resource#>>'{name,0,given,0}'), '([а-¤])+', '.', 'g'), ' ', regexp_replace((pr.resource#>>'{name,0,given,1}'), '([а-¤])+', '.', 'g')) doctor 
    , jsonb_build_object((current_date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb(current_date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '1 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '1 day')::date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '2 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '2 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '3 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '3 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '4 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '4 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '5 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '5 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '6 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '6 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '7 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '7 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '8 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '8 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '9 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '9 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '10 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '10 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '11 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '11 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '12 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '12 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '13 day')::date::text), CASE WHEN (count(prr.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '13 day')::date))) > 0 THEN '+' ELSE '-' END                        
                        ) activity
  FROM schedulerule s
  JOIN practitionerrole prr ON prr.id = jsonb_path_query_first(s.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
    AND prr.resource @@ 'code.#.text in ("Врач общей практики(семейный врач)","Врач-педиатр участковый","Врач-терапевт участковый","Врач-акушер-гинеколог","Врач-дерматовенеролог","Врач-колопроктолог","Врач-оториноларинголог","Врач-офтальмолог","Врач-психиатр участковый","Врач-психиатр детский участковый","Врач-психиатр подростковый участковый","Врач-травматолог-ортопед","Врач-уролог","Врач-хирург","Врач-хирург детский","Врач-стоматолог","Врач-стоматолог детский","Врач-стоматолог-терапевт")'::jsquery
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')  
  WHERE immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
    AND s.resource @@ 'availableTime.#.channel.#($="web")'::jsquery
  GROUP BY 1,2,3)
SELECT o.resource #>> '{alias,0}' "Название МО", 'запись к врачу через ЕПГУ"', "Услуга", g."role" "Должность", g.doctor "Врач/кабинет", g.activity "Активность"
FROM grouped g 
JOIN organization o ON o.id = g.org_id
ORDER BY 1,4;

---- 2 ----
WITH grouped AS (
  SELECT s.resource #>> '{mainOrganization,id}' org_id
    , s.resource #>> '{location,display}' loc_display 
    , jsonb_build_object((current_date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb(current_date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '1 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '1 day')::date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '2 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '2 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '3 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '3 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '4 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '4 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '5 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '5 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '6 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '6 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '7 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '7 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '8 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '8 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '9 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '9 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '10 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '10 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '11 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '11 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '12 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '12 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '13 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '13 day')::date))) > 0 THEN '+' ELSE '-' END                        
                        ) activity
  FROM schedulerule s
  JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    AND h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="4000")'::jsquery
  WHERE immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
    AND s.resource @@ 'availableTime.#.channel.#($="web")'::jsquery
  GROUP BY 1,2)
SELECT o.resource #>> '{alias,0}' "Название МО", 'запись на вакцинацию' "Услуга", g.loc_display "Врач/кабинет", g.activity "Активность"
FROM grouped g 
JOIN organization o ON o.id = g.org_id
ORDER BY 1,3;

---- 3 ----
WITH grouped AS (
  SELECT s.resource #>> '{mainOrganization,id}' org_id
    , prr.resource #>> '{code,0,text}' "role"
    , concat(pr.resource#>>'{name,0,family}', ' ', regexp_replace((pr.resource#>>'{name,0,given,0}'), '([а-¤])+', '.', 'g'), ' ', regexp_replace((pr.resource#>>'{name,0,given,1}'), '([а-¤])+', '.', 'g')) doctor 
    , jsonb_build_object((current_date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb(current_date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '1 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '1 day')::date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '2 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '2 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '3 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '3 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '4 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '4 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '5 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '5 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '6 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '6 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '7 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '7 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '8 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '8 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '9 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '9 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '10 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '10 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '11 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '11 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '12 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '12 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '13 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '13 day')::date))) > 0 THEN '+' ELSE '-' END                        
                        ) activity
  FROM schedulerule s
  JOIN practitionerrole prr ON prr.id = jsonb_path_query_first(s.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
    AND prr.resource @@ 'code.#.text in ("Врач общей практики(семейный врач)","Врач-педиатр участковый","Врач-терапевт участковый","Врач-акушер-гинеколог","Врач-дерматовенеролог","Врач-колопроктолог","Врач-оториноларинголог","Врач-офтальмолог","Врач-психиатр участковый","Врач-психиатр детский участковый","Врач-психиатр подростковый участковый","Врач-травматолог-ортопед","Врач-уролог","Врач-хирург","Врач-хирург детский","Врач-стоматолог","Врач-стоматолог детский","Врач-стоматолог-терапевт")'::jsquery
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')  
  JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    AND h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="153")'::jsquery
  WHERE immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
    AND s.resource @@ 'availableTime.#.channel.#($="web")'::jsquery
  GROUP BY 1,2,3)
SELECT o.resource #>> '{alias,0}' "Название МО", 'вызов врача на дом' "Услуга", g."role" "Должность", g.doctor "Врач/кабинет", g.activity "Активность"
FROM grouped g 
JOIN organization o ON o.id = g.org_id
ORDER BY 1,4;

---- 4 ----
WITH grouped AS (
  SELECT s.resource #>> '{mainOrganization,id}' org_id
    , prr.resource #>> '{code,0,text}' "role"
    , concat(pr.resource#>>'{name,0,family}', ' ', regexp_replace((pr.resource#>>'{name,0,given,0}'), '([а-¤])+', '.', 'g'), ' ', regexp_replace((pr.resource#>>'{name,0,given,1}'), '([а-¤])+', '.', 'g')) doctor 
    , jsonb_build_object((current_date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb(current_date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '1 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '1 day')::date))) > 0 THEN '+' ELSE '-' END
                        ,((current_date + INTERVAL '2 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '2 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '3 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '3 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '4 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '4 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '5 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '5 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '6 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '6 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '7 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '7 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '8 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '8 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '9 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '9 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '10 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '10 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '11 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '11 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '12 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '12 day')::date))) > 0 THEN '+' ELSE '-' END 
                        ,((current_date + INTERVAL '13 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '13 day')::date))) > 0 THEN '+' ELSE '-' END                        
                        ) activity
  FROM schedulerule s
  JOIN practitionerrole prr ON prr.id = jsonb_path_query_first(s.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
    AND prr.resource @@ 'code.#.text in ("Врач общей практики(семейный врач)","Врач-педиатр участковый","Врач-терапевт участковый","Врач-акушер-гинеколог","Врач-дерматовенеролог","Врач-колопроктолог","Врач-оториноларинголог","Врач-офтальмолог","Врач-психиатр участковый","Врач-психиатр детский участковый","Врач-психиатр подростковый участковый","Врач-травматолог-ортопед","Врач-уролог","Врач-хирург","Врач-хирург детский","Врач-стоматолог","Врач-стоматолог детский","Врач-стоматолог-терапевт")'::jsquery
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')  
  JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    AND h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery
  WHERE immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
    AND s.resource @@ 'availableTime.#.channel.#($="web")'::jsquery
  GROUP BY 1,2,3)
SELECT o.resource #>> '{alias,0}' "Название МО", 'вызов врача на дом' "Услуга", g."role" "Должность", g.doctor "Врач/кабинет", g.activity "Активность"
FROM grouped g 
JOIN organization o ON o.id = g.org_id
ORDER BY 1,4;