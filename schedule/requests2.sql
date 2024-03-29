---- 1 ----
WITH grouped AS (
  SELECT prr.resource #>> '{derived,moshort}' org_name
    , prr.resource #>> '{code,0,text}' "role"   
    , concat(pr.resource#>>'{name,0,family}', ' ', regexp_replace((pr.resource#>>'{name,0,given,0}'), '([�-�])+', '.', 'g'), ' ', regexp_replace((pr.resource#>>'{name,0,given,1}'), '([�-�])+', '.', 'g')) doctor 
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
  FROM practitionerrole prr  
  LEFT JOIN schedulerule s ON s.resource @@ concat('availableTime.#.channel.#($="web") and actor.#(resourceType="PractitionerRole" and id="', prr.id, '")')::jsquery
    AND immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')  
  WHERE prr.resource @@ 'code.#.text in ("���� ����� ��������(�������� ����)","����-������� ����������","����-�������� ����������","����-������-���������","����-����������������","����-�������������","����-�����������������","����-�����������","����-�������� ����������","����-�������� ������� ����������","����-�������� ������������ ����������","����-�����������-�������","����-������","����-������","����-������ �������","����-����������","����-���������� �������","����-����������-��������")'::jsquery 
    AND COALESCE(prr.resource->'active', 'true') = 'true'
  GROUP BY 1,2,3)
SELECT g.org_name "�������� ��", '������ � ����� ����� ����' "������", g."role" "���������", g.doctor "����/�������", g.activity "����������"
FROM grouped g 
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
  FROM practitionerrole prr  
  LEFT JOIN schedulerule s ON s.resource @@ concat('actor.#(resourceType="PractitionerRole" and id="', prr.id, '")')::jsquery
    AND immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
  JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    AND h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="4000")'::jsquery  
  WHERE prr.resource @@ 'code.#.text in ("���� ����� ��������(�������� ����)","����-������� ����������","����-�������� ����������","����-������-���������","����-����������������","����-�������������","����-�����������������","����-�����������","����-�������� ����������","����-�������� ������� ����������","����-�������� ������������ ����������","����-�����������-�������","����-������","����-������","����-������ �������","����-����������","����-���������� �������","����-����������-��������")'::jsquery 
    AND COALESCE(prr.resource->'active', 'true') = 'true'
  GROUP BY 1,2)
SELECT o.resource #>> '{alias,0}' "�������� ��", '������ �� ����������' "������", g.loc_display "����/�������", g.activity "����������"
FROM grouped g 
JOIN organization o ON o.id = g.org_id
ORDER BY 1,3;

---- 3 ----
WITH grouped AS (
  SELECT s.resource #>> '{mainOrganization,id}' org_id
    , prr.resource #>> '{code,0,text}' "role"
    , concat(pr.resource#>>'{name,0,family}', ' ', regexp_replace((pr.resource#>>'{name,0,given,0}'), '([�-�])+', '.', 'g'), ' ', regexp_replace((pr.resource#>>'{name,0,given,1}'), '([�-�])+', '.', 'g')) doctor 
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
    AND prr.resource @@ 'code.#.text in ("���� ����� ��������(�������� ����)","����-������� ����������","����-�������� ����������","����-������-���������","����-����������������","����-�������������","����-�����������������","����-�����������","����-�������� ����������","����-�������� ������� ����������","����-�������� ������������ ����������","����-�����������-�������","����-������","����-������","����-������ �������","����-����������","����-���������� �������","����-����������-��������")'::jsquery
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')  
  JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    AND h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="153")'::jsquery
  WHERE immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
    AND s.resource @@ 'availableTime.#.channel.#($="web")'::jsquery
  GROUP BY 1,2,3)
SELECT o.resource #>> '{alias,0}' "�������� ��", '����� ����� �� ���' "������", g."role" "���������", g.doctor "����/�������", g.activity "����������"
FROM grouped g 
JOIN organization o ON o.id = g.org_id
ORDER BY 1,4;

---- 4 ----
WITH grouped AS (
  SELECT s.resource #>> '{mainOrganization,id}' org_id
    , prr.resource #>> '{code,0,text}' "role"
    , concat(pr.resource#>>'{name,0,family}', ' ', regexp_replace((pr.resource#>>'{name,0,given,0}'), '([�-�])+', '.', 'g'), ' ', regexp_replace((pr.resource#>>'{name,0,given,1}'), '([�-�])+', '.', 'g')) doctor 
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
    AND prr.resource @@ 'code.#.text in ("���� ����� ��������(�������� ����)","����-������� ����������","����-�������� ����������","����-������-���������","����-����������������","����-�������������","����-�����������������","����-�����������","����-�������� ����������","����-�������� ������� ����������","����-�������� ������������ ����������","����-�����������-�������","����-������","����-������","����-������ �������","����-����������","����-���������� �������","����-����������-��������")'::jsquery
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')  
  JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
    AND h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="999")'::jsquery
  WHERE immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
    AND s.resource @@ 'availableTime.#.channel.#($="web")'::jsquery
  GROUP BY 1,2,3)
SELECT o.resource #>> '{alias,0}' "�������� ��", '����� ����� �� ���' "������", g."role" "���������", g.doctor "����/�������", g.activity "����������"
FROM grouped g 
JOIN organization o ON o.id = g.org_id
ORDER BY 1,4;