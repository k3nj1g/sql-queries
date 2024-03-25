--- Postgresql 
CREATE TABLE public.practitoner_code_for_report 
(code TEXT, display text);

-- фактическое количество слотов за отчетную неделю 2024-02-19 - 2024-02-26
INSERT INTO public.practitoner_code_for_report 
VALUES 
('110', 'врач-терапевт участковый'),
('49', 'врач общей практики (семейный врач)'),
('122', 'врач-хирург'),
('54', 'врач-офтальмолог'),
('53', 'врач-оториноларинголог'),
('13', 'врач-акушер-гинеколог'),
('87', 'врач-психиатр-нарколог'),
('119', 'врач-фтизиатр'),
('120', 'врач-фтизиатр'),
('100', 'врач-стоматолог'),
('103', 'врач-стоматолог-терапевт'),
('59', 'врач-педиатр участковый'),
('28', 'врач-детский хирург'),
('101', 'врач-стоматолог детский'),
('83', 'врач-психиатр детский (подростковый)'),
('85', 'врач-психиатр детский (подростковый)');

WITH schedules AS (
  SELECT sch.id sch_id
    , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' doctor_position 
  FROM schedulerule sch
  JOIN public.organization org
    ON org.id = sch.resource #>> '{mainOrganization,id}'
      AND identifier_value(org.resource, 'urn:identity:oid:Organization') IN ('1.2.643.5.1.13.13.12.2.21.1532','1.2.643.5.1.13.13.12.2.21.1523','1.2.643.5.1.13.13.12.2.21.1533','1.2.643.5.1.13.13.12.2.21.1517','1.2.643.5.1.13.13.12.2.21.1515','1.2.643.5.1.13.13.12.2.21.1558','1.2.643.5.1.13.13.12.2.21.1518','1.2.643.5.1.13.13.12.2.21.1525','1.2.643.5.1.13.13.12.2.21.10417','1.2.643.5.1.13.13.12.2.21.1529','1.2.643.5.1.13.13.12.2.21.1563','1.2.643.5.1.13.13.12.2.21.1564','1.2.643.5.1.13.13.12.2.21.1556','1.2.643.5.1.13.13.12.2.21.1565','1.2.643.5.1.13.13.12.2.21.1502','1.2.643.5.1.13.13.12.2.21.1504','1.2.643.5.1.13.13.12.2.21.1505','1.2.643.5.1.13.13.12.2.21.1506','1.2.643.5.1.13.13.12.2.21.1534','1.2.643.5.1.13.13.12.2.21.1559','1.2.643.5.1.13.13.12.2.21.1541','1.2.643.5.1.13.13.12.2.21.1542','1.2.643.5.1.13.13.12.2.21.1508','1.2.643.5.1.13.13.12.2.21.1531','1.2.643.5.1.13.13.12.2.21.1509','1.2.643.5.1.13.13.12.2.21.1514','1.2.643.5.1.13.13.12.2.21.1562','1.2.643.5.1.13.13.12.2.21.1510','1.2.643.5.1.13.13.12.2.21.1555','1.2.643.5.1.13.13.12.2.21.1516','1.2.643.5.1.13.13.12.2.21.1511','1.2.643.5.1.13.13.12.2.21.1512')
  JOIN practitionerrole prr
    ON prr.id = jsonb_path_query_first(sch.resource, '$.actor ? (@.resourceType == "PractitionerRole").id') #>> '{}'
      AND (jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}') IN (SELECT code FROM public.practitoner_code_for_report)
  WHERE sch.resource @@ 'actor.#.resourceType="PractitionerRole"'::jsquery
    AND immutable_tsrange(sch.resource #>> '{planningHorizon,start}', COALESCE((sch.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) && immutable_tsrange('2024-02-19', '2024-02-26')) 
, slots AS (
  SELECT doctor_position
    , schedule_slots(sch_id, '2024-02-19', '2024-02-26') generated_slots 
  FROM schedules sch)
, slots_count AS (
  SELECT doctor_position
    , COALESCE(jsonb_array_length(generated_slots), 0) slots_count
  FROM slots)
, slots_grouped AS (
  SELECT doctor_position
    , sum(slots_count) slots_count
  FROM slots_count
  GROUP BY doctor_position)
SELECT practitioner.display, slots_count
FROM slots_grouped
LEFT JOIN public.practitoner_code_for_report practitioner
  ON practitioner.code = doctor_position;
  
-- Людей состоит в листе ожидания на 2024-02-26
SELECT count(*)
FROM public.task 
WHERE resource @@ 'code.coding.#.code = "waitingList"'::jsquery
  AND (knife_extract_max_timestamptz(resource, '[["authoredOn"]]')) < '2024-02-26'
  AND (
    (resource ->> 'status' <> 'completed') 
    OR 
    (resource ->> 'status' = 'completed') AND ts >= '2024-02-19');

--- Clickhouse
-- Средняя длительность ожидания приема за неделю 2024-02-19 - 2024-02-26
WITH practitoner_code_for_report AS (
  SELECT c1 code, c2 display
  FROM VALUES (
    ('110', 'врач-терапевт участковый'),
    ('49', 'врач общей практики (семейный врач)'),
    ('122', 'врач-хирург'),
    ('54', 'врач-офтальмолог'),
    ('53', 'врач-оториноларинголог'),
    ('13', 'врач-акушер-гинеколог'),
    ('87', 'врач-психиатр-нарколог'),
    ('119', 'врач-фтизиатр'),
    ('120', 'врач-фтизиатр'),
    ('100', 'врач-стоматолог'),
    ('103', 'врач-стоматолог-терапевт'),
    ('59', 'врач-педиатр участковый'),
    ('28', 'врач-детский хирург'),
    ('101', 'врач-стоматолог детский'),
    ('83', 'врач-психиатр детский (подростковый)'),
    ('85', 'врач-психиатр детский (подростковый)'))
), appointments AS (
  SELECT cs.code code
    , cs.display display
    , ca.d_start "start"
    , ca.d_meta_createdat "created"
  FROM visiology.cd_appointment ca
  JOIN visiology.cs_practitionerroles cp
      ON cp.c_orig_id = ca.c_participant_practitionerRole
  JOIN (SELECT * FROM practitoner_code_for_report) cs
    ON cs.code = cp.c_code
  WHERE ca.d_start >= '2024-02-19'
    AND ca.d_start < '2024-02-26'
    AND ca.c_status = 'arrived')
, grouped AS (
  SELECT code code
    , display display
    , AVG(((toDateTime(app."start") - toDateTime(app."created")) / 60 / 60 / 24)) avg_day
  FROM appointments app
  GROUP BY code, display)
SELECT code
  , display
  , round(avg_day, 1)
FROM grouped;