-- создаём новую таблицу
CREATE TABLE public.observation_new (
	id text NOT NULL,
	txid int8 NOT NULL,
	cts timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	ts timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	resource_type text DEFAULT 'Observation'::text NULL,
	status public."resource_status" NOT NULL,
	resource jsonb NOT NULL,
	CONSTRAINT observation_new_pkey PRIMARY KEY (id)
)
PARTITION BY RANGE (id);

-- создаём индексы на новую таблицу 
CREATE INDEX observation_new_cts__btree ON public.observation_new USING btree (cts);
CREATE INDEX observation_new_resource__gin_jsquery ON public.observation_new USING gin (resource jsonb_path_value_ops);
CREATE INDEX observation_new_resource_effective_datetime ON public.observation_new USING btree (((resource #>> '{effective,dateTime}'::text[])));
CREATE INDEX observation_new_resource_eoc__pregnant ON public.observation_new USING gin (((resource -> 'episodeOfCare'::text)) jsonb_path_value_ops) WHERE ((resource -> 'category'::text) @@ '#."coding".#("system" = "urn:CodeSystem:pregnancy" AND "code" = "current-pregnancy")'::jsquery);
CREATE INDEX observation_new_resource_identifier__gin ON public.observation_new USING gin (knife_extract_text(resource, '[["identifier", "value"]]'::jsonb));
CREATE INDEX observation_new_resource_period_patient_condition__gist ON public.observation_new USING gist (immutable_tsrange((resource #>> '{effective,Period,start}'::text[]), (resource #>> '{effective,Period,end}'::text[]), '[]'::text)) WHERE (resource @@ '("category".#."coding".#("system" = "urn:CodeSystem:observation-category" AND "code" = "patient-condition") AND "value"."CodeableConcept"."coding".#("system" = "urn:CodeSystem:1.2.643.5.1.13.13.11.1006" AND "code" IN ("3", "4", "6")))'::jsquery);
CREATE INDEX observation_new_resource_subject_ref_valid ON public.observation_new USING btree (enp_valid((resource #>> '{subject,identifier,value}'::text[]))) WHERE (resource @@ '"subject"."identifier"."system" = "urn:identity:insurance-gov:Patient"'::jsquery);
CREATE INDEX observation_new_ts__btree ON public.observation_new USING btree (ts);
CREATE INDEX observation_new_txid__btree ON public.observation_new USING btree (txid);

-- создаём партиции
CREATE TABLE observation_y2024before PARTITION OF observation_new
    FOR VALUES FROM ('00000000-0000-0000-0000-000000000000') 
	TO ('018cc251-f400-0000-0000-000000000000');
CREATE TABLE observation_y2024q1 PARTITION OF observation_new
    FOR VALUES FROM ('018cc251-f400-0000-0000-000000000000') 
	TO ('018e96f4-a800-0000-0000-000000000000');
CREATE TABLE observation_y2024q2 PARTITION OF observation_new
    FOR VALUES FROM ('018e96f4-a800-0000-0000-000000000000') 
	TO ('01906b97-5c00-0000-0000-000000000000');
CREATE TABLE observation_y2024q3 PARTITION OF observation_new
    FOR VALUES FROM ('01906b97-5c00-0000-0000-000000000000') 
	TO ('01924560-6c00-0000-0000-000000000000');
CREATE TABLE observation_y2024q4 PARTITION OF observation_new
    FOR VALUES FROM ('01924560-6c00-0000-0000-000000000000') 
	TO ('01941f29-7c00-0000-0000-000000000000');
CREATE TABLE observation_y2025q1 PARTITION OF observation_new
    FOR VALUES FROM ('01941f29-7c00-0000-0000-000000000000') 
	TO ('0195eea5-d400-0000-0000-000000000000');
CREATE TABLE observation_y2025q2 PARTITION OF observation_new
    FOR VALUES FROM ('0195eea5-d400-0000-0000-000000000000') 
	TO ('0197c348-8800-0000-0000-000000000000');
CREATE TABLE observation_y2025q3 PARTITION OF observation_new
    FOR VALUES FROM ('0197c348-8800-0000-0000-000000000000') 
	TO ('01999d11-9800-0000-0000-000000000000');
CREATE TABLE observation_y2025q4 PARTITION OF observation_new
    FOR VALUES FROM ('01999d11-9800-0000-0000-000000000000') 
	TO ('019b76da-a800-0000-0000-000000000000');	
CREATE TABLE observation_y2026q1 PARTITION OF observation_new
    FOR VALUES FROM ('019b76da-a800-0000-0000-000000000000') 
	TO ('019d4657-0000-0000-0000-000000000000');
CREATE TABLE observation_y2026q2 PARTITION OF observation_new
    FOR VALUES FROM ('019d4657-0000-0000-0000-000000000000') 
	TO ('019f1af9-b400-0000-0000-000000000000');
CREATE TABLE observation_y2026q3 PARTITION OF observation_new
    FOR VALUES FROM ('019f1af9-b400-0000-0000-000000000000') 
	TO ('01a0f4c2-c400-0000-0000-000000000000');
CREATE TABLE observation_y2026q4 PARTITION OF observation_new
    FOR VALUES FROM ('01a0f4c2-c400-0000-0000-000000000000') 
	TO ('01a2ce8b-d400-0000-0000-000000000000');
CREATE TABLE observation_y2027q1 PARTITION OF observation_new
    FOR VALUES FROM ('01a2ce8b-d400-0000-0000-000000000000') 
	TO ('01a49e08-2c00-0000-0000-000000000000');
CREATE TABLE observation_y2027q2 PARTITION OF observation_new
    FOR VALUES FROM ('01a49e08-2c00-0000-0000-000000000000') 
	TO ('01a672aa-e000-0000-0000-000000000000');
CREATE TABLE observation_y2027q3 PARTITION OF observation_new
    FOR VALUES FROM ('01a672aa-e000-0000-0000-000000000000') 
	TO ('01a84c73-f000-0000-0000-000000000000');
CREATE TABLE observation_y2027q4 PARTITION OF observation_new
    FOR VALUES FROM ('01a84c73-f000-0000-0000-000000000000') 
	TO ('01aa263d-0000-0000-0000-000000000000');
CREATE TABLE observation_y2028q1 PARTITION OF observation_new
    FOR VALUES FROM ('01aa263d-0000-0000-0000-000000000000') 
	TO ('01abfadf-b400-0000-0000-000000000000');
CREATE TABLE observation_y2028q2 PARTITION OF observation_new
    FOR VALUES FROM ('01abfadf-b400-0000-0000-000000000000') 
	TO ('01adcf82-6800-0000-0000-000000000000');
CREATE TABLE observation_y2028q3 PARTITION OF observation_new
    FOR VALUES FROM ('01adcf82-6800-0000-0000-000000000000') 
	TO ('01afa94b-7800-0000-0000-000000000000');
CREATE TABLE observation_y2028q4 PARTITION OF observation_new
    FOR VALUES FROM ('01afa94b-7800-0000-0000-000000000000') 
	TO ('01b18314-8800-0000-0000-000000000000');
CREATE TABLE observation_y2029q1 PARTITION OF observation_new
    FOR VALUES FROM ('01b18314-8800-0000-0000-000000000000') 
	TO ('01b35290-e000-0000-0000-000000000000');
CREATE TABLE observation_y2029q2 PARTITION OF observation_new
    FOR VALUES FROM ('01b35290-e000-0000-0000-000000000000') 
	TO ('01b52733-9400-0000-0000-000000000000');
CREATE TABLE observation_y2029q3 PARTITION OF observation_new
    FOR VALUES FROM ('01b52733-9400-0000-0000-000000000000') 
	TO ('01b700fc-a400-0000-0000-000000000000');
CREATE TABLE observation_y2029q4 PARTITION OF observation_new
    FOR VALUES FROM ('01b700fc-a400-0000-0000-000000000000') 
	TO ('01b8dac5-b400-0000-0000-000000000000');
CREATE TABLE observation_default PARTITION OF observation_new
    DEFAULT;
CREATE TABLE observation_last1 PARTITION OF observation_new
    FOR VALUES FROM ('03bb2cc3-d800-0000-0000-000000000000') 
	TO ('20000000-0000-0000-0000-000000000000');
CREATE TABLE observation_last2 PARTITION OF observation_new
    FOR VALUES FROM ('20000000-0000-0000-0000-000000000000') 
	TO ('40000000-0000-0000-0000-000000000000');
CREATE TABLE observation_last3 PARTITION OF observation_new
    FOR VALUES FROM ('40000000-0000-0000-0000-000000000000') 
	TO ('60000000-0000-0000-0000-000000000000');
CREATE TABLE observation_last4 PARTITION OF observation_new
    FOR VALUES FROM ('60000000-0000-0000-0000-000000000000') 
	TO ('80000000-0000-0000-0000-000000000000');
CREATE TABLE observation_last5 PARTITION OF observation_new
    FOR VALUES FROM ('80000000-0000-0000-0000-000000000000') 
	TO ('a0000000-0000-0000-0000-000000000000');
CREATE TABLE observation_last6 PARTITION OF observation_new
    FOR VALUES FROM ('a0000000-0000-0000-0000-000000000000') 
	TO ('c0000000-0000-0000-0000-000000000000');
CREATE TABLE observation_last7 PARTITION OF observation_new
    FOR VALUES FROM ('c0000000-0000-0000-0000-000000000000') 
	TO ('ffffffff-ffff-ffff-ffff-ffffffffffff');

-- запоминаем последнее значение сиквенса транзакций
CREATE MATERIALIZED VIEW last_txid_of_observation AS 
SELECT last_value FROM public.transaction_id_seq;

-- загружаем данные
INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '0' AND id < '1';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '1' AND id < '2';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '2' AND id < '3';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '3' AND id < '4';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '4' AND id < '5';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '5' AND id < '6';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '6' AND id < '7';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '7' AND id < '8';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '8' AND id < '9';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > '9' AND id < 'a';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > 'a' AND id < 'b';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > 'b' AND id < 'c';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > 'c' AND id < 'd';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > 'd' AND id < 'e';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > 'e' AND id < 'f';

INSERT INTO observation_new (id,txid,cts,ts,resource_type,status,resource)
SELECT id,txid,cts,ts,resource_type,status,resource
FROM observation WHERE id > 'f';

-- загружаем оставшиеся данные с блокировкой таблицы
DO
$do$
BEGIN
LOCK TABLE observation;

INSERT INTO observation_new (id, txid, cts, ts, resource_type, status, resource)
SELECT id, txid, cts, ts, resource_type, status, resource
FROM observation
WHERE txid > (SELECT "last_value" FROM last_txid_of_observation LIMIT 1)
ON CONFLICT (id) DO UPDATE
SET txid = EXCLUDED.txid
  , ts = EXCLUDED.ts
  , status = EXCLUDED.status
  , resource = EXCLUDED.resource;
   
ALTER TABLE observation RENAME TO observation_backup;
ALTER TABLE observation_new RENAME TO observation;

ALTER TABLE observation_backup
DROP CONSTRAINT observation_pkey;

DROP INDEX observation_backup_cts__btree;
DROP INDEX observation_resource__gin_jsquery;
DROP INDEX observation_resource_effective_datetime;
DROP INDEX observation_resource_eoc__pregnant;
DROP INDEX observation_resource_identifier__gin;
DROP INDEX observation_resource_period_patient_condition__gist;
DROP INDEX observation_resource_subject_ref_valid;
DROP INDEX observation_ts__btree;
DROP INDEX observation_txid__btree;

ALTER INDEX observation_new_cts__btree RENAME TO observation_cts__btree;
ALTER INDEX observation_new_resource__gin_jsquery RENAME TO observation_resource__gin_jsquery;
ALTER INDEX observation_new_resource_effective_datetime RENAME TO observation_resource_effective_datetime;
ALTER INDEX observation_new_resource_eoc__pregnant RENAME TO observation_resource_eoc__pregnant;
ALTER INDEX observation_new_resource_identifier__gin RENAME TO observation_resource_identifier__gin;
ALTER INDEX observation_new_resource_period_patient_condition__gist RENAME TO observation_resource_period_patient_condition__gist;
ALTER INDEX observation_new_resource_subject_ref_valid RENAME TO observation_resource_subject_ref_valid;
ALTER INDEX observation_new_ts__btree RENAME TO observation_ts__btree;
ALTER INDEX observation_new_txid__btree RENAME TO observation_txid__btree;

ALTER TABLE observation
RENAME CONSTRAINT observation_new_pkey TO observation_pkey;

END
$do$

-- проверка результата
SELECT 
   (SELECT count(*) from observation) as table_new
  ,(SELECT count(*) from observation_backup) as table_old;
 
 EXPLAIN ANALYZE 
 SELECT *
 FROM observation o 
 WHERE resource #>> '{effective,dateTime}' > '2024-01-01'
   AND id > 'f'
 LIMIT 10;

DROP MATERIALIZED VIEW last_txid_of_observation;
