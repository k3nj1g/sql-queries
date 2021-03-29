--- 0. провакуумировать пациентов ---
VACUUM ANALYZE patient;

--- 1. посчитать пациентов без гражданства --- 
--EXPLAIN ANALYZE 
SELECT count(*)
FROM patient 
WHERE resource @@ 'identifier.#(system = "urn:identity:insurance-gov:Patient" and value = * and not period.end = *) and identifier.#(system = "urn:identity:passport-rf:Patient" and value = * and not period.end = *)'::jsquery
	AND NOT resource ?? 'citizenship';

--- 2. создать материализованное представление с пачкой пациентов без гражданства ---
CREATE MATERIALIZED VIEW patient_citizen AS (
	SELECT count(*)
	FROM patient 
	WHERE resource @@ 'identifier.#(system = "urn:identity:insurance-gov:Patient" and value = * and not period.end = *) and identifier.#(system = "urn:identity:passport-rf:Patient" and value = * and not period.end = *)'::jsquery
		AND NOT resource ?? 'citizenship'
	LIMIT 5000
);

--- 3. запустить обновление пациентов ---
UPDATE patient p
SET resource = jsonb_set(p.resource, '{citizenship}'
					, jsonb_build_array(
						jsonb_build_object('code'
							, jsonb_build_object('coding'
								, jsonb_build_array(
									jsonb_build_object('code', '643', 'system', 'urn:CodeSystem:citizenship', 'display', 'РОССИЯ Российская Федерация'))))))
FROM patient_citizen pc
WHERE p.id = pc.id

--- 4. взять новую пачку пациентов ---
REFRESH MATERIALIZED VIEW patient_citizen;

--- 5. повторить пункты 1, 2, 3 пока результат п.1 > 0

--- 6. удалить материализованное представление
DROP MATERIALIZED VIEW patient_citizen;
--- 

SELECT count(*) FROM patient_citizen;


--- проверка апдейта ресурса --- 	
SELECT jsonb_set(resource, '{citizenship}'
				, jsonb_build_array(
					jsonb_build_object('code'
						, jsonb_build_object('coding'
							, jsonb_build_array(
								jsonb_build_object('id', 'citizenship.643', 'code', '643', 'system', 'urn:CodeSystem:citizenship', 'display', 'РОССИЯ Российская Федерация'))))))
FROM patient p 
LIMIT 1
---

ALTER TABLE patient SET (autovacuum_enabled = false);

ALTER TABLE patient RESET (autovacuum_enabled);

SELECT reloptions
FROM pg_class
WHERE relname = 'patient';