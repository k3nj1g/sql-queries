-- находим нужную карту беременной по ЕНП пациента
SELECT eoc.id
FROM patient p
JOIN episodeofcare eoc ON eoc.resource @@ logic_revinclude(p.resource, p.id, 'patient')
WHERE p.id = (SELECT id FROM  patient WHERE resource @@ 'identifier.#(system = "urn:identity:enp:Patient" and value = "2148700870000027")'::jsquery)

-- проверка всех связанных ресурсов
WITH resources_to_delete AS (
	SELECT eoc.id eoc_id, ct.id ct_id, r.id r_id, o.id o_id, q.id q_id
	FROM episodeofcare eoc
	LEFT JOIN careteam ct ON ct.resource @@ logic_include(eoc.resource, 'team[*]')
	LEFT JOIN riskassessment r ON r.resource @@ logic_revinclude(eoc.resource, eoc.id, 'episodeOfCare')
	LEFT JOIN observation o ON o.resource @@ logic_revinclude(eoc.resource, eoc.id, 'episodeOfCare')
	LEFT JOIN questionnaireresponse q ON q.resource @@ logic_revinclude(eoc.resource, eoc.id, 'episodeOfCare')
	WHERE eoc.id = 'c1489f30-f097-46c5-b237-b536c133c446'
)
SELECT * FROM resources_to_delete

--удаление всех связанных ресурсов по eoc.id + флаги A06
WITH resources_to_delete AS (
	SELECT eoc.id eoc_id, ct.id ct_id, r.id r_id, o.id o_id, q.id q_id, f.id f_id
	FROM episodeofcare eoc
	LEFT JOIN careteam ct ON ct.resource @@ logic_include(eoc.resource, 'team[*]')
	LEFT JOIN riskassessment r ON r.resource @@ logic_revinclude(eoc.resource, eoc.id, 'episodeOfCare')
	LEFT JOIN observation o ON o.resource @@ logic_revinclude(eoc.resource, eoc.id, 'episodeOfCare')
	LEFT JOIN questionnaireresponse q ON q.resource @@ logic_revinclude(eoc.resource, eoc.id, 'episodeOfCare')
	LEFT JOIN patient p ON (p.resource @@ logic_include(eoc.resource, 'patient', NULL) OR p.id = any(array(SELECT jsonb_path_query(eoc.resource, '$.patient.id') #>> '{}')))
	LEFT JOIN flag f ON f.resource @@ logic_revinclude(p.resource, p.id, 'subject')
		AND jsonb_path_query_first(f.resource, '$.code.coding ? (@.system == "urn:CodeSystem:r21.tag").code') #>> '{}' LIKE 'A06%'
	WHERE eoc.id = 'dc171b39-ee5f-4a39-8aad-37ab138b8bba'
)
, careteam_to_delete AS (
	DELETE FROM careteam
	WHERE id IN (SELECT DISTINCT ct_id FROM resources_to_delete)
	RETURNING id
)
, riskassessment_to_delete AS (
	DELETE FROM riskassessment
	WHERE id IN (SELECT DISTINCT r_id FROM resources_to_delete)
	RETURNING id
)
, observation_to_delete AS (
	DELETE FROM observation
	WHERE id IN (SELECT DISTINCT o_id FROM resources_to_delete)
	RETURNING id
)
, questionnaireresponse_to_delete AS (
	DELETE FROM questionnaireresponse
	WHERE id IN (SELECT DISTINCT q_id FROM resources_to_delete)
	RETURNING id
)
, episodeofcare_to_delete AS (
	DELETE FROM episodeofcare
	WHERE id IN (SELECT DISTINCT eoc_id FROM resources_to_delete)
	RETURNING id
)
, flag_to_delete AS (
	DELETE FROM flag
	WHERE id IN (SELECT DISTINCT f_id FROM resources_to_delete)
	RETURNING id
)
SELECT concat('careteam:', id) FROM careteam_to_delete
UNION
SELECT concat('riskassessment:', id) FROM riskassessment_to_delete
UNION
SELECT concat('observation:', id) FROM observation_to_delete
UNION
SELECT concat('questionnaireresponse:', id) FROM questionnaireresponse_to_delete
UNION
SELECT concat('episodeofcare:', id) FROM episodeofcare_to_delete
UNION
SELECT concat('flag:', id) FROM flag_to_delete

