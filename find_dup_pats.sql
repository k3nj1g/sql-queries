select
  jsonb_agg(patient.*) as pts
 from patient
  where jsonb_path_exists(resource, '$.identifier[*] ? (@.system == "urn:identity:enp:Patient"
                                                        && ! (@.value starts with "00000000000000"))')
  group by jsonb_path_query_first(resource, '$.identifier[*] ? (@.system == "urn:identity:enp:Patient").value')
 having count(*) > 1
 
-- на jsquery и knife_extract
select jsonb_agg(patient.*) as pts
from patient
where resource @@ 'identifier.#(system = "urn:identity:enp:Patient" and not value = "00000000000000")'::jsquery
group by (knife_extract_text(resource,'[["identifier",{"system":"urn:identity:enp:Patient"},"value"]]'))[1]
having count(*) > 1

-- с лимитом пациентов
select jsonb_agg(p.*) as pts
from (select * from patient limit 10000) as p
where p.resource @@ 'identifier.#(system = "urn:identity:enp:Patient" and not value = "00000000000000")'::jsquery
group by (knife_extract_text(p.resource,'[["identifier",{"system":"urn:identity:enp:Patient"},"value"]]'))[1]
having count(*) > 1 

--- поиск по совпадению снилс
SELECT jsonb_agg(jsonb_path_query_first(p.resource, '$.identifier[*] ? (@.system == "urn:source:tfoms:Patient").value')) AS pts
FROM (SELECT * FROM patient ORDER BY id) p
WHERE p.resource @@ 'identifier.#(system="urn:identity:snils:Patient" and not value="000-000-000 00")'::jsquery
GROUP BY jsonb_path_query_first(p.resource, '$.identifier[*] ? (@.system == "urn:identity:snils:Patient").value'), p.resource #>> '{birthDate}', p.resource -> 'name'
HAVING count(*) > 1	

--- поиск по совпадению енп + др
SELECT jsonb_agg(p.id) AS pts
FROM patient p
WHERE p.resource @@ 'identifier.#(system="urn:identity:enp:Patient" and not value="00000000000000")'::jsquery
GROUP BY jsonb_path_query_first(p.resource, '$.identifier[*] ? (@.system == "urn:identity:enp:Patient").value'), p.resource #>> '{birthDate}'
HAVING count(*) > 1	

-- с использованием ROW_NUMBER
SELECT jsonb_agg(dups.id) as pts
FROM (SELECT id, ROW_NUMBER() OVER(PARTITION BY resource -> 'name', resource -> 'birthDate', jsonb_path_query_first(resource, '$.identifier ? (@.system == "urn:identity:snils:Patient").value') ORDER BY id ASC) AS ROW
	  FROM patient
	  where resource @@ 'identifier.#(system = "urn:identity:enp:Patient" and not value = "00000000000000")'::jsquery
	  ) dups
WHERE dups.Row > 1

--поиск пациентов, у которых совпадает снилс и др, но не совпадает код тфомс
SELECT doubles.pts -> 0 pat1, doubles.pts -> 1 pat2, doubles.pts -> 2 pat3
FROM (SELECT jsonb_agg(
	jsonb_build_object(
		'fio', concat(p.resource#>>'{name,0,family}', ' ', p.resource#>>'{name,0,given,0}', ' ', p.resource#>>'{name,0,given,1}'),
		'birth-date', p.resource#>>'{birthDate}',
        'address', jsonb_path_query_first(p.resource, '$.address ? (@.use == "home").text'),
        'snils', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:identity:snils:Patient").value'),
        'tfoms', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:source:tfoms:Patient").value'),
		'enp', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:identity:enp:Patient").value'),
		'polis', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:identity:insurance-gov:Patient").value')
	)) AS pts
	FROM patient p
	--(SELECT * FROM patient p LIMIT 100000) p
	WHERE p.resource @@ 'identifier.#(system="urn:identity:snils:Patient" and not value="000-000-000 00")'::jsquery
	GROUP BY jsonb_path_query_first(p.resource, '$.identifier [*] ? (@.system == "urn:identity:snils:Patient").value'), p.resource #>> '{birthDate}'
	HAVING count(*) > 1
		, jsonb_array_length(jsonb_agg(jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:source:tfoms:Patient").value'))) > 1
	) doubles 
WHERE doubles.pts #>> '{0,tfoms}' <> doubles.pts #>> '{1,tfoms}'

--поиск пациентов, у которых совпадает енп и др и только у одного указан тфомс
SELECT doubles.pts -> 0 pat1, doubles.pts -> 1 pat2, doubles.pts -> 2 pat3
FROM (SELECT jsonb_agg(
	jsonb_build_object(
		'fio', concat(p.resource#>>'{name,0,family}', ' ', p.resource#>>'{name,0,given,0}', ' ', p.resource#>>'{name,0,given,1}'),
		'birth-date', p.resource#>>'{birthDate}',
        'address', jsonb_path_query_first(p.resource, '$.address ? (@.use == "home").text'),
        'snils', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:identity:snils:Patient").value'),
        'tfoms', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:source:tfoms:Patient").value'),
		'enp', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:identity:enp:Patient").value'),
		'polis', jsonb_path_query_first(p.resource, '$.identifier ? (@.system == "urn:identity:insurance-gov:Patient").value')
	)) AS pts
	FROM patient p
	--(SELECT * FROM patient p LIMIT 100000) p
	WHERE p.resource @@ 'identifier.#(system="urn:identity:enp:Patient" and not value="00000000000000")'::jsquery 
		AND COALESCE(p.resource ->> 'active', 'true') = 'true'
	GROUP BY jsonb_path_query_first(p.resource, '$.identifier [*] ? (@.system == "urn:identity:enp:Patient").value'), p.resource #>> '{birthDate}'
	HAVING count(*) > 1
	) doubles 
WHERE jsonb_array_length(jsonb_path_query_array(doubles.pts, '$[*] ? (@.tfoms != null)')) = 1

--
 SELECT
	jsonb_agg(p.*) AS pts
FROM patient p
WHERE
	p.resource @@ 'identifier.#(system="urn:identity:snils:Patient" and not value="000-000-000 00")'::jsquery
GROUP BY
	jsonb_path_query_first(p.resource,
	'$.identifier [*] ? (@.system == "urn:identity:snils:Patient").value'),
	p.resource #>> '{birthDate}',
	p.resource -> 'name'
HAVING
	count(*) > 1
	
SELECT *
FROM pg_indexes
WHERE tablename = 'patient' AND indexdef iLIKE '%birthdate%'

DROP INDEX patient_resource__birthdate_gin_index;

CREATE INDEX patient_resource__name_gin_index ON patient
USING gin ((resource->'name') jsonb_path_value_ops)

VACUUM ANALYZE patient;