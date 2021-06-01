SELECT e.id privilege_pf_id
FROM episodeofcare e
  LEFT JOIN patient p ON p.resource @@ logic_include (e.resource,'patient')
WHERE e.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PrivilegePF")'::jsquery
AND   p IS NULL;

SELECT e.id privilege_covid19_id
FROM episodeofcare e
  LEFT JOIN patient p ON p.resource @@ logic_include (e.resource,'patient')
WHERE e.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PrivilegeCovid19")'::jsquery
AND   p IS NULL;

SELECT e.id privilege_cardio_id
FROM episodeofcare e
  LEFT JOIN patient p ON p.resource @@ logic_include (e.resource,'patient')
WHERE e.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PrivilegeCardio")'::jsquery
AND   p IS NULL;

SELECT e.id privilege_orfan_id
FROM episodeofcare e
  LEFT JOIN patient p ON p.resource @@ logic_include (e.resource,'patient')
WHERE e.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PrivilegeOrfan")'::jsquery
AND   p IS NULL;

SELECT e.id privilege_regional_id
FROM episodeofcare e
  LEFT JOIN patient p ON p.resource @@ logic_include (e.resource,'patient')
WHERE e.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PrivilegeRegional")'::jsquery
AND   p IS NULL;

SELECT e.id fr12_vzn_id
FROM episodeofcare e
  LEFT JOIN patient p ON p.resource @@ logic_include (e.resource,'patient')
WHERE e.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="FR12VZN")'::jsquery
AND   p IS NULL;

SELECT e.id privilege_circle_of_good_id
FROM episodeofcare e
  LEFT JOIN patient p ON p.resource @@ logic_include (e.resource,'patient')
WHERE e.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PrivilegeCircleOfGood")'::jsquery
AND   p IS NULL;