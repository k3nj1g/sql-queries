SELECT concat(
    resource#>>'{name,0,family}',
    ' ',
    resource#>>'{name,0,given,0}',
    ' ',
    resource#>>'{name,0,given,1}'
  ) fio,
  resource->>'birthDate',
  jsonb_path_query_first(
    resource,
    '$.identifier[*] ? (@.system=="urn:identity:enp:Patient")'
  )#>>'{value}',
  count(*)
FROM patient
WHERE resource->>'birthDate' IS NOT NULL
  AND jsonb_path_query_first(
    resource,
    '$.identifier[*] ? (@.system=="urn:identity:enp:Patient")'
  )#>>'{value}' IS NOT NULL
  AND COALESCE((resource->>'active'), 'true') = 'true'
GROUP BY 1,
  2,
  3
HAVING count(*) > 1;
SELECT patient_fio(p.resource),
  resource->>'birthDate',
  count(*)
FROM patient
WHERE resource->>'birthDate' IS NOT NULL
  AND COALESCE((resource->>'active'), 'true') = 'true'
GROUP BY 1,
  2
HAVING count(*) > 1;
SELECT jsonb_agg(
    jsonb_build_object(
      'id',
      p.id,
      'fio',
      patient_fio(p.resource),
      'birth-date',
      p.resource#>>'{birthDate}',
      'snils',
      jsonb_path_query_first(
        p.resource,
        '$.identifier ? (@.system == "urn:identity:snils:Patient").value'
      ),
      'tfoms',
      jsonb_path_query_first(
        p.resource,
        '$.identifier ? (@.system == "urn:source:tfoms:Patient").value'
      ),
      'enp',
      jsonb_path_query_first(
        p.resource,
        '$.identifier ? (@.system == "urn:identity:enp:Patient").value'
      )
    )
  ) AS pts
FROM patient p --(SELECT * FROM patient p LIMIT 100000) p
WHERE p.resource @@ 'identifier.#(system = "urn:identity:enp:Patient" and not value = "00000000000000")'::jsquery
  AND COALESCE((resource->>'active'), 'true') = 'true'
GROUP BY jsonb_path_query_first(
    p.resource,
    '$.identifier[*] ? (@.system == "urn:identity:enp:Patient").value'
  ),
  p.resource#>>'{birthDate}'
HAVING count(*) > 1
  AND cardinality(
    array_agg(DISTINCT lower(patient_fio(p.resource)))
  ) > 1;

SELECT JSONB_BUILD_ARRAY(p.*) AS source,
  JSONB_AGG(d.*) AS doubles
FROM patient AS p
  INNER JOIN patient AS d ON (
    (
      JSONB_PATH_QUERY_FIRST(
        d.resource,
        '$.identifier ? (!exists (@.period.end)) ? (@.system=="urn:identity:enp:Patient").value'
      ) = JSONB_PATH_QUERY_FIRST(
        p.resource,
        '$.identifier ? (!exists (@.period.end)) ? (@.system=="urn:identity:enp:Patient").value'
      )
    )
    OR (
      JSONB_PATH_QUERY_FIRST(
        d.resource,
        '$.identifier[*] ? (@.system=="urn:identity:snils:Patient").value'
      ) = JSONB_PATH_QUERY_FIRST(
        p.resource,
        '$.identifier[*] ? (@.system=="urn:identity:snils:Patient").value'
      )
    )
  )
  AND (
    (d.resource->>'birthDate') = (p.resource->>'birthDate')
  )
  AND (
    (
      LOWER((d.resource#>>'{name,0,given,0}')) = LOWER((p.resource#>>'{name,0,given,0}'))
    )
    AND (
      LOWER((d.resource#>>'{name,0,family}')) = LOWER((p.resource#>>'{name,0,family}'))
    )
    AND (
      COALESCE(LOWER((d.resource#>>'{name,0,given,1}')), '') = COALESCE(LOWER((p.resource#>>'{name,0,given,1}')), '')
    )
  )
  AND (d.id <> p.id)
  AND (
    COALESCE((d.resource#>>'{active}'), 'true') = 'true'
  )
WHERE (p.ts > 'yesterday')
  AND (
    COALESCE((p.resource#>>'{active}'), 'true') = 'true'
  )
GROUP BY p.id;

SELECT count(*)
FROM patient p
WHERE (p.ts > 'yesterday');
