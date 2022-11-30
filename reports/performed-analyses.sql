WITH grouped AS (
  SELECT 
    (
      JSONB_PATH_QUERY_FIRST(
        pd.resource,
        '$.type.coding ? (@.system=="urn:CodeSystem:LabResearchGroup").display'
      )#>>'{}'
    ) AS pd_lab_group,
    (pd.resource ->> 'name') pd_name,
    COALESCE(
      (
        JSONB_PATH_QUERY_FIRST(
          dr.resource,
          '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services").code'
        )#>>'{}'
      ),
      ''
    ) AS med_service,
    COALESCE(
      (
        JSONB_PATH_QUERY_FIRST(
          o.resource,
          '$.code.coding ? (@.system=="urn:CodeSystem:Laboratory-Research-and-Test").code'
        )#>>'{}'
      ),
      ''
    ) AS test,
    COUNT(DISTINCT sr.*) AS requests,
    COUNT(o.*) AS tests,
    COUNT(o.*) FILTER (
      WHERE (
          JSONB_PATH_QUERY_FIRST(
            sr.resource,
            '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
          )#>>'{}'
        ) = '1'
    ) AS tests_st,
    COUNT(o.*) FILTER (
      WHERE (
          JSONB_PATH_QUERY_FIRST(
            sr.resource,
            '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
          )#>>'{}'
        ) IS NULL
    ) AS tests_no_location,
    COUNT(o.*) FILTER (
      WHERE (
          JSONB_PATH_QUERY_FIRST(
            sr.resource,
            '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
          )#>>'{}'
        ) = '2'
    ) AS tests_day_st,
    COUNT(o.*) FILTER (
      WHERE (
          JSONB_PATH_QUERY_FIRST(
            sr.resource,
            '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
          )#>>'{}'
        ) = '3'
    ) AS tests_amb,
    array_agg(sr.id) sr_id
  FROM servicerequest AS sr
    INNER JOIN plandefinition pd ON pd.id = SPLIT_PART((sr.resource#>>'{instantiatesCanonical,0}'), '/', 2)
    INNER JOIN diagnosticreport AS dr ON (
      (dr.resource->'basedOn') @@ LOGIC_REVINCLUDE(sr.resource, sr.id, '#')
    )
    INNER JOIN observation AS o ON (
      o.resource @@ LOGIC_INCLUDE(dr.resource, 'result')
    )
    OR (
      o.id = ANY(
        ARRAY(
          (
            SELECT (
                JSONB_PATH_QUERY(dr.resource, '$.result.id')#>>'{}'
              )
          )
        )
      )
    )
  WHERE (
      JSONB_PATH_QUERY_FIRST(
        sr.resource,
        '$.performer ? (@.resourceType=="Organization" || @.type=="Organization")'
      ) @@ LOGIC_REVINCLUDE(
        '{"identifier":[{"value":"213001001","system":"urn:identity:kpp:Organization"},{"value":"1022100982056","system":"urn:identity:ogrn:Organization"},{"value":"1.2.643.5.1.13.13.12.2.21.1525","system":"urn:identity:oid:Organization"},{"value":"1.2.643.5.1.13.3.25.21.31","system":"urn:identity:old-oid:Organization"},{"value":"6802006","system":"urn:identity:frmo-head:Organization"},{"value":"2126002610","system":"urn:identity:inn:Organization"},{"value":"05213344","system":"urn:identity:okpo:Organization"},{"value":"6802006","system":"urn:source:frmo-head:Organization"},{"value":"eba5ea5c-0436-11e8-b7c8-005056871882","system":"urn:source:1c:Organization"},{"value":"ab241ca8-ff79-4330-9ca4-c80e6324b1ad","system":"urn:source:rmis:Organization"},{"value":"17b2a212-5354-4c04-a712-16de9e9bd329","system":"urn:source:paknitsmbu:Organization"},{"value":"212320","system":"urn:identity:ffoms.f003:OrganizationInfo"}]}',
        'ab241ca8-ff79-4330-9ca4-c80e6324b1ad'
      )
    )
    AND (
      (sr.resource#>>'{performerInfo,requestStatus}') = 'completed'
    )
    AND (sr.resource->>'authoredOn') BETWEEN '2022-11-01' AND '2022-11-21T23:59:59'
    AND (
      sr.resource @@ 'category.#.coding.#(code in ("Referral-IMI","Referral-LMI","Referral-Rehabilitation","Referral-Consultation","Referral-Hospitalization") and system="urn:CodeSystem:servicerequest-category")'::jsquery
    )
    AND (sr.resource #>> '{managingOrganization,id}' = 'ab241ca8-ff79-4330-9ca4-c80e6324b1ad')
  GROUP BY 
    pd_lab_group,
    pd_name,
    med_service,
    test
)
SELECT 
  pd_lab_group,
  pd_name,
  (c_med_service.resource->>'display') AS med_service,
  (c_test.resource->>'code') AS test_code,
  (c_test.resource->>'display') AS test,
  tests AS tests_all,
  tests_st,
  tests_no_location,
  tests_day_st,
  tests_amb
  -- sr_id
FROM grouped
  INNER JOIN concept AS c_med_service ON (
    (c_med_service.resource#>>'{system}') = 'urn:CodeSystem:Nomenclature-medical-services'
  )
  AND (
    (c_med_service.resource#>>'{code}') = med_service
  )
  INNER JOIN concept AS c_test ON (
    (c_test.resource#>>'{system}') = 'urn:CodeSystem:Laboratory-Research-and-Test'
  )
  AND ((c_test.resource#>>'{code}') = test)