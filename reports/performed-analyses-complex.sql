WITH filtered AS (
    SELECT sr.*
    FROM servicerequest AS sr
    WHERE (
            JSONB_PATH_QUERY_FIRST(
                sr.resource,
                '$.performer ? (@.resourceType=="Organization" || @.type=="Organization")'
            ) @@ LOGIC_REVINCLUDE(
                '{"identifier":[{"value":"213001001","system":"urn:identity:kpp:Organization"},{"value":"1022100982056","system":"urn:identity:ogrn:Organization"},{"value":"1.2.643.5.1.13.13.12.2.21.1525","system":"urn:identity:oid:Organization"},{"value":"2126002610","system":"urn:identity:inn:Organization"},{"value":"05213344","system":"urn:identity:okpo:Organization"},{"value":"1.2.643.5.1.13.3.25.21.31","system":"urn:identity:old-oid:Organization"},{"value":"6802006","system":"urn:identity:frmo-head:Organization"},{"value":"6802006","system":"urn:source:frmo-head:Organization"},{"value":"eba5ea5c-0436-11e8-b7c8-005056871882","system":"urn:source:1c:Organization"},{"value":"ab241ca8-ff79-4330-9ca4-c80e6324b1ad","system":"urn:source:rmis:Organization"},{"value":"17b2a212-5354-4c04-a712-16de9e9bd329","system":"urn:source:paknitsmbu:Organization"},{"value":"212320","system":"urn:identity:ffoms.f003:OrganizationInfo"}]}',
                'ab241ca8-ff79-4330-9ca4-c80e6324b1ad'
            )
        )
        AND (
            (sr.resource#>>'{performerInfo,requestStatus}') = 'completed'
        )
        AND (sr.resource->>'authoredOn') BETWEEN '2023-02-27' AND '2023-02-27T23:59:59'
        AND (
            sr.resource @@ 'category.#.coding.#(code in ("Referral-IMI","Referral-LMI","Referral-Rehabilitation","Referral-Consultation","Referral-Hospitalization") and system="urn:CodeSystem:servicerequest-category")'::jsquery
        )
),
grouped AS (
    SELECT CASE
            WHEN pd IS NOT NULL THEN COALESCE(
                (
                    JSONB_PATH_QUERY_FIRST(
                        pd.resource,
                        '$.type.coding ? (@.system=="urn:CodeSystem:LabResearchGroup").display'
                    )#>>'{}'
                ),
                'Иное'
            )
            ELSE 'Прочее'
        END AS pd_lab_group,
        COALESCE(
            (pd.resource->>'name'),
            (
                JSONB_PATH_QUERY_FIRST(
                    sr.resource,
                    '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services" || @.system=="urn:CodeSystem:rmis:ServiceRequest").display'
                )#>>'{}'
            )
        ) AS pd_name,
        COALESCE(
            (
                JSONB_PATH_QUERY_FIRST(
                    dr.resource,
                    '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services").code'
                )#>>'{}'
            ),
            ''
        ) AS test,
        'test name' AS test_name,
        COUNT(dr.*) AS tests,
        COUNT(dr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) = '1'
        ) AS tests_st,
        COUNT(dr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) IS NULL
        ) AS tests_no_location,
        COUNT(dr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) = '2'
        ) AS tests_day_st,
        COUNT(dr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) = '3'
        ) AS tests_amb
        , array_agg(sr.id) srs
    FROM filtered AS sr
        INNER JOIN diagnosticreport AS dr ON (
            (dr.resource->'basedOn') @@ LOGIC_REVINCLUDE(sr.resource, sr.id, '#')
        )
        LEFT JOIN plandefinition AS pd ON (
            pd.id = SPLIT_PART(
                (sr.resource#>>'{instantiatesCanonical,0}'),
                '/',
                2
            )
        )
        AND NOT (
            pd.id IN ('2ec6effd-53fb-493d-a684-a36cb7afaaeb')
        )
    WHERE pd.id IN ('2ec6effd-53fb-493d-a684-a36cb7afaaeb')
    GROUP BY pd_lab_group,
        pd_name,
        test
),
complex AS (
    SELECT CASE
            WHEN pd IS NOT NULL THEN COALESCE(
                (
                    JSONB_PATH_QUERY_FIRST(
                        pd.resource,
                        '$.type.coding ? (@.system=="urn:CodeSystem:LabResearchGroup").display'
                    )#>>'{}'
                ),
                'Иное'
            )
            ELSE 'Прочее'
        END AS pd_lab_group,
        COALESCE(
            (pd.resource->>'name'),
            (
                JSONB_PATH_QUERY_FIRST(
                    sr.resource,
                    '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services" || @.system=="urn:CodeSystem:rmis:ServiceRequest").display'
                )#>>'{}'
            )
        ) AS pd_name,
        COALESCE(
            (
                JSONB_PATH_QUERY_FIRST(
                    sr.resource,
                    '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services" || @.system=="urn:CodeSystem:rmis:ServiceRequest").code'
                )#>>'{}'
            ),
            ''
        ) AS test,
        COALESCE(
            (
                JSONB_PATH_QUERY_FIRST(
                    sr.resource,
                    '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services" || @.system=="urn:CodeSystem:rmis:ServiceRequest").display'
                )#>>'{}'
            ),
            ''
        ) AS test_name,
        COUNT(sr.*) AS tests,
        COUNT(sr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) = '1'
        ) AS tests_st,
        COUNT(sr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) IS NULL
        ) AS tests_no_location,
        COUNT(sr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) = '2'
        ) AS tests_day_st,
        COUNT(sr.*) FILTER (
            WHERE (
                    JSONB_PATH_QUERY_FIRST(
                        sr.resource,
                        '$.locationCode.coding ? (@.system=="urn:CodeSystem:mis.medical-help-type").code'
                    )#>>'{}'
                ) = '3'
        ) AS tests_amb
        , array_agg(sr.id) srs
    FROM filtered AS sr
        LEFT JOIN plandefinition AS pd ON (
            pd.id = SPLIT_PART(
                (sr.resource#>>'{instantiatesCanonical,0}'),
                '/',
                2
            )
        )
        AND (
            pd.id IN ('2ec6effd-53fb-493d-a684-a36cb7afaaeb')
        )
    WHERE pd.id IN ('2ec6effd-53fb-493d-a684-a36cb7afaaeb')
    GROUP BY pd_lab_group,
        pd_name,
        test,
        test_name
)
SELECT 
    srs,
    pd_lab_group,
    pd_name,
    tests AS tests_all,
    tests_st,
    tests_no_location,
    tests_day_st,
    tests_amb,
    COALESCE((c_test.resource->>'display'), test_name) AS test
FROM (
        SELECT *
        FROM grouped
        UNION ALL
        SELECT *
        FROM complex
    ) AS grouped
    LEFT JOIN concept AS c_test ON (
        (c_test.resource#>>'{system}') = 'urn:CodeSystem:Nomenclature-medical-services'
    )
    AND ((c_test.resource#>>'{code}') = test)
