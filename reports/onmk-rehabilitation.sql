WITH pre AS (
    SELECT p.id AS p_id,
        ct.resource AS ct_resource,
        stage_num,
        stage_type,
        vipis_code,
        working,
        survived,
        ARRAY_AGG(stage_num) OVER w AS stage_nums,
        ARRAY_AGG(fact_2.id) OVER w AS stage_2_array
    FROM flag AS f
        INNER JOIN LATERAL (
            SELECT p.id,
                p.resource,
                AGE(
                    TO_DATE((resource->>'birthDate'), 'YYYY-MM-DD')
                ) < CASE
                    WHEN (p.resource->>'gender') = 'male' THEN CAST('65 years' AS interval)
                    ELSE CAST('61 years' AS interval)
                END AS working,
                CASE
                    WHEN (p.resource#>>'{deceased,dateTime}') IS NOT NULL THEN (p.resource#>>'{deceased,dateTime}') > (f.resource#>>'{period,end}')
                    ELSE true
                END AS survived
            FROM patient AS p
            WHERE (p.id = (f.resource#>>'{subject,id}'))
                AND (
                    COALESCE((p.resource->>'active'), 'true') = 'true'
                )
        ) AS p ON TRUE
        LEFT JOIN LATERAL (
            SELECT ct.id,
                ct.resource,
                (
                    JSONB_PATH_QUERY_FIRST(
                        ct.resource,
                        '$.category.coding ? (@.system=="urn:CodeSystem:rehab-stage").code'
                    )#>>'{}'
                ) AS stage_num,
                (
                    JSONB_PATH_QUERY_FIRST(
                        ct.resource,
                        '$.category.coding ? (@.system=="urn:CodeSystem:rehab-type").code'
                    )#>>'{}'
                ) AS stage_type,
                (
                    JSONB_PATH_QUERY_FIRST(
                        ct.resource,
                        '$.reasonCode ? (exists (@.coding ? (@.system=="urn:CodeSystem:rehab-shrm" && @.code=="vipis"))).coding ? (@.system=="urn:CodeSystem:shrm-values").code'
                    )#>>'{}'
                ) AS vipis_code,
                (
                    JSONB_PATH_QUERY_FIRST(
                        ct.resource,
                        '$.reasonCode ? (exists (@.coding ? (@.system=="urn:CodeSystem:rehab-shrm" && @.code=="post"))).coding ? (@.system=="urn:CodeSystem:shrm-values").code'
                    )#>>'{}'
                ) AS post_code
            FROM careteam AS ct
            WHERE (
                    ct.resource @@ LOGIC_REVINCLUDE(
                        p.resource,
                        p.id,
                        'subject',
                        ' and category.#.coding.#(system="urn:CodeSystem:rehab-stage")'
                    )
                )
                AND (ct.resource#>>'{period,end}') BETWEEN '2022-01-01' AND '2023-01-31'
        ) AS ct ON TRUE
        LEFT JOIN LATERAL (
            SELECT ct.id
            FROM careteam AS ct
            WHERE (
                    ct.resource @@ LOGIC_REVINCLUDE(
                        p.resource,
                        p.id,
                        'subject',
                        ' and category.#.coding.#(system="urn:CodeSystem:rehab-stage" and code="2")'
                    )
                )
                AND (ct.resource#>>'{period,end}') BETWEEN '2022-01-01' AND '2023-01-31'
        ) AS fact_2 ON TRUE
        LEFT JOIN encounter AS enc ON (
            enc.resource @@ LOGIC_INCLUDE(ct.resource, 'encounter')
        )
        OR (
            enc.id = ANY(
                ARRAY(
                    (
                        SELECT (
                                JSONB_PATH_QUERY(ct.resource, '$.encounter.id')#>>'{}'
                            )
                    )
                )
            )
        )
    WHERE (
            f.resource @@ 'code.coding.#(system="urn:CodeSystem:r21.tag" and code="R01.1") and status="active"'::jsquery
        )
        AND (
            IMMUTABLE_TSRANGE(
                (f.resource#>>'{period,start}'),
                COALESCE((f.resource#>>'{period,end}'), 'infinity')
            ) && IMMUTABLE_TSRANGE('2022-01-01', '2023-01-31')
        ) WINDOW w AS (PARTITION BY p)
)
SELECT COUNT(DISTINCT p_id) AS all,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND survived
    ) AS fact_1,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND working
            AND survived
    ) AS fact_1_working,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '0')
            AND survived
    ) AS fact_1_shrm_0,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '1')
            AND survived
    ) AS fact_1_shrm_1,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '2')
            AND survived
    ) AS fact_1_shrm_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '3')
            AND survived
    ) AS fact_1_shrm_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '4')
            AND survived
    ) AS fact_1_shrm_4,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '5')
            AND survived
    ) AS fact_1_shrm_5,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '6')
            AND survived
    ) AS fact_1_shrm_6,
    COUNT(DISTINCT p_id) FILTER (
        WHERE survived
            AND (stage_nums @> ARRAY ['1', '4'])
    ) AS from_1_to_4,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND survived
    ) AS fact_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND survived
            AND (stage_nums @> ARRAY ['1', '2'])
    ) AS from_1_to_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'plan')
            AND (vipis_code = '4')
            AND survived
    ) AS plan_2_shrm_4,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'plan')
            AND (vipis_code = '5')
            AND survived
    ) AS plan_2_shrm_5,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'plan')
            AND (vipis_code = '6')
            AND survived
    ) AS plan_2_shrm_6,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND survived
            AND (
                ARRAY_LENGTH(
                    (
                        (
                            SELECT ARRAY(
                                    (
                                        SELECT DISTINCT id
                                        FROM UNNEST(stage_2_array) AS STAGES(id)
                                    )
                                )
                        )
                    ),
                    1
                ) > 1
            )
    ) AS fact_2_many,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '0')
            AND survived
    ) AS fact_2_shrm_0,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '1')
            AND survived
    ) AS fact_2_shrm_1,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '2')
            AND survived
    ) AS fact_2_shrm_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '3')
            AND survived
    ) AS fact_2_shrm_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '4')
            AND survived
    ) AS fact_2_shrm_4,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '5')
            AND survived
    ) AS fact_2_shrm_5,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '6')
            AND survived
    ) AS fact_2_shrm_6,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND survived
    ) AS fact_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND working
            AND survived
    ) AS fact_3_working,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '2')
            AND survived
    ) AS fact_3_shrm_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '3')
            AND survived
    ) AS fact_3_shrm_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
    ) AS total_1,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '0')
    ) AS total_1_shrm_0,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '1')
    ) AS total_1_shrm_1,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '2')
    ) AS total_1_shrm_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '3')
    ) AS total_1_shrm_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '4')
    ) AS total_1_shrm_4,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '5')
    ) AS total_1_shrm_5,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '1')
            AND (stage_type = 'fact')
            AND (vipis_code = '6')
    ) AS total_1_shrm_6,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
    ) AS total_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '0')
    ) AS total_2_shrm_0,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '1')
    ) AS total_2_shrm_1,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '2')
    ) AS total_2_shrm_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '3')
    ) AS total_2_shrm_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '4')
    ) AS total_2_shrm_4,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '5')
    ) AS total_2_shrm_5,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '2')
            AND (stage_type = 'fact')
            AND (vipis_code = '6')
    ) AS total_2_shrm_6,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
    ) AS total_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '0')
    ) AS total_3_shrm_0,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '1')
    ) AS total_3_shrm_1,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '2')
    ) AS total_3_shrm_2,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '3')
    ) AS total_3_shrm_3,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '4')
    ) AS total_3_shrm_4,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '5')
    ) AS total_3_shrm_5,
    COUNT(DISTINCT p_id) FILTER (
        WHERE (stage_num = '3')
            AND (stage_type = 'fact')
            AND (vipis_code = '6')
    ) AS total_3_shrm_6,
    COUNT(DISTINCT p_id) FILTER (
        WHERE NOT survived
    ) AS dead
FROM pre;


SELECT DISTINCT ON (p) * 
FROM flag AS f
    INNER JOIN LATERAL (
        SELECT p.id
        FROM patient AS p
        WHERE (p.id = (f.resource#>>'{subject,id}') AND not (
                    COALESCE((p.resource->>'active'), 'true') = 'true'
                ))
    ) AS p ON TRUE
WHERE (
        f.resource @@ 'code.coding.#(system="urn:CodeSystem:r21.tag" and code="R01.1") and status="active"'::jsquery
    )
    AND (
        IMMUTABLE_TSRANGE(
            (f.resource#>>'{period,start}'),
            COALESCE((f.resource#>>'{period,end}'), 'infinity')
        ) && IMMUTABLE_TSRANGE('2022-01-01', '2023-01-31')
    )