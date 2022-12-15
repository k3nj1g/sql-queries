SELECT row_to_json(patient_subselect.*) AS patient
FROM (
            SELECT patient.id AS id,
                  patient.resource_type AS resource_type,
                  patient.status AS status,
                  patient.cts AS cts,
                  patient.ts AS ts,
                  patient.txid AS txid,
                  (
                        "patient".resource || jsonb_build_object(
                              'id',
                              "patient".id,
                              'resourceType',
                              "patient".resource_type
                        )
                  ) AS resource,
                  (
                        SELECT json_agg(row_to_json(condition_subselect.*)) AS condition
                        FROM (
                                    SELECT condition.id AS id,
                                          condition.resource_type AS resource_type,
                                          condition.status AS status,
                                          condition.cts AS cts,
                                          condition.ts AS ts,
                                          condition.txid AS txid,
                                          (
                                                "condition".resource || jsonb_build_object(
                                                      'id',
                                                      "condition".id,
                                                      'resourceType',
                                                      "condition".resource_type
                                                )
                                          ) AS resource
                                    FROM (
                                                SELECT res.agg->>'id' AS id,
                                                      res.agg->>'txid' AS txid,
                                                      CAST(res.agg->>'ts' AS timestamptz) AS ts,
                                                      res.agg->>'resource_type' AS resource_type,
                                                      res.agg->>'status' AS status,
                                                      res.agg->'resource' AS resource,
                                                      CAST(res.agg->>'cts' AS timestamptz) AS cts
                                                FROM (
                                                            WITH grouped AS (
                                                                  SELECT jsonb_agg(row_to_json(cond.*)) AS agg
                                                                  FROM condition cond
                                                                  WHERE (
                                                                              (
                                                                                    cond.resource @@ logic_revinclude(patient.resource, patient.id, 'subject')
                                                                                    AND (
                                                                                          SELECT mkb
                                                                                          FROM unnest(
                                                                                                      knife_extract_text(
                                                                                                            cond.resource,
                                                                                                            '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                      )
                                                                                                ) mkb
                                                                                          WHERE (
                                                                                                      mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                                                      AND (
                                                                                                            pat_morg.id <> '1150e915-f639-4234-a795-1767e0a0be5f'
                                                                                                            OR pat_morg.id IS NULL
                                                                                                      )
                                                                                                )
                                                                                          LIMIT 1
                                                                                    ) IS NULL
                                                                                    AND (
                                                                                          (
                                                                                                (
                                                                                                      (
                                                                                                            knife_extract_text(
                                                                                                                  cond.resource,
                                                                                                                  '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                            )
                                                                                                      ) [1] >= 'A15'
                                                                                                      AND (
                                                                                                            knife_extract_text(
                                                                                                                  cond.resource,
                                                                                                                  '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                            )
                                                                                                      ) [1] <= 'A19'
                                                                                                )
                                                                                                OR (
                                                                                                      (
                                                                                                            knife_extract_text(
                                                                                                                  cond.resource,
                                                                                                                  '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                            )
                                                                                                      ) [1] LIKE 'A19%'
                                                                                                )
                                                                                          )
                                                                                          OR (
                                                                                                (
                                                                                                      (
                                                                                                            knife_extract_text(
                                                                                                                  cond.resource,
                                                                                                                  '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                            )
                                                                                                      ) [1] >= 'B90'
                                                                                                      AND (
                                                                                                            knife_extract_text(
                                                                                                                  cond.resource,
                                                                                                                  '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                            )
                                                                                                      ) [1] <= 'B90'
                                                                                                )
                                                                                                OR (
                                                                                                      (
                                                                                                            knife_extract_text(
                                                                                                                  cond.resource,
                                                                                                                  '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                            )
                                                                                                      ) [1] LIKE 'B90%'
                                                                                                )
                                                                                          )
                                                                                    )
                                                                              )
                                                                              AND LOWER(patient.resource#>>'{name,0,given,0}') = CASE
                                                                                    WHEN cond.resource#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                                          split_part(
                                                                                                split_part(cond.resource#>>'{subject,display}', ',', 1),
                                                                                                ' ',
                                                                                                2
                                                                                          )
                                                                                    )
                                                                                    ELSE lower(patient.resource#>>'{name,0,given,0}')
                                                                              END
                                                                              AND coalesce(
                                                                                    lower(patient.resource#>>'{name,0,given,1}'),
                                                                                    ''
                                                                              ) = CASE
                                                                                    WHEN cond.resource#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                                          split_part(
                                                                                                split_part(cond.resource#>>'{subject,display}', ',', 1),
                                                                                                ' ',
                                                                                                3
                                                                                          )
                                                                                    )
                                                                                    ELSE coalesce(
                                                                                          lower(patient.resource#>>'{name,0,given,1}'),
                                                                                          ''
                                                                                    )
                                                                              END
                                                                              AND CAST(patient.resource->>'birthDate' AS date) = CASE
                                                                                    WHEN cond.resource#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                                          split_part(cond.resource#>>'{subject,display}', ',', 2),
                                                                                          'DD.mm.YYYY'
                                                                                    )
                                                                                    ELSE CAST(patient.resource->>'birthDate' AS date)
                                                                              END
                                                                        )
                                                                  GROUP BY knife_extract_text(
                                                                              cond.resource::jsonb,
                                                                              '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'::jsonb
                                                                        )
                                                            )
                                                            SELECT (
                                                                        SELECT grouped_agg
                                                                        FROM jsonb_array_elements(grouped.agg) grouped_agg
                                                                        ORDER BY grouped_agg#>>'{resource,recordedDate}' DESC NULLS LAST
                                                                        LIMIT 1
                                                                  ) AS agg
                                                            FROM grouped
                                                      ) res
                                          ) condition
                                    ORDER BY knife_extract_text(
                                                condition.resource::jsonb,
                                                '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'::jsonb
                                          )
                                    LIMIT NULL
                              ) condition_subselect
                  ) AS condition,
                  (
                        SELECT json_agg(row_to_json(medrecordtuber_subselect.*)) AS medrecordtuber
                        FROM (
                                    SELECT medrecordtuber.id AS id,
                                          medrecordtuber.resource_type AS resource_type,
                                          medrecordtuber.status AS status,
                                          medrecordtuber.cts AS cts,
                                          medrecordtuber.ts AS ts,
                                          medrecordtuber.txid AS txid,
                                          (
                                                "medrecordtuber".resource || jsonb_build_object(
                                                      'id',
                                                      "medrecordtuber".id,
                                                      'resourceType',
                                                      "medrecordtuber".resource_type
                                                )
                                          ) AS resource
                                    FROM medrecordtuber
                                    WHERE medrecordtuber.resource @@ logic_revinclude(patient.resource, patient.id, 'subject')
                                    LIMIT 6
                              ) medrecordtuber_subselect
                  ) AS med_record_tuber,
                  (
                        SELECT json_agg(row_to_json(encounter_subselect.*)) AS encounter
                        FROM (
                                    SELECT encounter.id AS id,
                                          encounter.resource_type AS resource_type,
                                          encounter.status AS status,
                                          encounter.cts AS cts,
                                          encounter.ts AS ts,
                                          encounter.txid AS txid,
                                          (
                                                "encounter".resource || jsonb_build_object(
                                                      'id',
                                                      "encounter".id,
                                                      'resourceType',
                                                      "encounter".resource_type
                                                )
                                          ) AS resource,
                                          (
                                                SELECT json_agg(row_to_json(documentreference_subselect.*)) AS documentreference
                                                FROM (
                                                            SELECT documentreference.id AS id,
                                                                  documentreference.resource_type AS resource_type,
                                                                  documentreference.status AS status,
                                                                  documentreference.cts AS cts,
                                                                  documentreference.ts AS ts,
                                                                  documentreference.txid AS txid,
                                                                  (
                                                                        "documentreference".resource || jsonb_build_object(
                                                                              'id',
                                                                              "documentreference".id,
                                                                              'resourceType',
                                                                              "documentreference".resource_type
                                                                        )
                                                                  ) AS resource
                                                            FROM documentreference
                                                            WHERE (
                                                                        (
                                                                              select mkb
                                                                              from unnest(
                                                                                          knife_extract_text(
                                                                                                documentreference.resource,
                                                                                                '[["extension",{"url":"urn:extension:diagnosis"},"extension",{"url":"mkb"},"valueCodeableConcept","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                          )
                                                                                    ) mkb
                                                                              where (
                                                                                          mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                                          AND (
                                                                                                pat_morg.id <> '1150e915-f639-4234-a795-1767e0a0be5f'
                                                                                                OR pat_morg.id is null
                                                                                          )
                                                                                    )
                                                                              limit 1
                                                                        ) is null
                                                                        AND documentreference.resource @@ logic_revinclude(
                                                                              encounter.resource,
                                                                              encounter.id,
                                                                              'context.encounter.#'
                                                                        )
                                                                        AND coalesce(documentreference.resource->>'active', 'true') = 'true'
                                                                  )
                                                            ORDER BY CAST(
                                                                        documentreference.resource->>'date' AS timestamptz
                                                                  ) DESC NULLS LAST
                                                            LIMIT 6
                                                      ) documentreference_subselect
                                          ) AS documentReference
                                    FROM encounter
                                    WHERE (
                                                encounter.resource @@ logic_revinclude(patient.resource, patient.id, 'subject')
                                                AND (
                                                      select mkb
                                                      from unnest(
                                                                  knife_extract_text(
                                                                        encounter.resource,
                                                                        '[["contained",{},"code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                  )
                                                            ) mkb
                                                      where (
                                                                  mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                  AND (
                                                                        pat_morg.id <> '1150e915-f639-4234-a795-1767e0a0be5f'
                                                                        OR pat_morg.id is null
                                                                  )
                                                            )
                                                      limit 1
                                                ) is null
                                                AND (
                                                      encounter.resource @@ 'serviceType.coding.#((system = "urn:CodeSystem:oms.v002-care-profile" and (code = "110" or code = "28")))'::jsquery
                                                      OR (
                                                            SELECT enc_code
                                                            FROM unnest(
                                                                        knife_extract_text(
                                                                              encounter.resource::jsonb,
                                                                              '[["contained",{},"code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'::jsonb
                                                                        )
                                                                  ) enc_code
                                                            WHERE (
                                                                        (
                                                                              (
                                                                                    enc_code >= 'A15'
                                                                                    AND enc_code < 'A19'
                                                                              )
                                                                              OR (enc_code like 'A19%')
                                                                        )
                                                                        OR (
                                                                              (
                                                                                    enc_code >= 'B90'
                                                                                    AND enc_code < 'B90'
                                                                              )
                                                                              OR (enc_code like 'B90%')
                                                                        )
                                                                  )
                                                            LIMIT 1
                                                      ) IS NOT NULL
                                                )
                                          )
                                    ORDER BY CAST(
                                                encounter.resource#>>'{period,start}' AS timestamptz
                                          ) DESC NULLS FIRST
                                    LIMIT 6
                              ) encounter_subselect
                  ) AS encounter
            FROM patient
                  LEFT JOIN patientbinding pb ON pb.resource#>>'{patient,id}' = patient.id
                  LEFT JOIN organization pat_morg ON pat_morg.id = pb.resource#>>'{organization,id}'
            WHERE patient.id = '01b7ff7e-1d6f-4ae6-a122-c36e7437eaec'
            LIMIT 1
      ) patient_subselect