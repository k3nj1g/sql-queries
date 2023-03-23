EXPLAIN ANALYZE
SELECT row_to_json("patient_subselect".*) AS "patient"
FROM (
              SELECT "patient"."id" AS "id",
                     "patient"."resource_type" AS "resource_type",
                     "patient"."status" AS "status",
                     "patient"."cts" AS "cts",
                     "patient"."ts" AS "ts",
                     "patient"."txid" AS "txid",
                     (
                            "patient".resource || jsonb_build_object(
                                   'id',
                                   "patient".id,
                                   'resourceType',
                                   "patient".resource_type
                            )
                     ) AS "resource",
                     (
                            SELECT json_agg(row_to_json("encounter_subselect".*)) AS "encounter"
                            FROM (
                                          SELECT "encounter"."id" AS "id",
                                                 "encounter"."resource_type" AS "resource_type",
                                                 "encounter"."status" AS "status",
                                                 "encounter"."cts" AS "cts",
                                                 "encounter"."ts" AS "ts",
                                                 "encounter"."txid" AS "txid",
                                                 (
                                                        "encounter".resource || jsonb_build_object(
                                                               'id',
                                                               "encounter".id,
                                                               'resourceType',
                                                               "encounter".resource_type
                                                        )
                                                 ) AS "resource"
                                          FROM "encounter"
                                          WHERE (
                                                        (
                                                               (
                                                                      select mkb
                                                                      from unnest(
                                                                                    knife_extract_text(
                                                                                           encounter.resource,
                                                                                           '[["contained",{},"code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                    )
                                                                             ) mkb
                                                                      where mkb SIMILAR TO 'F%'
                                                                             OR mkb SIMILAR TO 'F1%'
                                                                             OR mkb SIMILAR TO '(B2[0-4])%'
                                                                             OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                             OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                      limit 1
                                                               ) is null
                                                               AND "encounter"."resource" @@ logic_revinclude("patient"."resource", "patient"."id", 'subject')
                                                        )
                                                        AND lower("patient"."resource"#>>'{name,0,given,0}') = CASE
                                                               WHEN "encounter"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "encounter"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             2
                                                                      )
                                                               )
                                                               ELSE lower("patient"."resource"#>>'{name,0,given,0}')
                                                        END
                                                        AND coalesce(
                                                               lower("patient"."resource"#>>'{name,0,given,1}'),
                                                               ''
                                                        ) = CASE
                                                               WHEN "encounter"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "encounter"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             3
                                                                      )
                                                               )
                                                               ELSE coalesce(
                                                                      lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                      ''
                                                               )
                                                        END
                                                        AND CAST("patient"."resource"->>'birthDate' AS "date") = CASE
                                                               WHEN "encounter"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                      split_part(
                                                                             "encounter"."resource"#>>'{subject,display}',
                                                                             ',',
                                                                             2
                                                                      ),
                                                                      'DD.mm.YYYY'
                                                               )
                                                               ELSE CAST("patient"."resource"->>'birthDate' AS "date")
                                                        END
                                                 )
                                          ORDER BY CAST(
                                                        "encounter"."resource"#>>'{period,start}' AS "timestamptz"
                                                 ) DESC NULLS FIRST
                                          LIMIT 6
                                   ) "encounter_subselect"
                     ) AS "encounter",
                     (
                            SELECT json_agg(row_to_json("documentreference_subselect".*)) AS "documentreference"
                            FROM (
                                          SELECT "documentreference"."id" AS "id",
                                                 "documentreference"."resource_type" AS "resource_type",
                                                 "documentreference"."status" AS "status",
                                                 "documentreference"."cts" AS "cts",
                                                 "documentreference"."ts" AS "ts",
                                                 "documentreference"."txid" AS "txid",
                                                 (
                                                        "documentreference".resource || jsonb_build_object(
                                                               'id',
                                                               "documentreference".id,
                                                               'resourceType',
                                                               "documentreference".resource_type
                                                        )
                                                 ) AS "resource"
                                          FROM "documentreference"
                                          WHERE (
                                                        (
                                                               "documentreference"."resource" @@ logic_revinclude("patient"."resource", "patient"."id", 'subject')
                                                               AND lower("patient"."resource"#>>'{name,0,given,0}') = CASE
                                                                      WHEN "documentreference"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                             split_part(
                                                                                    split_part(
                                                                                           "documentreference"."resource"#>>'{subject,display}',
                                                                                           ',',
                                                                                           1
                                                                                    ),
                                                                                    ' ',
                                                                                    2
                                                                             )
                                                                      )
                                                                      ELSE lower("patient"."resource"#>>'{name,0,given,0}')
                                                               END
                                                               AND coalesce(
                                                                      lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                      ''
                                                               ) = CASE
                                                                      WHEN "documentreference"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                             split_part(
                                                                                    split_part(
                                                                                           "documentreference"."resource"#>>'{subject,display}',
                                                                                           ',',
                                                                                           1
                                                                                    ),
                                                                                    ' ',
                                                                                    3
                                                                             )
                                                                      )
                                                                      ELSE coalesce(
                                                                             lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                             ''
                                                                      )
                                                               END
                                                               AND CAST("patient"."resource"->>'birthDate' AS "date") = CASE
                                                                      WHEN "documentreference"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                             split_part(
                                                                                    "documentreference"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    2
                                                                             ),
                                                                             'DD.mm.YYYY'
                                                                      )
                                                                      ELSE CAST("patient"."resource"->>'birthDate' AS "date")
                                                               END
                                                        )
                                                        AND "documentreference"."resource" @@ 'not status="draft" and category.#.coding.#(system="urn:CodeSystem:medrecord-type" and code in ("result-mse","referral-mse","return-mse"))'::jsquery
                                                        AND (
                                                               select mkb
                                                               from unnest(
                                                                             knife_extract_text(
                                                                                    documentreference.resource,
                                                                                    '[["extension",{"url":"urn:extension:diagnosis"},"extension",{"url":"mkb"},"valueCodeableConcept","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                             )
                                                                      ) mkb
                                                               where mkb SIMILAR TO 'F%'
                                                                      OR mkb SIMILAR TO 'F1%'
                                                                      OR mkb SIMILAR TO '(B2[0-4])%'
                                                                      OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                      OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                               limit 1
                                                        ) is null
                                                 )
                                          ORDER BY CAST(
                                                        "documentreference"."resource"->>'date' AS "timestamptz"
                                                 ) DESC NULLS LAST
                                          LIMIT 6
                                   ) "documentreference_subselect"
                     ) AS "mse",
                     (
                            SELECT json_agg(row_to_json("medicationrequest_subselect".*)) AS "medicationrequest"
                            FROM (
                                          SELECT "medicationrequest"."id" AS "id",
                                                 "medicationrequest"."resource_type" AS "resource_type",
                                                 "medicationrequest"."status" AS "status",
                                                 "medicationrequest"."cts" AS "cts",
                                                 "medicationrequest"."ts" AS "ts",
                                                 "medicationrequest"."txid" AS "txid",
                                                 (
                                                        "medicationrequest".resource || jsonb_build_object(
                                                               'id',
                                                               "medicationrequest".id,
                                                               'resourceType',
                                                               "medicationrequest".resource_type
                                                        )
                                                 ) AS "resource",
                                                 (
                                                        SELECT row_to_json("concept_subselect".*) AS "concept"
                                                        FROM (
                                                                      SELECT "concept"."id" AS "id",
                                                                             "concept"."resource_type" AS "resource_type",
                                                                             "concept"."status" AS "status",
                                                                             "concept"."cts" AS "cts",
                                                                             "concept"."ts" AS "ts",
                                                                             "concept"."txid" AS "txid",
                                                                             (
                                                                                    "concept".resource || jsonb_build_object(
                                                                                           'id',
                                                                                           "concept".id,
                                                                                           'resourceType',
                                                                                           "concept".resource_type
                                                                                    )
                                                                             ) AS "resource"
                                                                      FROM "concept" "concept"
                                                                      WHERE (
                                                                                    "concept"."resource"#>>'{system}' = 'urn:CodeSystem:mz.smnn.esklp'
                                                                                    AND "concept"."resource" @@ CAST(
                                                                                           concat(
                                                                                                  'property.SMNN_CODE = "',
                                                                                                  jsonb_path_query_first(
                                                                                                         "medicationrequest"."resource",
                                                                                                         '$.medication.CodeableConcept.coding[*] ? (@.system == "urn:CodeSystem:rmis.klp").code'
                                                                                                  )#>>'{}',
                                                                                                  '"'
                                                                                           ) AS "jsquery"
                                                                                    )
                                                                             )
                                                                      LIMIT 1
                                                               ) "concept_subselect"
                                                 ) AS "smnn_node",
                                                 (
                                                        SELECT row_to_json("concept_subselect".*) AS "concept"
                                                        FROM (
                                                                      SELECT "concept"."id" AS "id",
                                                                             "concept"."resource_type" AS "resource_type",
                                                                             "concept"."status" AS "status",
                                                                             "concept"."cts" AS "cts",
                                                                             "concept"."ts" AS "ts",
                                                                             "concept"."txid" AS "txid",
                                                                             (
                                                                                    "concept".resource || jsonb_build_object(
                                                                                           'id',
                                                                                           "concept".id,
                                                                                           'resourceType',
                                                                                           "concept".resource_type
                                                                                    )
                                                                             ) AS "resource"
                                                                      FROM "concept" "concept"
                                                                      WHERE (
                                                                                    "concept"."resource"#>>'{system}' = 'urn:CodeSystem:mz.specialized-nutrition'
                                                                                    AND "concept"."resource"#>>'{code}' = jsonb_path_query_first(
                                                                                           "medicationrequest"."resource",
                                                                                           '$.medication.CodeableConcept.coding[*] ? (@.system == "urn:CodeSystem:rmis.health").code'
                                                                                    )#>>'{}'
                                                                             )
                                                                      LIMIT 1
                                                               ) "concept_subselect"
                                                 ) AS "specialized_nutrition",
                                                 (
                                                        SELECT row_to_json("concept_subselect".*) AS "concept"
                                                        FROM (
                                                                      SELECT "concept"."id" AS "id",
                                                                             "concept"."resource_type" AS "resource_type",
                                                                             "concept"."status" AS "status",
                                                                             "concept"."cts" AS "cts",
                                                                             "concept"."ts" AS "ts",
                                                                             "concept"."txid" AS "txid",
                                                                             (
                                                                                    "concept".resource || jsonb_build_object(
                                                                                           'id',
                                                                                           "concept".id,
                                                                                           'resourceType',
                                                                                           "concept".resource_type
                                                                                    )
                                                                             ) AS "resource"
                                                                      FROM "concept" "concept"
                                                                      WHERE (
                                                                                    "concept"."resource"#>>'{system}' = 'urn:CodeSystem:mz.medical-devices'
                                                                                    AND "concept"."resource"#>>'{code}' = jsonb_path_query_first(
                                                                                           "medicationrequest"."resource",
                                                                                           '$.medication.CodeableConcept.coding[*] ? (@.system == "urn:CodeSystem:rmis.medicament").code'
                                                                                    )#>>'{}'
                                                                             )
                                                                      LIMIT 1
                                                               ) "concept_subselect"
                                                 ) AS "medical_devices"
                                          FROM "medicationrequest"
                                          WHERE (
                                                        "medicationrequest"."resource" @@ logic_revinclude("patient"."resource", "patient"."id", 'subject')
                                                 )
                                          ORDER BY medicationrequest.resource->>'authoredOn' DESC NULLS LAST
                                          LIMIT 6
                                   ) "medicationrequest_subselect"
                     ) AS "medicationRequest",
                     (
                            SELECT json_agg(row_to_json("documentreference_subselect".*)) AS "documentreference"
                            FROM (
                                          SELECT "documentreference"."id" AS "id",
                                                 "documentreference"."resource_type" AS "resource_type",
                                                 "documentreference"."status" AS "status",
                                                 "documentreference"."cts" AS "cts",
                                                 "documentreference"."ts" AS "ts",
                                                 "documentreference"."txid" AS "txid",
                                                 (
                                                        "documentreference".resource || jsonb_build_object(
                                                               'id',
                                                               "documentreference".id,
                                                               'resourceType',
                                                               "documentreference".resource_type
                                                        )
                                                 ) AS "resource"
                                          FROM "documentreference"
                                                 LEFT JOIN "servicerequest" "sr" ON (
                                                        (
                                                               "sr"."resource" @@ logic_include(
                                                                      "documentreference"."resource",
                                                                      'context.related'
                                                               )
                                                               OR "sr"."id" = any(
                                                                      array(
                                                                             (
                                                                                    SELECT jsonb_path_query(
                                                                                                  "documentreference"."resource",
                                                                                                  '$.context.related.id'
                                                                                           )#>>'{}'
                                                                             )
                                                                      )
                                                               )
                                                        )
                                                        OR "sr"."resource" @@ any_identifier_match("documentreference"."resource")
                                                 )
                                          WHERE (
                                                        (
                                                               (
                                                                      "documentreference"."resource" @@ logic_revinclude(
                                                                             "patient"."resource",
                                                                             "patient"."id",
                                                                             'subject',
                                                                             ' and not status="superseded" and not docStatus in ("superseded","preliminary") and not medicalReport.impossibleReason=* and not category.#.coding.#(code in ("TMK-service-request-attachment","result-mse","referral-mse","deathCertificate","documenttoREMD"))'
                                                                      )
                                                                      AND coalesce(
                                                                             "documentreference"."resource"->>'active',
                                                                             'true'
                                                                      ) = 'true'
                                                                      AND (
                                                                             select mkb
                                                                             from unnest(
                                                                                           knife_extract_text(
                                                                                                  documentreference.resource,
                                                                                                  '[["extension",{"url":"urn:extension:diagnosis"},"extension",{"url":"mkb"},"valueCodeableConcept","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                           )
                                                                                    ) mkb
                                                                             where mkb SIMILAR TO 'F%'
                                                                                    OR mkb SIMILAR TO 'F1%'
                                                                                    OR mkb SIMILAR TO '(B2[0-4])%'
                                                                                    OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                                    OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                             limit 1
                                                                      ) is null
                                                                      AND (
                                                                             select mkb
                                                                             from unnest(
                                                                                           knife_extract_text(
                                                                                                  documentreference.resource,
                                                                                                  '[["medicalReport","diagnosis","code"]]'
                                                                                           )
                                                                                    ) mkb
                                                                             where mkb SIMILAR TO 'F%'
                                                                                    OR mkb SIMILAR TO 'F1%'
                                                                                    OR mkb SIMILAR TO '(B2[0-4])%'
                                                                                    OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                                    OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                             limit 1
                                                                      ) is null
                                                               )
                                                               AND lower("patient"."resource"#>>'{name,0,given,0}') = CASE
                                                                      WHEN "documentreference"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                             split_part(
                                                                                    split_part(
                                                                                           "documentreference"."resource"#>>'{subject,display}',
                                                                                           ',',
                                                                                           1
                                                                                    ),
                                                                                    ' ',
                                                                                    2
                                                                             )
                                                                      )
                                                                      ELSE lower("patient"."resource"#>>'{name,0,given,0}')
                                                               END
                                                               AND coalesce(
                                                                      lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                      ''
                                                               ) = CASE
                                                                      WHEN "documentreference"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                             split_part(
                                                                                    split_part(
                                                                                           "documentreference"."resource"#>>'{subject,display}',
                                                                                           ',',
                                                                                           1
                                                                                    ),
                                                                                    ' ',
                                                                                    3
                                                                             )
                                                                      )
                                                                      ELSE coalesce(
                                                                             lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                             ''
                                                                      )
                                                               END
                                                               AND CAST("patient"."resource"->>'birthDate' AS "date") = CASE
                                                                      WHEN "documentreference"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                             split_part(
                                                                                    "documentreference"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    2
                                                                             ),
                                                                             'DD.mm.YYYY'
                                                                      )
                                                                      ELSE CAST("patient"."resource"->>'birthDate' AS "date")
                                                               END
                                                        )
                                                        AND (
                                                               select mkb
                                                               from unnest(
                                                                             knife_extract_text(
                                                                                    sr.resource,
                                                                                    '[["reasonCode","coding",{"system":"urn:CodeSystem:icd-10"},"code"],["reasonReference","identifier",{"system":"urn:CodeSystem:icd-10"},"value"]]'
                                                                             )
                                                                      ) mkb
                                                               where mkb SIMILAR TO 'F%'
                                                                      OR mkb SIMILAR TO 'F1%'
                                                                      OR mkb SIMILAR TO '(B2[0-4])%'
                                                                      OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                      OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                               limit 1
                                                        ) is null
                                                 )
                                          ORDER BY CAST(
                                                        "documentreference"."resource"->>'date' AS "timestamptz"
                                                 ) DESC NULLS LAST
                                          LIMIT 6
                                   ) "documentreference_subselect"
                     ) AS "documentReference",
                     (
                            SELECT json_agg(row_to_json("condition_subselect".*)) AS "condition"
                            FROM (
                                          SELECT "condition"."id" AS "id",
                                                 "condition"."resource_type" AS "resource_type",
                                                 "condition"."status" AS "status",
                                                 "condition"."cts" AS "cts",
                                                 "condition"."ts" AS "ts",
                                                 "condition"."txid" AS "txid",
                                                 (
                                                        "condition".resource || jsonb_build_object(
                                                               'id',
                                                               "condition".id,
                                                               'resourceType',
                                                               "condition".resource_type
                                                        )
                                                 ) AS "resource"
                                          FROM (
                                                        SELECT "res"."agg"->>'id' AS "id",
                                                               "res"."agg"->>'txid' AS "txid",
                                                               CAST("res"."agg"->>'ts' AS "timestamptz") AS "ts",
                                                               "res"."agg"->>'resource_type' AS "resource_type",
                                                               "res"."agg"->>'status' AS "status",
                                                               CAST("res"."agg"->>'cts' AS "timestamptz") AS "cts",
                                                               "res"."agg"->'resource' AS "resource"
                                                        FROM (
                                                                      WITH "_rules" AS (
                                                                             SELECT jsonb_array_elements("concept"."resource"#>'{property,condition}') AS "_rule"
                                                                             FROM "concept" "concept"
                                                                             WHERE (
                                                                                           "concept"."resource"#>>'{system}' = 'urn:CodeSystem:r21.resource-tag'
                                                                                           AND "concept"."resource"#>>'{code}' = 'Condition'
                                                                                    )
                                                                      ),
                                                                      "rules" AS (
                                                                             SELECT "r"."_rule"#>>'{mkb-from,code}' AS "mkb_from",
                                                                                    "r"."_rule"#>>'{mkb-to,code}' AS "mkb_to"
                                                                             FROM "_rules" "r"
                                                                      ),
                                                                      "grouped" AS (
                                                                             SELECT jsonb_agg(row_to_json("cond".*)) AS "agg"
                                                                             FROM "condition" "cond"
                                                                             WHERE (
                                                                                           (
                                                                                                  "cond"."resource" @@ logic_revinclude(
                                                                                                         "patient"."resource",
                                                                                                         "patient"."id",
                                                                                                         'subject',
                                                                                                         ' and not clinicalStatus.coding.#.code="inactive"'
                                                                                                  )
                                                                                                  AND (
                                                                                                         select mkb
                                                                                                         from unnest(
                                                                                                                       knife_extract_text(
                                                                                                                              cond.resource,
                                                                                                                              '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                                       )
                                                                                                                ) mkb
                                                                                                         where mkb SIMILAR TO 'F%'
                                                                                                                OR mkb SIMILAR TO 'F1%'
                                                                                                                OR mkb SIMILAR TO '(B2[0-4])%'
                                                                                                                OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                                                                OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                                                         limit 1
                                                                                                  ) is null
                                                                                                  AND (
                                                                                                         SELECT 1
                                                                                                         FROM "rules" "r"
                                                                                                         WHERE (
                                                                                                                       (
                                                                                                                              (
                                                                                                                                     knife_extract_text(
                                                                                                                                            cond.resource,
                                                                                                                                            '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                                                     )
                                                                                                                              ) [1] >= "r"."mkb_from"
                                                                                                                              AND (
                                                                                                                                     knife_extract_text(
                                                                                                                                            cond.resource,
                                                                                                                                            '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                                                     )
                                                                                                                              ) [1] <= "r"."mkb_to"
                                                                                                                       )
                                                                                                                       OR (
                                                                                                                              (
                                                                                                                                     knife_extract_text(
                                                                                                                                            cond.resource,
                                                                                                                                            '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'
                                                                                                                                     )
                                                                                                                              ) [1] like concat("r"."mkb_to", '%')
                                                                                                                       )
                                                                                                                )
                                                                                                         LIMIT 1
                                                                                                  ) IS NOT NULL
                                                                                           )
                                                                                           AND lower("patient"."resource"#>>'{name,0,given,0}') = CASE
                                                                                                  WHEN "cond"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                                                         split_part(
                                                                                                                split_part(
                                                                                                                       "cond"."resource"#>>'{subject,display}',
                                                                                                                       ',',
                                                                                                                       1
                                                                                                                ),
                                                                                                                ' ',
                                                                                                                2
                                                                                                         )
                                                                                                  )
                                                                                                  ELSE lower("patient"."resource"#>>'{name,0,given,0}')
                                                                                           END
                                                                                           AND coalesce(
                                                                                                  lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                                                  ''
                                                                                           ) = CASE
                                                                                                  WHEN "cond"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                                                         split_part(
                                                                                                                split_part(
                                                                                                                       "cond"."resource"#>>'{subject,display}',
                                                                                                                       ',',
                                                                                                                       1
                                                                                                                ),
                                                                                                                ' ',
                                                                                                                3
                                                                                                         )
                                                                                                  )
                                                                                                  ELSE coalesce(
                                                                                                         lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                                                         ''
                                                                                                  )
                                                                                           END
                                                                                           AND CAST("patient"."resource"->>'birthDate' AS "date") = CASE
                                                                                                  WHEN "cond"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                                                         split_part(
                                                                                                                "cond"."resource"#>>'{subject,display}',
                                                                                                                ',',
                                                                                                                2
                                                                                                         ),
                                                                                                         'DD.mm.YYYY'
                                                                                                  )
                                                                                                  ELSE CAST("patient"."resource"->>'birthDate' AS "date")
                                                                                           END
                                                                                    )
                                                                             GROUP BY knife_extract_text(
                                                                                           cond.resource::jsonb,
                                                                                           '[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'::jsonb
                                                                                    )
                                                                      )
                                                                      SELECT (
                                                                                    SELECT "grouped_agg"
                                                                                    FROM jsonb_array_elements("grouped"."agg") "grouped_agg"
                                                                                    ORDER BY "grouped_agg"#>>'{resource,recordedDate}' DESC NULLS LAST
                                                                                    LIMIT 1
                                                                             ) AS "agg"
                                                                      FROM "grouped"
                                                               ) "res"
                                                 ) "condition"
                                          ORDER BY "condition"."resource"->>'recordedDate' DESC NULLS LAST
                                          LIMIT NULL
                                   ) "condition_subselect"
                     ) AS "conditionRegistry",
                     (
                            SELECT json_agg(row_to_json("servicerequest_subselect".*)) AS "servicerequest"
                            FROM (
                                          SELECT "servicerequest"."id" AS "id",
                                                 "servicerequest"."resource_type" AS "resource_type",
                                                 "servicerequest"."status" AS "status",
                                                 "servicerequest"."cts" AS "cts",
                                                 "servicerequest"."ts" AS "ts",
                                                 "servicerequest"."txid" AS "txid",
                                                 (
                                                        "servicerequest".resource || jsonb_build_object(
                                                               'id',
                                                               "servicerequest".id,
                                                               'resourceType',
                                                               "servicerequest".resource_type
                                                        )
                                                 ) AS "resource",
                                                 (
                                                        SELECT row_to_json("diagnosticreport_subselect".*) AS "diagnosticreport"
                                                        FROM (
                                                                      SELECT "diagnosticreport"."id" AS "id",
                                                                             "diagnosticreport"."resource_type" AS "resource_type",
                                                                             "diagnosticreport"."status" AS "status",
                                                                             "diagnosticreport"."cts" AS "cts",
                                                                             "diagnosticreport"."ts" AS "ts",
                                                                             "diagnosticreport"."txid" AS "txid",
                                                                             (
                                                                                    "diagnosticreport".resource || jsonb_build_object(
                                                                                           'id',
                                                                                           "diagnosticreport".id,
                                                                                           'resourceType',
                                                                                           "diagnosticreport".resource_type
                                                                                    )
                                                                             ) AS "resource"
                                                                      FROM "diagnosticreport"
                                                                      WHERE "diagnosticreport"."resource" @@ logic_revinclude(
                                                                                    "servicerequest"."resource",
                                                                                    "servicerequest"."id",
                                                                                    'basedOn.#'
                                                                             )
                                                                      ORDER BY "ts" DESC
                                                                      LIMIT 1
                                                               ) "diagnosticreport_subselect"
                                                 ) AS "diagnosticReport"
                                          FROM "servicerequest"
                                          WHERE (
                                                        (
                                                               "servicerequest"."resource" @@ logic_revinclude("patient"."resource", "patient"."id", 'subject')
                                                               AND "servicerequest"."resource" @@ 'category.#.coding.#(system= "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")'::jsquery
                                                               AND (
                                                                      select mkb
                                                                      from unnest(
                                                                                    knife_extract_text(
                                                                                           servicerequest.resource,
                                                                                           '[["reasonCode","coding",{"system":"urn:CodeSystem:icd-10"},"code"],["reasonReference","identifier",{"system":"urn:CodeSystem:icd-10"},"value"]]'
                                                                                    )
                                                                             ) mkb
                                                                      where mkb SIMILAR TO 'F%'
                                                                             OR mkb SIMILAR TO 'F1%'
                                                                             OR mkb SIMILAR TO '(B2[0-4])%'
                                                                             OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                             OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                      limit 1
                                                               ) is null
                                                        )
                                                        AND lower("patient"."resource"#>>'{name,0,given,0}') = CASE
                                                               WHEN "servicerequest"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "servicerequest"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             2
                                                                      )
                                                               )
                                                               ELSE lower("patient"."resource"#>>'{name,0,given,0}')
                                                        END
                                                        AND coalesce(
                                                               lower("patient"."resource"#>>'{name,0,given,1}'),
                                                               ''
                                                        ) = CASE
                                                               WHEN "servicerequest"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "servicerequest"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             3
                                                                      )
                                                               )
                                                               ELSE coalesce(
                                                                      lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                      ''
                                                               )
                                                        END
                                                        AND CAST("patient"."resource"->>'birthDate' AS "date") = CASE
                                                               WHEN "servicerequest"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                      split_part(
                                                                             "servicerequest"."resource"#>>'{subject,display}',
                                                                             ',',
                                                                             2
                                                                      ),
                                                                      'DD.mm.YYYY'
                                                               )
                                                               ELSE CAST("patient"."resource"->>'birthDate' AS "date")
                                                        END
                                                 )
                                          ORDER BY CAST(
                                                        "servicerequest"."resource"->>'authoredOn' AS "timestamptz"
                                                 ) DESC NULLS FIRST
                                          LIMIT 6
                                   ) "servicerequest_subselect"
                     ) AS "laboratory",
                     (
                            SELECT json_agg(row_to_json("task_subselect".*)) AS "task"
                            FROM (
                                          SELECT "survey"."id" AS "id",
                                                 "survey"."resource_type" AS "resource_type",
                                                 "survey"."status" AS "status",
                                                 "survey"."cts" AS "cts",
                                                 "survey"."ts" AS "ts",
                                                 "survey"."txid" AS "txid",
                                                 (
                                                        "survey".resource || jsonb_build_object(
                                                               'id',
                                                               "survey".id,
                                                               'resourceType',
                                                               "survey".resource_type
                                                        )
                                                 ) AS "resource"
                                          FROM "task" "survey"
                                          WHERE "survey"."resource" @@ logic_revinclude(
                                                        "patient"."resource",
                                                        "patient"."id",
                                                        'for',
                                                        ' and code.coding.#(system="urn:CodeSystem:chu-task-code" and code="remoteQuestioning")'
                                                 )
                                          LIMIT 6
                                   ) "task_subselect"
                     ) AS "survey",
                     (
                            SELECT json_agg(row_to_json("servicerequest_subselect".*)) AS "servicerequest"
                            FROM (
                                          SELECT "servicerequest"."id" AS "id",
                                                 "servicerequest"."resource_type" AS "resource_type",
                                                 "servicerequest"."status" AS "status",
                                                 "servicerequest"."cts" AS "cts",
                                                 "servicerequest"."ts" AS "ts",
                                                 "servicerequest"."txid" AS "txid",
                                                 (
                                                        "servicerequest".resource || jsonb_build_object(
                                                               'id',
                                                               "servicerequest".id,
                                                               'resourceType',
                                                               "servicerequest".resource_type
                                                        )
                                                 ) AS "resource",
                                                 (
                                                        SELECT row_to_json("diagnosticreport_subselect".*) AS "diagnosticreport"
                                                        FROM (
                                                                      SELECT "diagnosticreport"."id" AS "id",
                                                                             "diagnosticreport"."resource_type" AS "resource_type",
                                                                             "diagnosticreport"."status" AS "status",
                                                                             "diagnosticreport"."cts" AS "cts",
                                                                             "diagnosticreport"."ts" AS "ts",
                                                                             "diagnosticreport"."txid" AS "txid",
                                                                             (
                                                                                    "diagnosticreport".resource || jsonb_build_object(
                                                                                           'id',
                                                                                           "diagnosticreport".id,
                                                                                           'resourceType',
                                                                                           "diagnosticreport".resource_type
                                                                                    )
                                                                             ) AS "resource"
                                                                      FROM "diagnosticreport"
                                                                      WHERE (
                                                                                    "diagnosticreport"."resource" @@ logic_revinclude(
                                                                                           "servicerequest"."resource",
                                                                                           "servicerequest"."id",
                                                                                           'basedOn.#'
                                                                                    )
                                                                                    AND "diagnosticreport"."resource" @@ 'category.#.coding.#(code="basic-result" and system="urn:CodeSystem:diagnosticreport-categoryIMI")'::jsquery
                                                                             )
                                                                      ORDER BY "ts" DESC
                                                                      LIMIT 1
                                                               ) "diagnosticreport_subselect"
                                                 ) AS "diagnosticReport"
                                          FROM "servicerequest"
                                          WHERE (
                                                        (
                                                               "servicerequest"."resource" @@ logic_revinclude("patient"."resource", "patient"."id", 'subject')
                                                               AND "servicerequest"."resource" @@ 'category.#.coding.#(system= "urn:CodeSystem:servicerequest-category" and code = "Referral-IMI")'::jsquery
                                                               AND (
                                                                      select mkb
                                                                      from unnest(
                                                                                    knife_extract_text(
                                                                                           servicerequest.resource,
                                                                                           '[["reasonCode","coding",{"system":"urn:CodeSystem:icd-10"},"code"],["reasonReference","identifier",{"system":"urn:CodeSystem:icd-10"},"value"]]'
                                                                                    )
                                                                             ) mkb
                                                                      where mkb SIMILAR TO 'F%'
                                                                             OR mkb SIMILAR TO 'F1%'
                                                                             OR mkb SIMILAR TO '(B2[0-4])%'
                                                                             OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                             OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                      limit 1
                                                               ) is null
                                                        )
                                                        AND lower("patient"."resource"#>>'{name,0,given,0}') = CASE
                                                               WHEN "servicerequest"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "servicerequest"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             2
                                                                      )
                                                               )
                                                               ELSE lower("patient"."resource"#>>'{name,0,given,0}')
                                                        END
                                                        AND coalesce(
                                                               lower("patient"."resource"#>>'{name,0,given,1}'),
                                                               ''
                                                        ) = CASE
                                                               WHEN "servicerequest"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "servicerequest"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             3
                                                                      )
                                                               )
                                                               ELSE coalesce(
                                                                      lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                      ''
                                                               )
                                                        END
                                                        AND CAST("patient"."resource"->>'birthDate' AS "date") = CASE
                                                               WHEN "servicerequest"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                      split_part(
                                                                             "servicerequest"."resource"#>>'{subject,display}',
                                                                             ',',
                                                                             2
                                                                      ),
                                                                      'DD.mm.YYYY'
                                                               )
                                                               ELSE CAST("patient"."resource"->>'birthDate' AS "date")
                                                        END
                                                 )
                                          ORDER BY CAST(
                                                        "servicerequest"."resource"->>'authoredOn' AS "timestamptz"
                                                 ) DESC NULLS FIRST
                                          LIMIT 6
                                   ) "servicerequest_subselect"
                     ) AS "instrumental",
                     (
                            SELECT json_agg(row_to_json("diagnosticreport_subselect".*)) AS "diagnosticreport"
                            FROM (
                                          SELECT "diagnosticreport"."id" AS "id",
                                                 "diagnosticreport"."resource_type" AS "resource_type",
                                                 "diagnosticreport"."status" AS "status",
                                                 "diagnosticreport"."cts" AS "cts",
                                                 "diagnosticreport"."ts" AS "ts",
                                                 "diagnosticreport"."txid" AS "txid",
                                                 (
                                                        "diagnosticreport".resource || jsonb_build_object(
                                                               'id',
                                                               "diagnosticreport".id,
                                                               'resourceType',
                                                               "diagnosticreport".resource_type
                                                        )
                                                 ) AS "resource"
                                          FROM "diagnosticreport"
                                                 LEFT JOIN "servicerequest" "sr" ON "sr"."resource" @@ any_identifier_match("diagnosticreport"."resource")
                                          WHERE (
                                                        (
                                                               "diagnosticreport"."resource" @@ logic_revinclude("patient"."resource", "patient"."id", 'subject')
                                                               AND "diagnosticreport"."resource" @@ 'not category.#.coding.#(system= "urn:CodeSystem:diagnosticreport-categoryIMI" and (code = "second-result" or code = "consultation-result"))'::jsquery
                                                               AND diagnosticreport.resource ? 'radiationDose'
                                                               AND (
                                                                      select mkb
                                                                      from unnest(
                                                                                    knife_extract_text(
                                                                                           sr.resource,
                                                                                           '[["reasonCode","coding",{"system":"urn:CodeSystem:icd-10"},"code"],["reasonReference","identifier",{"system":"urn:CodeSystem:icd-10"},"value"]]'
                                                                                    )
                                                                             ) mkb
                                                                      where mkb SIMILAR TO 'F%'
                                                                             OR mkb SIMILAR TO 'F1%'
                                                                             OR mkb SIMILAR TO '(B2[0-4])%'
                                                                             OR mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                             OR mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                      limit 1
                                                               ) is null
                                                        )
                                                        AND lower("patient"."resource"#>>'{name,0,given,0}') = CASE
                                                               WHEN "diagnosticreport"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "diagnosticreport"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             2
                                                                      )
                                                               )
                                                               ELSE lower("patient"."resource"#>>'{name,0,given,0}')
                                                        END
                                                        AND coalesce(
                                                               lower("patient"."resource"#>>'{name,0,given,1}'),
                                                               ''
                                                        ) = CASE
                                                               WHEN "diagnosticreport"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN lower(
                                                                      split_part(
                                                                             split_part(
                                                                                    "diagnosticreport"."resource"#>>'{subject,display}',
                                                                                    ',',
                                                                                    1
                                                                             ),
                                                                             ' ',
                                                                             3
                                                                      )
                                                               )
                                                               ELSE coalesce(
                                                                      lower("patient"."resource"#>>'{name,0,given,1}'),
                                                                      ''
                                                               )
                                                        END
                                                        AND CAST("patient"."resource"->>'birthDate' AS "date") = CASE
                                                               WHEN "diagnosticreport"."resource"#>>'{subject,identifier,value}' = '0000000000000000' THEN to_date(
                                                                      split_part(
                                                                             "diagnosticreport"."resource"#>>'{subject,display}',
                                                                             ',',
                                                                             2
                                                                      ),
                                                                      'DD.mm.YYYY'
                                                               )
                                                               ELSE CAST("patient"."resource"->>'birthDate' AS "date")
                                                        END
                                                 )
                                          ORDER BY CAST(
                                                        "diagnosticreport"."resource"#>>'{effective,dateTime}' AS "timestamptz"
                                                 ) DESC NULLS FIRST
                                   ) "diagnosticreport_subselect"
                     ) AS "xRay",
                     (
                            SELECT json_agg(row_to_json("rmis_semd_document_subselect".*)) AS "rmis_semd_document"
                            FROM (
                                          SELECT *,
                                                 (
                                                        SELECT json_agg(
                                                                      row_to_json("rmis_semd_document_signature_subselect".*)
                                                               ) AS "rmis_semd_document_signature"
                                                        FROM (
                                                                      SELECT *
                                                                      FROM "rmis"."semd_document_signature" "sds"
                                                                      WHERE "sds"."semd_document_id" = "sd"."id"
                                                               ) "rmis_semd_document_signature_subselect"
                                                 ) AS "semd_document_signatures",
                                                 (
                                                        SELECT row_to_json("rmis_semd_document_extension_subselect".*) AS "rmis_semd_document_extension"
                                                        FROM (
                                                                      SELECT *
                                                                      FROM "rmis"."semd_document_extension" "sds"
                                                                      WHERE "sds"."semd_document_id" = "sd"."id"
                                                                      LIMIT 1
                                                               ) "rmis_semd_document_extension_subselect"
                                                 ) AS "semd_document_extension"
                                          FROM "rmis"."semd_document" "sd"
                                          WHERE (
                                                        "sd"."j_for" @@ logic_revinclude("patient"."resource", "patient"."id")
                                                        AND ("sd"."code_ramd_2_41" in ('63', '64', '116'))
                                                 )
                                          ORDER BY CAST("sd"."execution_period_start" AS "timestamptz") DESC NULLS LAST
                                          LIMIT 6
                                   ) "rmis_semd_document_subselect"
                     ) AS "medicalReport"
              FROM "patient"
              WHERE "patient"."id" = '4a5701d1-f1bb-4786-8a9a-d1d02c6399af'
       ) "patient_subselect"
LIMIT 100
