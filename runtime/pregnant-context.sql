EXPLAIN ANALYZE 
SELECT row_to_json("episodeofcare_subselect".*) AS "episodeofcare"
FROM (SELECT "episodeofcare"."id" AS "id",
             "episodeofcare"."resource_type" AS "resource_type",
             "episodeofcare"."status" AS "status",
             "episodeofcare"."ts" AS "ts",
             "episodeofcare"."txid" AS "txid",
             ("episodeofcare".resource || jsonb_build_object('id',"episodeofcare".id,'resourceType',"episodeofcare".resource_type)) AS "resource",
             (SELECT json_agg(row_to_json("careteam_subselect".*)) AS "careteam"
              FROM (SELECT "careteam"."id" AS "id",
                           "careteam"."resource_type" AS "resource_type",
                           "careteam"."status" AS "status",
                           "careteam"."ts" AS "ts",
                           "careteam"."txid" AS "txid",
                           ("careteam".resource || jsonb_build_object('id',"careteam".id,'resourceType',"careteam".resource_type)) AS "resource"
                    FROM "careteam"
                    WHERE ("careteam"."resource" @@ logic_include("episodeofcare"."resource",'team[*]') OR "careteam"."id" = ANY (ARRAY ((SELECT jsonb_path_query("episodeofcare"."resource",'$.team[*].id') #>> '{}'))))) "careteam_subselect") AS "careteam",
             (SELECT row_to_json("patient_subselect".*) AS "patient"
              FROM (SELECT "pt"."id" AS "id",
                           "pt"."resource_type" AS "resource_type",
                           "pt"."status" AS "status",
                           "pt"."ts" AS "ts",
                           "pt"."txid" AS "txid",
                           ("pt".resource || jsonb_build_object('id',"pt".id,'resourceType',"pt".resource_type)) AS "resource",
                           (SELECT json_agg(row_to_json("encounter_subselect".*)) AS "encounter"
                            FROM (SELECT "enc"."id" AS "id",
                                         "enc"."resource_type" AS "resource_type",
                                         "enc"."status" AS "status",
                                         "enc"."ts" AS "ts",
                                         "enc"."txid" AS "txid",
                                         ("enc".resource || jsonb_build_object('id',"enc".id,'resourceType',"enc".resource_type)) AS "resource"
                                  FROM "encounter" "enc"
                                  WHERE ("enc"."resource" @@ logic_revinclude("pt"."resource","pt"."id",'subject') AND cast("enc"."resource" #>> '{period,start}' AS "timestamp") BETWEEN cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") AND coalesce(cast("episodeofcare"."resource" #>> '{period,end}' AS "timestamp"),(cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") + '9 month'::interval)))
                                  ORDER BY cast("enc"."resource" #>> '{period,start}' AS "timestamptz") DESC NULLS last) "encounter_subselect") AS "encounter",
                           (SELECT json_agg(row_to_json("documentreference_subselect".*)) AS "documentreference"
                            FROM (SELECT "dr"."id" AS "id",
                                         "dr"."resource_type" AS "resource_type",
                                         "dr"."status" AS "status",
                                         "dr"."ts" AS "ts",
                                         "dr"."txid" AS "txid",
                                         ("dr".resource || jsonb_build_object('id',"dr".id,'resourceType',"dr".resource_type)) AS "resource"
                                  FROM "documentreference" "dr"
                                  WHERE (("dr"."resource" @@ logic_revinclude("pt"."resource","pt"."id",'subject',' and not status="superseded" and not docStatus in ("superseded","preliminary") and not medicalReport.impossibleReason=* and not category.#.coding.#(code in ("TMK-service-request-attachment","result-mse","referral-mse","deathCertificate","documenttoREMD"))') AND coalesce("dr"."resource" ->> 'active','true') = 'true' AND (SELECT mkb
                                                                                                                                                                                                                                                                                                                                                                                                                                                     FROM unnest(knife_extract_text (dr.resource,'[["extension",{"url":"urn:extension:diagnosis"},"extension",{"url":"mkb"},"valueCodeableConcept","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb
                                                                                                                                                                                                                                                                                                                                                                                                                                                     WHERE mkb SIMILAR TO 'F%'
                                                                                                                                                                                                                                                                                                                                                                                                                                                     OR    mkb SIMILAR TO 'F1%'
                                                                                                                                                                                                                                                                                                                                                                                                                                                     OR    mkb SIMILAR TO '(B2[0-4])%'
                                                                                                                                                                                                                                                                                                                                                                                                                                                     OR    mkb SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                                                                                                                                                                                                                                                                                                                                                                                                                     OR    mkb SIMILAR TO '(A1[5-9]|B90|R76.1|Z20.1)%'
                                                                                                                                                                                                                                                                                                                                                                                                                                                     LIMIT 1) IS NULL AND cast("dr"."resource" ->> 'date' AS "timestamp") BETWEEN cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") AND coalesce(cast("episodeofcare"."resource" #>> '{period,end}' AS "timestamp"),(cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") + '9 month'::interval))) AND lower("pt"."resource" #>> '{name,0,given,0}') = CASE WHEN "dr"."resource" #>> '{subject,identifier,value}' = '0000000000000000' THEN lower(split_part(split_part("dr"."resource" #>> '{subject,display}',',',1),' ',2)) ELSE lower("pt"."resource" #>> '{name,0,given,0}') END AND coalesce(lower("pt"."resource" #>> '{name,0,given,1}'),'') = CASE WHEN "dr"."resource" #>> '{subject,identifier,value}' = '0000000000000000' THEN lower(split_part(split_part("dr"."resource" #>> '{subject,display}',',',1),' ',3)) ELSE coalesce(lower("pt"."resource" #>> '{name,0,given,1}'),'') END AND cast("pt"."resource" ->> 'birthDate' AS "date") = CASE WHEN "dr"."resource" #>> '{subject,identifier,value}' = '0000000000000000' THEN to_date(split_part("dr"."resource" #>> '{subject,display}',',',2),'DD.mm.YYYY') ELSE cast("pt"."resource" ->> 'birthDate' AS "date") END)
                                  ORDER BY cast("dr"."resource" ->> 'date' AS "timestamptz") DESC NULLS last LIMIT 6) "documentreference_subselect") AS "document",
                           (SELECT json_agg(row_to_json("patient_subselect".*)) AS "patient"
                            FROM (SELECT "newborn"."id" AS "id",
                                         "newborn"."resource_type" AS "resource_type",
                                         "newborn"."status" AS "status",
                                         "newborn"."ts" AS "ts",
                                         "newborn"."txid" AS "txid",
                                         ("newborn".resource || jsonb_build_object('id',"newborn".id,'resourceType',"newborn".resource_type)) AS "resource",
                                         (SELECT row_to_json("patient_subselect".*) AS "patient"
                                          FROM (SELECT "child"."id" AS "id",
                                                       "child"."resource_type" AS "resource_type",
                                                       "child"."status" AS "status",
                                                       "child"."ts" AS "ts",
                                                       "child"."txid" AS "txid",
                                                       ("child".resource || jsonb_build_object('id',"child".id,'resourceType',"child".resource_type)) AS "resource"
                                                FROM "patient" "child"
                                                WHERE "child"."resource" @@ cast(concat('identifier.#(system="urn:identity:newborn:Patient" and value=',jsonb_path_query_first("newborn"."resource",'$.identifier [*] ? (@.system == "urn:identity:newborn:Patient").value'),') and ','identifier.#(system="urn:source:tfoms:Patient") and active=true') AS "jsquery")
                                                ORDER BY "child"."cts" LIMIT 1) "patient_subselect") AS "child"
                                  FROM "patient" "newborn"
                                  WHERE ("newborn"."resource" @@ logic_revinclude("pt"."resource","pt"."id",'extension.#.extension.#.valueReference',' and extension.#(url="urn:extension:patient-type" and valueCode="newborn")') AND "newborn"."resource" ->> 'birthDate' > "episodeofcare"."resource" #>> '{period,start}')) "patient_subselect") AS "newborn",
                           (SELECT json_agg(row_to_json("episodeofcare_subselect".*)) AS "episodeofcare"
                            FROM (SELECT "previous"."id" AS "id",
                                         "previous"."resource_type" AS "resource_type",
                                         "previous"."status" AS "status",
                                         "previous"."ts" AS "ts",
                                         "previous"."txid" AS "txid",
                                         ("previous".resource || jsonb_build_object('id',"previous".id,'resourceType',"previous".resource_type)) AS "resource",
                                         (SELECT row_to_json("observation_subselect".*) AS "observation"
                                          FROM (SELECT "pregnancies"."id" AS "id",
                                                       "pregnancies"."resource_type" AS "resource_type",
                                                       "pregnancies"."status" AS "status",
                                                       "pregnancies"."ts" AS "ts",
                                                       "pregnancies"."txid" AS "txid",
                                                       ("pregnancies".resource || jsonb_build_object('id',"pregnancies".id,'resourceType',"pregnancies".resource_type)) AS "resource"
                                                FROM "observation" "pregnancies"
                                                WHERE "pregnancies"."resource" @@ logic_revinclude("previous"."resource","previous"."id",'episodeOfCare', 'and category.#.coding.#(system="urn:CodeSystem:pregnancy" and code="current-pregnancy") and code.coding.#(system="urn:CodeSystem:pregnancy-information" and code="pregnancies")')
                                                ORDER BY "pregnancies"."resource" ->> 'status' LIMIT 1) "observation_subselect") AS "num"
                                  FROM "episodeofcare" "previous"
                                  WHERE "previous"."resource" @@ logic_revinclude("pt"."resource","pt"."id",'patient', 'and type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PregnantCard")')) "episodeofcare_subselect") AS "previous",
                           (SELECT json_agg(row_to_json("servicerequest_subselect".*)) AS "servicerequest"
                            FROM (SELECT "sr"."id" AS "id",
                                         "sr"."resource_type" AS "resource_type",
                                         "sr"."status" AS "status",
                                         "sr"."ts" AS "ts",
                                         "sr"."txid" AS "txid",
                                         ("sr".resource || jsonb_build_object('id',"sr".id,'resourceType',"sr".resource_type)) AS "resource"
                                  FROM "servicerequest" "sr"
                                  WHERE ("sr"."resource" @@ logic_revinclude("pt"."resource","pt"."id",'subject',' and category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="Referral-LMI")') AND cast("sr"."resource" ->> 'authoredOn' AS "timestamp") BETWEEN cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") AND coalesce(cast("episodeofcare"."resource" #>> '{period,end}' AS "timestamp"),(cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") + '9 month'::interval)))
                                  ORDER BY cast("sr"."resource" ->> 'authoredOn' AS "timestamp") DESC LIMIT 6) "servicerequest_subselect") AS "laboratory",
                           (SELECT json_agg(row_to_json("servicerequest_subselect".*)) AS "servicerequest"
                            FROM (SELECT "sr"."id" AS "id",
                                         "sr"."resource_type" AS "resource_type",
                                         "sr"."status" AS "status",
                                         "sr"."ts" AS "ts",
                                         "sr"."txid" AS "txid",
                                         ("sr".resource || jsonb_build_object('id',"sr".id,'resourceType',"sr".resource_type)) AS "resource"
                                  FROM "servicerequest" "sr"
                                  WHERE ("sr"."resource" @@ logic_revinclude("pt"."resource","pt"."id",'subject',' and category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="Referral-IMI")') AND cast("sr"."resource" ->> 'authoredOn' AS "timestamp") BETWEEN cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") AND coalesce(cast("episodeofcare"."resource" #>> '{period,end}' AS "timestamp"),(cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") + '9 month'::interval)))
                                  ORDER BY cast("sr"."resource" ->> 'authoredOn' AS "timestamp") DESC LIMIT 6) "servicerequest_subselect") AS "instrumental",
                           (SELECT json_agg(row_to_json("diagnosticreport_subselect".*)) AS "diagnosticreport"
                            FROM (SELECT "diagnosticreport"."id" AS "id",
                                         "diagnosticreport"."resource_type" AS "resource_type",
                                         "diagnosticreport"."status" AS "status",
                                         "diagnosticreport"."ts" AS "ts",
                                         "diagnosticreport"."txid" AS "txid",
                                         ("diagnosticreport".resource || jsonb_build_object('id',"diagnosticreport".id,'resourceType',"diagnosticreport".resource_type)) AS "resource"
                                  FROM "diagnosticreport"
                                  WHERE (("diagnosticreport"."resource" @@ logic_revinclude("pt"."resource","pt"."id",'subject') AND resource ? 'radiationDose' AND cast("diagnosticreport"."resource" #>> '{effective,dateTime}' AS "timestamp") BETWEEN cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") AND coalesce(cast("episodeofcare"."resource" #>> '{period,end}' AS "timestamp"),(cast("episodeofcare"."resource" #>> '{period,start}' AS "timestamp") + '9 month'::interval))) AND lower("pt"."resource" #>> '{name,0,given,0}') = CASE WHEN "diagnosticreport"."resource" #>> '{subject,identifier,value}' = '0000000000000000' THEN lower(split_part(split_part("diagnosticreport"."resource" #>> '{subject,display}',',',1),' ',2)) ELSE lower("pt"."resource" #>> '{name,0,given,0}') END AND coalesce(lower("pt"."resource" #>> '{name,0,given,1}'),'') = CASE WHEN "diagnosticreport"."resource" #>> '{subject,identifier,value}' = '0000000000000000' THEN lower(split_part(split_part("diagnosticreport"."resource" #>> '{subject,display}',',',1),' ',3)) ELSE coalesce(lower("pt"."resource" #>> '{name,0,given,1}'),'') END AND cast("pt"."resource" ->> 'birthDate' AS "date") = CASE WHEN "diagnosticreport"."resource" #>> '{subject,identifier,value}' = '0000000000000000' THEN to_date(split_part("diagnosticreport"."resource" #>> '{subject,display}',',',2),'DD.mm.YYYY') ELSE cast("pt"."resource" ->> 'birthDate' AS "date") END)
                                  ORDER BY cast("diagnosticreport"."resource" #>> '{effective,dateTime}' AS "timestamp") DESC) "diagnosticreport_subselect") AS "xRay"
                    FROM "patient" "pt"
                    WHERE (coalesce("pt"."resource" ->> 'active','true') = 'true' AND ("pt"."resource" @@ logic_include("episodeofcare"."resource", 'patient') OR "pt"."id" = episodeofcare.resource #>> '{patient,id}'))) "patient_subselect") AS "patient",
             (SELECT json_agg(row_to_json("riskassessment_subselect".*)) AS "riskassessment"
              FROM (SELECT "risk"."id" AS "id",
                           "risk"."resource_type" AS "resource_type",
                           "risk"."status" AS "status",
                           "risk"."ts" AS "ts",
                           "risk"."txid" AS "txid",
                           ("risk".resource || jsonb_build_object('id',"risk".id,'resourceType',"risk".resource_type)) AS "resource"
                    FROM "riskassessment" "risk"
                    WHERE "risk"."resource" @@ logic_revinclude("episodeofcare"."resource","episodeofcare"."id",'episodeOfCare')) "riskassessment_subselect") AS "risks",
             (SELECT json_agg(row_to_json("observation_subselect".*)) AS "observation"
              FROM (SELECT "obs"."id" AS "id",
                           "obs"."resource_type" AS "resource_type",
                           "obs"."status" AS "status",
                           "obs"."ts" AS "ts",
                           "obs"."txid" AS "txid",
                           ("obs".resource || jsonb_build_object('id',"obs".id,'resourceType',"obs".resource_type)) AS "resource"
                    FROM "observation" "obs"
                    WHERE "obs"."resource" @@ logic_revinclude("episodeofcare"."resource","episodeofcare"."id",'episodeOfCare')) "observation_subselect") AS "observs"
      FROM "episodeofcare"
      WHERE "id" = '3ad64c79-48c8-4d93-b24b-127c2ff24a8a') "episodeofcare_subselect"
LIMIT 100 OFFSET 0