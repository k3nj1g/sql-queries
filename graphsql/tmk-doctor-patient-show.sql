SELECT ROW_TO_JSON("servicerequest_subselect".*) AS "servicerequest"
FROM (SELECT "sr"."id" AS "id",
             "sr"."resource_type" AS "resource_type",
             "sr"."status" AS "status",
             "sr"."cts" AS "cts",
             "sr"."ts" AS "ts",
             "sr"."txid" AS "txid",
             ("sr".resource || JSONB_BUILD_OBJECT('id',"sr".id,'resourceType',"sr".resource_type)) AS "resource",
             (SELECT ROW_TO_JSON("patient_subselect".*) AS "patient"
              FROM (SELECT "p"."id" AS "id",
                           "p"."resource_type" AS "resource_type",
                           "p"."status" AS "status",
                           "p"."cts" AS "cts",
                           "p"."ts" AS "ts",
                           "p"."txid" AS "txid",
                           ("p".resource || JSONB_BUILD_OBJECT('id',"p".id,'resourceType',"p".resource_type)) AS "resource"
                    FROM "patient" "p"
                    WHERE (("p"."resource" @@ logic_include("sr"."resource",'subject') OR "p"."id" = ANY (ARRAY ((SELECT JSONB_PATH_QUERY("sr"."resource",'$.subject.id') #>> '{}')))) AND COALESCE("p"."resource" #>> '{active}','true') = 'true')) "patient_subselect") AS "patient",
             (SELECT ROW_TO_JSON("appointment_subselect".*) AS "appointment"
              FROM (SELECT "a"."id" AS "id",
                           "a"."resource_type" AS "resource_type",
                           "a"."status" AS "status",
                           "a"."cts" AS "cts",
                           "a"."ts" AS "ts",
                           "a"."txid" AS "txid",
                           ("a".resource || JSONB_BUILD_OBJECT('id',"a".id,'resourceType',"a".resource_type)) AS "resource"
                    FROM "appointment" "a"
                    WHERE ("a"."resource" @@ logic_include("sr"."resource",'supportingInfo') OR "a"."id" = ANY (ARRAY ((SELECT JSONB_PATH_QUERY("sr"."resource",'$.supportingInfo.id') #>> '{}'))))) "appointment_subselect") AS "appointment",
             (SELECT ROW_TO_JSON("documentreference_subselect".*) AS "documentreference"
              FROM (SELECT "d"."id" AS "id",
                           "d"."resource_type" AS "resource_type",
                           "d"."status" AS "status",
                           "d"."cts" AS "cts",
                           "d"."ts" AS "ts",
                           "d"."txid" AS "txid",
                           ("d".resource || JSONB_BUILD_OBJECT('id',"d".id,'resourceType',"d".resource_type)) AS "resource"
                    FROM "documentreference" "d"
                    WHERE ("d"."resource" @@ logic_include("sr"."resource",'supportingInfo') OR "d"."id" = ANY (ARRAY ((SELECT JSONB_PATH_QUERY("sr"."resource",'$.supportingInfo.id') #>> '{}'))))) "documentreference_subselect") AS "document",
             (SELECT ROW_TO_JSON("documentreference_subselect".*) AS "documentreference"
              FROM (SELECT "c"."id" AS "id",
                           "c"."resource_type" AS "resource_type",
                           "c"."status" AS "status",
                           "c"."cts" AS "cts",
                           "c"."ts" AS "ts",
                           "c"."txid" AS "txid",
                           ("c".resource || JSONB_BUILD_OBJECT('id',"c".id,'resourceType',"c".resource_type)) AS "resource"
                    FROM "documentreference" "c"
                    WHERE "c"."resource" @@ logic_revinclude("sr"."resource","sr"."id",'context.related.#')) "documentreference_subselect") AS "conclusion"
      FROM "servicerequest" "sr"
      WHERE "sr"."id" = 'ea27f250-7ad6-422c-b675-62108cbaa05a') "servicerequest_subselect";