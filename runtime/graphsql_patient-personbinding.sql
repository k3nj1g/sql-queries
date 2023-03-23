SELECT row_to_json("patient_subselect".*) AS "patient"
FROM (
        SELECT "p"."id" AS "id",
            "p"."resource_type" AS "resource_type",
            "p"."status" AS "status",
            "p"."cts" AS "cts",
            "p"."ts" AS "ts",
            "p"."txid" AS "txid",
            (
                "p".resource || jsonb_build_object('id', "p".id, 'resourceType', "p".resource_type)
            ) AS "resource",
            (
                SELECT row_to_json("personbinding_subselect".*) AS "personbinding"
                FROM (
                        SELECT "pb"."id" AS "id",
                            "pb"."resource_type" AS "resource_type",
                            "pb"."status" AS "status",
                            "pb"."cts" AS "cts",
                            "pb"."ts" AS "ts",
                            "pb"."txid" AS "txid",
                            (
                                "pb".resource || jsonb_build_object(
                                    'id',
                                    "pb".id,
                                    'resourceType',
                                    "pb".resource_type
                                )
                            ) AS "resource",
                            (
                                SELECT row_to_json("sector_subselect".*) AS "sector"
                                FROM (
                                        SELECT "s"."id" AS "id",
                                            "s"."resource_type" AS "resource_type",
                                            "s"."status" AS "status",
                                            "s"."cts" AS "cts",
                                            "s"."ts" AS "ts",
                                            "s"."txid" AS "txid",
                                            (
                                                "s".resource || jsonb_build_object('id', "s".id, 'resourceType', "s".resource_type)
                                            ) AS "resource",
                                            (
                                                SELECT json_agg(row_to_json("organization_subselect".*)) AS "organization"
                                                FROM (
                                                        SELECT "org"."id" AS "id",
                                                            "org"."resource_type" AS "resource_type",
                                                            "org"."status" AS "status",
                                                            "org"."cts" AS "cts",
                                                            "org"."ts" AS "ts",
                                                            "org"."txid" AS "txid",
                                                            (
                                                                "org".resource || jsonb_build_object(
                                                                    'id',
                                                                    "org".id,
                                                                    'resourceType',
                                                                    "org".resource_type
                                                                )
                                                            ) AS "resource"
                                                        FROM "organization" "org"
                                                        WHERE (
                                                                "org"."resource" @@ logic_include("s"."resource", 'organization')
                                                                OR "org"."id" = s.resource#>>'{organization,id}'
                                                            )
                                                    ) "organization_subselect"
                                            ) AS "organization"
                                        FROM "sector" "s"
                                        WHERE (
                                                "s"."resource" @@ logic_include("pb"."resource", 'sector')
                                                OR "s"."id" = pb.resource#>>'{sector,id}'
                                            )
                                    ) "sector_subselect"
                            ) AS "sector"
                        FROM "personbinding" "pb"
                        WHERE "pb"."resource" @@ logic_revinclude("p"."resource", "p"."id", 'subject')
                    ) "personbinding_subselect"
            ) AS "personbinding",
            (
                SELECT json_agg(row_to_json("task_subselect".*)) AS "task"
                FROM (
                        SELECT "t"."id" AS "id",
                            "t"."resource_type" AS "resource_type",
                            "t"."status" AS "status",
                            "t"."cts" AS "cts",
                            "t"."ts" AS "ts",
                            "t"."txid" AS "txid",
                            (
                                "t".resource || jsonb_build_object('id', "t".id, 'resourceType', "t".resource_type)
                            ) AS "resource"
                        FROM "task" "t"
                        WHERE "t"."resource" @@ logic_revinclude("p"."resource", "p"."id", 'for')
                        ORDER BY knife_extract_max_timestamptz(
                                "t"."resource",
                                '[["executionPeriod","end"],["executionPeriod","start"]]'
                            ) DESC NULLS LAST
                        LIMIT 2
                    ) "task_subselect"
            ) AS "tasks",
            (
                SELECT json_agg(row_to_json("relatedperson_subselect".*)) AS "relatedperson"
                FROM (
                        SELECT "rp"."id" AS "id",
                            "rp"."resource_type" AS "resource_type",
                            "rp"."status" AS "status",
                            "rp"."cts" AS "cts",
                            "rp"."ts" AS "ts",
                            "rp"."txid" AS "txid",
                            (
                                "rp".resource || jsonb_build_object(
                                    'id',
                                    "rp".id,
                                    'resourceType',
                                    "rp".resource_type
                                )
                            ) AS "resource"
                        FROM "relatedperson" "rp"
                        WHERE "rp"."resource" @@ logic_revinclude("p"."resource", "p"."id", 'patient')
                    ) "relatedperson_subselect"
            ) AS "relatedperson"
        FROM "patient" "p"
        WHERE "p"."id" = 'f18c0948-0032-4b72-8b90-0bda9ac364b6'
    ) "patient_subselect"
LIMIT 100
