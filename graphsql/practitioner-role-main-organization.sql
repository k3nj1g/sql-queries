SELECT ROW_TO_JSON("practitionerrole_subselect".*) AS "practitionerrole"
FROM (SELECT "pr"."id" AS "id",
             "pr"."resource_type" AS "resource_type",
             "pr"."status" AS "status",
             "pr"."cts" AS "cts",
             "pr"."ts" AS "ts",
             "pr"."txid" AS "txid",
             ("pr".resource || JSONB_BUILD_OBJECT('id',"pr".id,'resourceType',"pr".resource_type)) AS "resource",
             (SELECT ROW_TO_JSON("practitioner_subselect".*) AS "practitioner"
              FROM (SELECT "practitioner"."id" AS "id",
                           "practitioner"."resource_type" AS "resource_type",
                           "practitioner"."status" AS "status",
                           "practitioner"."cts" AS "cts",
                           "practitioner"."ts" AS "ts",
                           "practitioner"."txid" AS "txid",
                           ("practitioner".resource || JSONB_BUILD_OBJECT('id',"practitioner".id,'resourceType',"practitioner".resource_type)) AS "resource"
                    FROM "practitioner" "practitioner"
                    WHERE ("practitioner"."resource" @@ logic_include("pr"."resource",'practitioner') OR "practitioner"."id" = ANY (ARRAY ((SELECT JSONB_PATH_QUERY("pr"."resource",'$.practitioner.id') #>> '{}'))))) "practitioner_subselect") AS "practitioner"
             (SELECT ROW_TO_JSON("organization_subselect".*) AS "organization"
              FROM (SELECT "o"."id" AS "id",
                           "o"."resource_type" AS "resource_type",
                           "o"."status" AS "status",
                           "o"."cts" AS "cts",
                           "o"."ts" AS "ts",
                           "o"."txid" AS "txid",
                           ("o".resource || JSONB_BUILD_OBJECT('id',"o".id,'resourceType',"o".resource_type)) AS "resource",
                           (SELECT ROW_TO_JSON("organization_subselect".*) AS "organization"
                            FROM (SELECT "mo"."id" AS "id",
                                         "mo"."resource_type" AS "resource_type",
                                         "mo"."status" AS "status",
                                         "mo"."cts" AS "cts",
                                         "mo"."ts" AS "ts",
                                         "mo"."txid" AS "txid",
                                         ("mo".resource || JSONB_BUILD_OBJECT('id',"mo".id,'resourceType',"mo".resource_type)) AS "resource",
                                         (SELECT ROW_TO_JSON("organizationinfo_subselect".*) AS "organizationinfo"
                                          FROM (SELECT "moi"."id" AS "id",
                                                       "moi"."resource_type" AS "resource_type",
                                                       "moi"."status" AS "status",
                                                       "moi"."cts" AS "cts",
                                                       "moi"."ts" AS "ts",
                                                       "moi"."txid" AS "txid",
                                                       ("moi".resource || JSONB_BUILD_OBJECT('id',"moi".id,'resourceType',"moi".resource_type)) AS "resource"
                                                FROM "organizationinfo" "moi"
                                                WHERE "moi"."id" = "mo"."id") "organizationinfo_subselect") AS "mainOrganizationInfo"
                                  FROM "organization" "mo"
                                  WHERE ("mo"."resource" @@ logic_include("o"."resource",'mainOrganization') OR "mo"."id" = o.resource #>> '{mainOrganization,id}')) "organization_subselect") AS "mainOrganization",
                           (SELECT ROW_TO_JSON("organizationinfo_subselect".*) AS "organizationinfo"
                            FROM (SELECT "oi"."id" AS "id",
                                         "oi"."resource_type" AS "resource_type",
                                         "oi"."status" AS "status",
                                         "oi"."cts" AS "cts",
                                         "oi"."ts" AS "ts",
                                         "oi"."txid" AS "txid",
                                         ("oi".resource || JSONB_BUILD_OBJECT('id',"oi".id,'resourceType',"oi".resource_type)) AS "resource"
                                  FROM "organizationinfo" "oi"
                                  WHERE "oi"."id" = "o"."id") "organizationinfo_subselect") AS "organizationInfo"
                    FROM "organization" "o"
                    WHERE (("o"."resource" @@ logic_include("pr"."resource",'organization') OR "o"."id" = pr.resource #>> '{organization,id}') AND COALESCE("o"."resource" ->> 'active','true') = 'true')) "organization_subselect") AS "organization"
      FROM "practitionerrole" "pr"
      WHERE "id" = 'f48538b6-4617-4d12-85a1-31b0c873b78b') "practitionerrole_subselect"