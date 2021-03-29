EXPLAIN ANALYZE 
SELECT row_to_json( "episodeofcare_subselect".*) AS  "episodeofcare"
FROM (SELECT  "id",
              "resource_type",
              "status",
              "ts",
              "txid",
             (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource",
             (SELECT json_agg(row_to_json( "careteam_subselect".*)) AS  "careteam"
              FROM (SELECT  "id",
                            "resource_type",
                            "status",
                            "ts",
                            "txid",
                           (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource"
                    FROM  "careteam"
                    WHERE ( "careteam". "resource" @@ logic_include( "episodeofcare". "resource",'team[*]') OR  "careteam". "id" = ANY (ARRAY ((SELECT jsonb_path_query( "episodeofcare". "resource",'$.team[*].id') #>> '{}')))))  "careteam_subselect") AS  "careteam",
             (SELECT row_to_json( "patient_subselect".*) AS  "patient"
              FROM (SELECT  "id",
                            "resource_type",
                            "status",
                            "ts",
                            "txid",
                           (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource",
                           (SELECT json_agg(row_to_json( "encounter_subselect".*)) AS  "encounter"
                            FROM (SELECT  "id",
                                          "resource_type",
                                          "status",
                                          "ts",
                                          "txid",
                                         (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource",
                                         (SELECT json_agg(row_to_json( "documentreference_subselect".*)) AS  "documentreference"
                                          FROM (SELECT  "id",
                                                        "resource_type",
                                                        "status",
                                                        "ts",
                                                        "txid",
                                                       (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource"
                                                FROM  "documentreference"  "dr"
                                                WHERE ( "dr". "resource" @@ logic_revinclude( "enc". "resource", "enc". "id",'context.encounter.#') AND coalesce( "dr". "resource" ->> 'active','true') = 'true')
                                                ORDER BY cast( "dr". "resource" ->> 'date' AS  "timestamptz") DESC NULLS last LIMIT 6)  "documentreference_subselect") AS  "document"
                                  FROM  "encounter"  "enc"
                                  WHERE ( "enc". "resource" @@ logic_revinclude( "pt". "resource", "pt". "id",'subject') AND cast( "enc". "resource" #>> '{period,start}' AS  "timestamp") BETWEEN cast( "episodeofcare". "resource" #>> '{period,start}' AS  "timestamp") AND coalesce(cast( "episodeofcare". "resource" #>> '{period,end}' AS  "timestamp"),(cast( "episodeofcare". "resource" #>> '{period,start}' AS  "timestamp") + '9 month'::interval))))  "encounter_subselect") AS  "encounter",
                           (SELECT json_agg(row_to_json( "patient_subselect".*)) AS  "patient"
                            FROM (SELECT  "id",
                                          "resource_type",
                                          "status",
                                          "ts",
                                          "txid",
                                         (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource",
                                         (SELECT row_to_json( "patient_subselect".*) AS  "patient"
                                          FROM (SELECT  "id",
                                                        "resource_type",
                                                        "status",
                                                        "ts",
                                                        "txid",
                                                       (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource"
                                                FROM  "patient"  "child"
                                                WHERE  "child". "resource" @@ cast(concat('identifier.#(system = "urn:identity:newborn:Patient" and value = ',jsonb_path_query_first( "newborn". "resource",'$.identifier [*] ? (@.system == "urn:identity:newborn:Patient").value'),') and identifier.#(system = "urn:source:tfoms:Patient") and active = true') AS  "jsquery")
--                                                ORDER BY "child".ts
                                                LIMIT 1)  "patient_subselect") AS  "child"
                                  FROM  "patient"  "newborn"
                                  WHERE ( "newborn". "resource" @@ logic_revinclude( "pt". "resource", "pt". "id",'extension.#.extension.#.valueReference',' and extension.#(url="urn:extension:patient-type" and valueCode="newborn")') AND  "newborn". "resource" ->> 'birthDate' >  "episodeofcare". "resource" #>> '{period,start}'))  "patient_subselect") AS  "newborn",
                           (SELECT json_agg(row_to_json( "episodeofcare_subselect".*)) AS  "episodeofcare"
                            FROM (SELECT  "id",
                                          "resource_type",
                                          "status",
                                          "ts",
                                          "txid",
                                         (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource",
                                         (SELECT row_to_json( "observation_subselect".*) AS  "observation"
                                          FROM (SELECT  "id",
                                                        "resource_type",
                                                        "status",
                                                        "ts",
                                                        "txid",
                                                       (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource"
                                                FROM  "observation"  "pregnancies"
                                                WHERE  "pregnancies". "resource" @@ logic_revinclude( "previous". "resource", "previous". "id",'episodeOfCare', ' and category.#.coding.#(system="urn:CodeSystem:pregnancy" and code="current-pregnancy") and code.coding.#(system="urn:CodeSystem:pregnancy-information" and code="pregnancies")')
                                                ORDER BY  "pregnancies". "resource" ->> 'status' LIMIT 1)  "observation_subselect") AS  "num"
                                  FROM  "episodeofcare"  "previous"
                                  WHERE  "previous". "resource" @@ logic_revinclude( "pt". "resource", "pt". "id",'patient', ' and type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PregnantCard")'))  "episodeofcare_subselect") AS  "previous"
                    FROM  "patient"  "pt"
                    WHERE (coalesce( "pt". "resource" ->> 'active','true') = 'true' AND ( "pt". "resource" @@ logic_include( "episodeofcare". "resource", 'patient') OR  "pt". "id" = episodeofcare.resource #>> '{patient,id}')))  "patient_subselect") AS  "patient",
             (SELECT json_agg(row_to_json( "riskassessment_subselect".*)) AS  "riskassessment"
              FROM (SELECT  "id",
                            "resource_type",
                            "status",
                            "ts",
                            "txid",
                           (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource"
                    FROM  "riskassessment"  "risk"
                    WHERE  "risk". "resource" @@ logic_revinclude( "episodeofcare". "resource", "episodeofcare". "id",'episodeOfCare'))  "riskassessment_subselect") AS  "risks",
             (SELECT json_agg(row_to_json( "observation_subselect".*)) AS  "observation"
              FROM (SELECT  "id",
                            "resource_type",
                            "status",
                            "ts",
                            "txid",
                           (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS  "resource"
                    FROM  "observation"  "obs"
                    WHERE  "obs". "resource" @@ logic_revinclude( "episodeofcare". "resource", "episodeofcare". "id",'episodeOfCare'))  "observation_subselect") AS  "observs"
      FROM  "episodeofcare"
      WHERE  "id" = '849c96ff-5319-4a59-a014-85ef3bd3c176')  "episodeofcare_subselect"