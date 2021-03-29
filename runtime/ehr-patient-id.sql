--EXPLAIN ANALYZE 
SELECT row_to_json("patient_subselect".*) AS "patient"
FROM (SELECT "id",
             "resource_type",
             "status",
             "ts",
             "txid",
             (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource",
             (SELECT json_agg(row_to_json("condition_subselect".*)) AS "condition"
              FROM (SELECT "id",
                           "resource_type",
                           "status",
                           "ts",
                           "txid",
                           (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource"
                    FROM (SELECT "res"."agg" ->> 'id' AS "id",
                                 "res"."agg" ->> 'txid' AS "txid",
                                 cast("res"."agg" ->> 'ts' AS "timestamptz") AS "ts",
                                 "res"."agg" ->> 'resource_type' AS "resource_type",
                                 "res"."agg" ->> 'status' AS "status",
                                 "res"."agg" -> 'resource' AS "resource",
                                 cast("res"."agg" ->> 'cts' AS "timestamptz") AS "cts"
                          FROM (WITH 
                      				"_rules" AS (
                      					SELECT jsonb_array_elements("concept"."resource" #> '{property,condition}') AS "_rule"
                          				FROM "concept" "concept"
		                          		WHERE ("concept"."resource" #>> '{system}' = 'urn:CodeSystem:r21.resource-tag' 
		                          			AND "concept"."resource" #>> '{code}' = 'Condition'))
	                          		,"rules" AS (
	                          			SELECT "r"."_rule" #>> '{mkb-from,code}' AS "mkb_from",
                                               "r"."_rule" #>> '{mkb-to,code}' AS "mkb_to"
                                        FROM "_rules" "r")
                                    ,"grouped" AS (
                                    	SELECT jsonb_agg(row_to_json("cond".*)) AS "agg"
                                        FROM "condition" "cond"
                                        WHERE ("cond"."resource" @@ logic_revinclude("patient"."resource","patient"."id",'subject')
                                        	AND (SELECT 1
                                                 FROM "rules" "r"
                                                 WHERE (((knife_extract_text(cond.resource,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'))[1] >= "r"."mkb_from" 
                                                 	AND (knife_extract_text(cond.resource,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'))[1] <= "r"."mkb_to")
                                                 	OR ((knife_extract_text(cond.resource,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'))[1] LIKE concat("r"."mkb_to",'%')))
                                                 LIMIT 1) IS NOT NULL 
                                        	AND (SELECT mkb_10
                                                 FROM unnest(knife_extract_text (cond.resource,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                                                 WHERE mkb_10 SIMILAR TO '(B2[0-4])%'
                                               	 LIMIT 1) IS NULL 
                                       	    AND (SELECT mkb_10
                                                 FROM unnest(knife_extract_text (cond.resource,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                                                 WHERE mkb_10 SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                                                 LIMIT 1) IS NULL 
                                            AND (SELECT mkb_10
                                                 FROM unnest(knife_extract_text (cond.resource,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                                                 WHERE mkb_10 SIMILAR TO '(F0[0-7]|F1|F2[03]|F7[0-389])%'
                                                 LIMIT 1) IS NULL
                                            AND lower(patient.resource #>> '{name,0,given,0}') = CASE 
                                            													     WHEN (cond.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(cond.resource #>> '{subject,display}', ',', 1), ' ' , 2)) 
                                            													     ELSE lower(patient.resource #>> '{name,0,given,0}') 
                    	                    												     END 
	   					                    AND COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') = CASE 
                                            													     				WHEN (cond.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(cond.resource #>> '{subject,display}', ',', 1), ' ' , 3)) 
						                    																		ELSE COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') 
						                    																	END
						                    AND (patient.resource #>> '{birthDate}')::date = CASE 
                                            											 	 	WHEN (cond.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN to_date(split_part(cond.resource #>> '{subject,display}', ',', 2), 'DD.mm.YYYY')
                                            											 	 	ELSE (patient.resource #>> '{birthDate}')::date
                    	                    										 	 	 END)
                                                                GROUP BY knife_extract_text(cond.resource::jsonb,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'::jsonb))
                                                    SELECT (SELECT "grouped_agg"
                                                            FROM jsonb_array_elements("grouped"."agg") "grouped_agg"
                                                            ORDER BY "grouped_agg" #>> '{resource,recordedDate}' DESC NULLS last LIMIT 1) AS "agg" FROM "grouped") "res") "condition"
                    ORDER BY knife_extract_text(condition.resource::jsonb,'[["code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]'::jsonb) LIMIT NULL) "condition_subselect") AS "conditionRegistry",
             (SELECT json_agg(row_to_json("encounter_subselect".*)) AS "encounter"
              FROM (SELECT "id",
                           "resource_type",
                           "status",
                           "ts",
                           "txid",
                           (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource"
                    FROM "encounter"
                    WHERE ("encounter"."resource" @@ logic_revinclude("patient"."resource","patient"."id",'subject') 
                    	AND (SELECT mkb_10
                          	 FROM unnest(knife_extract_text (encounter.resource,'[["contained",{},"code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                          	 WHERE mkb_10 SIMILAR TO '(B2[0-4])%'
                          	 LIMIT 1) IS NULL 
                  	    AND (SELECT mkb_10
                             FROM unnest(knife_extract_text (encounter.resource,'[["contained",{},"code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                             WHERE mkb_10 SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                             LIMIT 1) IS NULL 
                        AND (SELECT mkb_10
                             FROM unnest(knife_extract_text (encounter.resource,'[["contained",{},"code","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                             WHERE mkb_10 SIMILAR TO '(F0[0-7]|F1|F2[03]|F7[0-389])%'
                             LIMIT 1) IS NULL
                        AND lower(patient.resource #>> '{name,0,given,0}') = CASE 
                        													     WHEN (encounter.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(encounter.resource #>> '{subject,display}', ',', 1), ' ' , 2)) 
                        													     ELSE lower(patient.resource #>> '{name,0,given,0}') 
                    													     END 
	   					AND COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') = CASE 
                        													     				WHEN (encounter.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(encounter.resource #>> '{subject,display}', ',', 1), ' ' , 3)) 
																								ELSE COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') 
																							END
						AND (patient.resource #>> '{birthDate}')::date = CASE 
                        											 	 	WHEN (encounter.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN to_date(split_part(encounter.resource #>> '{subject,display}', ',', 2), 'DD.mm.YYYY')
                        											 	 	ELSE (patient.resource #>> '{birthDate}')::date
                    											 	 	 END)
                    ORDER BY cast("encounter"."resource" #>> '{period,start}' AS "timestamptz") DESC NULLS first LIMIT 6) "encounter_subselect") AS "encounter",
            (SELECT json_agg(row_to_json("documentreference_subselect".*)) AS "documentreference"
             FROM (SELECT "id",
                          "resource_type",
                          "status",
                          "ts",
                          "txid",
                          (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource"
                   FROM "documentreference"
                   WHERE ("documentreference"."resource" @@ logic_revinclude("patient"."resource","patient"."id",'subject',' and (not status = "superseded") and (not docStatus = "superseded") and (not category.#.coding.#(system= "urn:CodeSystem:medrecord-group" and code = "TMK-service-request-attachment" or code = "result-mse" or code = "referral-mse"))') 
                       AND coalesce(documentreference.resource ->> 'active','true') = 'true' 
                       AND (SELECT mkb_10
                            FROM unnest(knife_extract_text (documentreference.resource,'[["extension",{"url":"urn:extension:diagnosis"},"extension",{"url":"mkb"},"valueCodeableConcept","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                            WHERE mkb_10 SIMILAR TO '(B2[0-4])%'
                            LIMIT 1) IS NULL
                       AND (SELECT mkb_10
                            FROM unnest(knife_extract_text (documentreference.resource,'[["extension",{"url":"urn:extension:diagnosis"},"extension",{"url":"mkb"},"valueCodeableConcept","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                            WHERE mkb_10 SIMILAR TO '(A5[0-469]|A6[03]|Z22.[48]|Z11.3|Z71.2|Z86.1|Z20.2|N89|N34.1|B37.3|B37.4)%'
                            LIMIT 1) IS NULL
                       AND (SELECT mkb_10
                            FROM unnest(knife_extract_text (documentreference.resource,'[["extension",{"url":"urn:extension:diagnosis"},"extension",{"url":"mkb"},"valueCodeableConcept","coding",{"system":"urn:CodeSystem:icd-10"},"code"]]')) mkb_10
                            WHERE mkb_10 SIMILAR TO '(F0[0-7]|F1|F2[03]|F7[0-389])%'
		                    LIMIT 1) IS NULL
                       AND lower(patient.resource #>> '{name,0,given,0}') = CASE 
                       													        WHEN (documentreference.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(documentreference.resource #>> '{subject,display}', ',', 1), ' ' , 2)) 
                       													        ELSE lower(patient.resource #>> '{name,0,given,0}') 
                       												        END 
	   				   AND COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') = CASE 
                       													     				   WHEN (documentreference.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(documentreference.resource #>> '{subject,display}', ',', 1), ' ' , 3)) 
					   																		   ELSE COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') 
					   																	   END
					   AND (patient.resource #>> '{birthDate}')::date = CASE 
                       											 	 	    WHEN (documentreference.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN to_date(split_part(documentreference.resource #>> '{subject,display}', ',', 2), 'DD.mm.YYYY')
                       											 	 	    ELSE (patient.resource #>> '{birthDate}')::date
                       										 	 	    END)
                   ORDER BY cast("documentreference"."resource" ->> 'date' AS "timestamptz") DESC NULLS LAST 
                   LIMIT 6) "documentreference_subselect") AS "documentReference",
            (SELECT json_agg(row_to_json("documentreference_subselect".*)) AS "documentreference"
             FROM (SELECT "id",
                          "resource_type",
                          "status",
                          "ts",
                          "txid",
                          (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource"
                   FROM "documentreference"
                   WHERE "documentreference"."resource" @@ logic_revinclude("patient"."resource","patient"."id",'subject','and (not status = "draft") and category.#.coding.#(code = "result-mse" or code = "referral-mse")')
                       AND lower(patient.resource #>> '{name,0,given,0}') = CASE 
                                                                               WHEN (documentreference.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(documentreference.resource #>> '{subject,display}', ',', 1), ' ' , 2)) 
                                                                               ELSE lower(patient.resource #>> '{name,0,given,0}') 
                                                                           END 
                       AND COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') = CASE 
                                                                                               WHEN (documentreference.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(documentreference.resource #>> '{subject,display}', ',', 1), ' ' , 3)) 
                                                                                               ELSE COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') 
                                                                                           END
                       AND (patient.resource #>> '{birthDate}')::date = CASE 
                                                                           WHEN (documentreference.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN to_date(split_part(documentreference.resource #>> '{subject,display}', ',', 2), 'DD.mm.YYYY')
                                                                           ELSE (patient.resource #>> '{birthDate}')::date
                                                                       END                  
                   ORDER BY cast("documentreference"."resource" ->> 'date' AS "timestamptz") DESC NULLS last LIMIT 6) "documentreference_subselect") AS "mse",
            (SELECT json_agg(row_to_json("servicerequest_subselect".*)) AS "servicerequest"
             FROM (SELECT "id",
                          "resource_type",
                          "status",
                          "ts",
                          "txid",
                          (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource",
                          (SELECT row_to_json("diagnosticreport_subselect".*) AS "diagnosticreport"
                           FROM (SELECT "id",
                                        "resource_type",
                                        "status",
                                        "ts",
                                        "txid",
                                        (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource"
                                 FROM "diagnosticreport"
                                 WHERE "diagnosticreport"."resource" @@ logic_revinclude("servicerequest"."resource","servicerequest"."id",'basedOn.#')
                                 ORDER BY "diagnosticreport".ts DESC 
                                 LIMIT 1) "diagnosticreport_subselect") AS "diagnosticReport"
                   FROM "servicerequest"
                   WHERE ("servicerequest"."resource" @@ logic_revinclude("patient"."resource","patient"."id",'subject') AND "servicerequest"."resource" @@ 'category.#.coding.#(system= "urn:CodeSystem:servicerequest-category" and code = "Referral-LMI")'::jsquery)
                       AND lower(patient.resource #>> '{name,0,given,0}') = CASE 
                       													        WHEN (servicerequest.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(servicerequest.resource #>> '{subject,display}', ',', 1), ' ' , 2)) 
                       													        ELSE lower(patient.resource #>> '{name,0,given,0}') 
                       												        END 
	   				   AND COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') = CASE 
                       													     				   WHEN (servicerequest.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(servicerequest.resource #>> '{subject,display}', ',', 1), ' ' , 3)) 
					   																		   ELSE COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') 
					   																	   END
					   AND (patient.resource #>> '{birthDate}')::date = CASE 
                       											 	 	    WHEN (servicerequest.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN to_date(split_part(servicerequest.resource #>> '{subject,display}', ',', 2), 'DD.mm.YYYY')
                       											 	 	    ELSE (patient.resource #>> '{birthDate}')::date
                       										 	 	    END                  
                   ORDER BY cast("servicerequest"."resource" ->> 'authoredOn' AS "timestamptz") DESC NULLS first LIMIT 6) "servicerequest_subselect") AS "laboratory",
            (SELECT json_agg(row_to_json("servicerequest_subselect".*)) AS "servicerequest"
             FROM (SELECT "id",
                          "resource_type",
                          "status",
                          "ts",
                          "txid",
                          (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource",
                          (SELECT row_to_json("diagnosticreport_subselect".*) AS "diagnosticreport"
                           FROM (SELECT "id",
                                        "resource_type",
                                        "status",
                                        "ts",
                                        "txid",
                                        (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource"
                                 FROM "diagnosticreport"
                                 WHERE "diagnosticreport"."resource" @@ logic_revinclude("servicerequest"."resource","servicerequest"."id",'basedOn.#')
								  ORDER BY "diagnosticreport".ts DESC 
                                 LIMIT 1) "diagnosticreport_subselect") AS "diagnosticReport"
                   FROM "servicerequest"
                   WHERE ("servicerequest"."resource" @@ logic_revinclude("patient"."resource","patient"."id",'subject') AND "servicerequest"."resource" @@ 'category.#.coding.#(system= "urn:CodeSystem:servicerequest-category" and code = "Referral-IMI")'::jsquery)
                       AND lower(patient.resource #>> '{name,0,given,0}') = CASE 
                       													        WHEN (servicerequest.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(servicerequest.resource #>> '{subject,display}', ',', 1), ' ' , 2)) 
                       													        ELSE lower(patient.resource #>> '{name,0,given,0}') 
                       												        END 
	   				   AND COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') = CASE 
                       													     				   WHEN (servicerequest.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(servicerequest.resource #>> '{subject,display}', ',', 1), ' ' , 3)) 
					   																		   ELSE COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') 
					   																	   END
					   AND (patient.resource #>> '{birthDate}')::date = CASE 
                       											 	 	    WHEN (servicerequest.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN to_date(split_part(servicerequest.resource #>> '{subject,display}', ',', 2), 'DD.mm.YYYY')
                       											 	 	    ELSE (patient.resource #>> '{birthDate}')::date
                       										 	 	    END                         
                   ORDER BY cast("servicerequest"."resource" ->> 'authoredOn' AS "timestamptz") DESC NULLS first LIMIT 6) "servicerequest_subselect") AS "instrumental",
            (SELECT json_agg(row_to_json("diagnosticreport_subselect".*)) AS "diagnosticreport"
             FROM (SELECT "id",
                          "resource_type",
                          "status",
                          "ts",
                          "txid",
                          (resource || jsonb_build_object('id',id,'resourceType',resource_type)) AS "resource"
                   FROM "diagnosticreport"
                   WHERE ("diagnosticreport"."resource" @@ logic_revinclude("patient"."resource","patient"."id",'subject') 
                       AND resource ?? 'radiationDose'
                       AND lower(patient.resource #>> '{name,0,given,0}') = CASE 
                       													        WHEN (diagnosticreport.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(diagnosticreport.resource #>> '{subject,display}', ',', 1), ' ' , 2)) 
                       													        ELSE lower(patient.resource #>> '{name,0,given,0}') 
                       												        END 
	   				   AND COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') = CASE 
                       													     				   WHEN (diagnosticreport.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN lower(split_part(split_part(diagnosticreport.resource #>> '{subject,display}', ',', 1), ' ' , 3)) 
					   																		   ELSE COALESCE (lower(patient.resource #>> '{name,0,given,1}'), '') 
					   																	   END
					   AND (patient.resource #>> '{birthDate}')::date = CASE 
                       											 	 	    WHEN (diagnosticreport.resource #>> '{subject,identifier,value}') = '0000000000000000' THEN to_date(split_part(diagnosticreport.resource #>> '{subject,display}', ',', 2), 'DD.mm.YYYY')
                       											 	 	    ELSE (patient.resource #>> '{birthDate}')::date
                       										 	 	    END      
                       )) "diagnosticreport_subselect") AS "xRay"
      FROM "patient"
      WHERE "id" = '0640c4d8-ae1b-4960-8d3f-74523c60dce5') "patient_subselect"