EXPLAIN ANALYZE 
SELECT
	row_to_json("servicerequest_subselect" .*) AS "servicerequest"
FROM
	(
	SELECT
		"id",
		"resource_type",
		"status",
		"ts",
		"txid",
		(resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS "resource",
		(
		SELECT
			row_to_json("patient_subselect" .*) AS "patient"
		FROM
			(
			SELECT
				"id",
				"resource_type",
				"status",
				"ts",
				"txid",
				(resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS "resource"
			FROM
				"patient" "p"
			WHERE
				("p"."resource" @@ logic_include("sr"."resource",
				'subject',
				NULL)
				OR "p"."id" = ANY(ARRAY((
				SELECT
					jsonb_path_query("sr"."resource",
					'$.subject.id') #>> '{}'))))) "patient_subselect") AS "patient",
		(
		SELECT
			row_to_json("organization_subselect" .*) AS "organization"
		FROM
			(
			SELECT
				"id",
				"resource_type",
				"status",
				"ts",
				"txid",
				(resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS "resource"
			FROM
				"organization" "org"
			WHERE
				("org"."resource" @@ logic_include("sr"."resource",
				'performer[*]',
				NULL)
				OR "org"."id" = ANY(ARRAY((
				SELECT
					jsonb_path_query("sr"."resource",
					'$.performer[*].id') #>> '{}'))))
			LIMIT 1) "organization_subselect") AS "performer",
		(
		SELECT
			row_to_json("specimen_subselect" .*) AS "specimen"
		FROM
			(
			SELECT
				"id",
				"resource_type",
				"status",
				"ts",
				"txid",
				(resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS "resource"
			FROM
				"specimen" "s"
			WHERE
				("s"."resource" @@ logic_include("sr"."resource",
				'specimen[*]',
				NULL)
				OR "s"."id" = ANY(ARRAY((
				SELECT
					jsonb_path_query("sr"."resource",
					'$.specimen[*].id') #>> '{}'))))
			ORDER BY "s".ts					
			LIMIT 1) "specimen_subselect") AS "material",
		(
		SELECT
			json_agg(row_to_json("diagnosticreport_subselect" .*)) AS "diagnosticreport"
		FROM
			(
			SELECT
				"id",
				"resource_type",
				"status",
				"ts",
				"txid",
				(resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS "resource",
				(
				SELECT
					json_agg(row_to_json("observation_subselect" .*)) AS "observation"
				FROM
					(
					SELECT
						"id",
						"resource_type",
						"status",
						"ts",
						"txid",
						(resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS "resource"
					FROM
						"observation" "obs"
					WHERE
						("obs"."resource" @@ logic_include("dr"."resource",
						'result[*]',
						NULL)
						OR "obs"."id" = ANY(ARRAY((
						SELECT
							jsonb_path_query("dr"."resource",
							'$.result[*].id') #>> '{}'))))) "observation_subselect") AS "results",
				(
				SELECT
					row_to_json("specimen_subselect" .*) AS "specimen"
				FROM
					(
					SELECT
						"id",
						"resource_type",
						"status",
						"ts",
						"txid",
						(resource || jsonb_build_object('id', id , 'resourceType', resource_type)) AS "resource"
					FROM
						"specimen" "s"
					WHERE
						("s"."resource" @@ logic_include("dr"."resource",
						'specimen[*]',
						NULL)
						OR "s"."id" = ANY(ARRAY((
						SELECT
							jsonb_path_query("dr"."resource",
							'$.specimen[*].id') #>> '{}'))))
					ORDER BY "s".ts
					LIMIT 1) "specimen_subselect") AS "material"
			FROM
				"diagnosticreport" "dr"
			WHERE
				"dr"."resource" @@ logic_revinclude("sr"."resource",
				"sr"."id",
				'basedOn.#',
				' and not (status = "cancelled" and not conclusion = *)')) "diagnosticreport_subselect") AS "reports"
	FROM
		"servicerequest" "sr"
	WHERE
		"sr"."id" = 'cba7bbf4-a392-492f-a05e-149f0cd41d33') "servicerequest_subselect"