EXPLAIN ANALYZE 
SELECT
	COALESCE(org.resource #>> '{alias,0}', org.resource ->> 'name') AS org
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		'2020-01-01' BETWEEN ct.resource #>> '{period,start}' AND COALESCE(ct.resource #>> '{period,end}', 'infinity')
	) AS count_start
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		ct.resource #>> '{period,start}' BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
	) AS count_in
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			ct.resource #>> '{period,start}' BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-start"))).value.Quantity.value') #>> '{}' AS integer) < 12
		)
	) AS count_in_before_12
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			ct.resource #>> '{period,start}' BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-start"))).value.Quantity.value') #>> '{}' AS integer) < 14
		)
	) AS count_in_before_14
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			ct.resource #>> '{period,start}' BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "observed-previously"))).value.boolean') #>> '{}' AS TEXT) = 'true'
		)
	) AS count_in_from
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			CAST(eoc.resource #>> '{period,end}' AS timestamp) BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND (
				jsonb_path_query_first(
					current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code'
				) #>> '{}' IN (
					'1', '2'
				)
			)
		)
	) AS count_out
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			CAST(eoc.resource #>> '{period,end}' AS timestamp) BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '1'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-type"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS integer) BETWEEN 22 AND 24
		)
	) AS count_out_22_24
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			CAST(eoc.resource #>> '{period,end}' AS timestamp) BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '1'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-type"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS integer) BETWEEN 25 AND 27
		)
	) AS count_out_25_27
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			CAST(eoc.resource #>> '{period,end}' AS timestamp) BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '1'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-type"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS integer) BETWEEN 28 AND 37
		)
	) AS count_out_28_37
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			CAST(eoc.resource #>> '{period,end}' AS timestamp) BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS integer) < 22
		)
	) AS count_out_break
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		(
			CAST(eoc.resource #>> '{period,end}' AS timestamp) BETWEEN '2020-01-01' AND '2020-11-09T23:59:59'
			AND jsonb_path_query_first(
				current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code'
			) #>> '{}' = '3'
		)
	) AS count_out_to
	, count(DISTINCT eoc.id) FILTER (
	WHERE
		'2020-11-09T23:59:59' BETWEEN ct.resource #>> '{period,start}' AND COALESCE(ct.resource #>> '{period,end}', 'infinity')
	) AS count_end
FROM
	careteam ct
INNER JOIN episodeofcare eoc ON
	(
		eoc.resource -> 'team' @@ logic_revinclude(
			ct.resource, ct.id, '#'
		)
		AND eoc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PregnantCard")'::jsquery
		AND ct.resource #>> '{period,start}' < COALESCE(ct.resource #>> '{period,end}', 'infinity')
	)
INNER JOIN organization org ON
	(
		org.resource @@ logic_include(
			ct.resource, 'managingOrganization[0]'
		)
		AND org.resource -> 'partOf' IS NULL
	)
LEFT JOIN observation current_pregnancy ON
	(
		current_pregnancy.resource -> 'episodeOfCare' @@ logic_revinclude(
			eoc.resource, eoc.id
		)
		AND current_pregnancy.resource -> 'category' @@ '#.coding.#(system="urn:CodeSystem:pregnancy" and code="current-pregnancy")'::jsquery
		AND current_pregnancy.resource #>> '{effective,dateTime}' BETWEEN ct.resource #>> '{period,start}' AND COALESCE(ct.resource #>> '{period,end}', 'infinity')
	)
WHERE
	(
		immutable_tsrange(
			ct.resource #>> '{period,start}', COALESCE(ct.resource #>> '{period,end}', 'infinity')
		) && immutable_tsrange(
			'2020-01-01', '2020-11-09T23:59:59'
		)
	)
GROUP BY
	org.id