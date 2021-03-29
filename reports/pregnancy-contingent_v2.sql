SELECT coalesce(org.resource #>> '{alias,0}', org.resource ->> 'name') AS org
	, count(DISTINCT eoc.id) FILTER(WHERE '2020-01-01' BETWEEN ct.resource #>> '{period,start}'
			AND ct.resource #>> '{period,end}') AS count_start
	, count(DISTINCT eoc.id) FILTER(WHERE ct.resource #>> '{period,start}' BETWEEN '2020-01-01'
			AND '2020-11-12T23:59:59') AS count_in
	, count(DISTINCT eoc.id) FILTER(WHERE (
			ct.resource #>> '{period,start}' BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-start"))).value.Quantity.value') #>> '{}' AS INT) < 12
			)) AS count_in_before_12
	, count(DISTINCT eoc.id) FILTER(WHERE (
			ct.resource #>> '{period,start}' BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-start"))).value.Quantity.value') #>> '{}' AS INT) < 14
			)) AS count_in_before_14
	, count(DISTINCT eoc.id) FILTER(WHERE (
			ct.resource #>> '{period,start}' BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "observed-previously"))).value.boolean') #>> '{}' AS TEXT) = 'true'
			)) AS count_in_from
	, count(DISTINCT eoc.id) FILTER(WHERE (
			CAST(eoc.resource #>> '{period,end}' AS TIMESTAMP) BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND (
				jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code') #>> '{}' IN (
					'1'
					, '2'
					)
				)
			)) AS count_out
	, count(DISTINCT eoc.id) FILTER(WHERE (
			CAST(eoc.resource #>> '{period,end}' AS TIMESTAMP) BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code') #>> '{}' = '1'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-type"))).value.CodeableConcept.coding[0].code') #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS INT) BETWEEN 22
				AND 24
			)) AS count_out_22_24
	, count(DISTINCT eoc.id) FILTER(WHERE (
			CAST(eoc.resource #>> '{period,end}' AS TIMESTAMP) BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code') #>> '{}' = '1'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-type"))).value.CodeableConcept.coding[0].code') #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS INT) BETWEEN 25
				AND 27
			)) AS count_out_25_27
	, count(DISTINCT eoc.id) FILTER(WHERE (
			CAST(eoc.resource #>> '{period,end}' AS TIMESTAMP) BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code') #>> '{}' = '1'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-type"))).value.CodeableConcept.coding[0].code') #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS INT) BETWEEN 28
				AND 37
			)) AS count_out_28_37
	, count(DISTINCT eoc.id) FILTER(WHERE (
			CAST(eoc.resource #>> '{period,end}' AS TIMESTAMP) BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code') #>> '{}' = '2'
			AND CAST(jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "gestational-age-end"))).value.Quantity.value') #>> '{}' AS INT) < 22
			)) AS count_out_break
	, count(DISTINCT eoc.id) FILTER(WHERE (
			CAST(ct.resource #>> '{period,end}' AS TIMESTAMP) BETWEEN '2020-01-01'
				AND '2020-11-12T23:59:59'
			AND jsonb_path_query_first(current_pregnancy.resource, '$ ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "reason"))).value.CodeableConcept.coding[0].code') #>> '{}' = '3'
			)) AS count_out_to
	, count(DISTINCT eoc.id) FILTER(WHERE '2020-01-01' BETWEEN ct.resource #>> '{period,start}'
			AND coalesce(ct.resource #>> '{period,end}', 'infinity')) AS count_end
FROM careteam ct
INNER JOIN episodeofcare eoc ON (
		eoc.resource -> 'team' @@ logic_revinclude(ct.resource, ct.id, '#')
		AND eoc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PregnantCard")'::jsquery
		)
INNER JOIN organization org ON (
		org.resource @@ logic_include(ct.resource, 'managingOrganization[0]')
		AND org.resource -> 'partOf' IS NULL
		)
LEFT JOIN LATERAL(SELECT jsonb_agg(resource) AS resource FROM observation WHERE (
			observation.resource -> 'episodeOfCare' @@ logic_revinclude(eoc.resource, eoc.id)
			AND resource -> 'category' @@ '#.coding.#(system="urn:CodeSystem:pregnancy" and code="current-pregnancy")'::jsquery
			AND immutable_tsrange(ct.resource #>> '{period,start}', ct.resource #>> '{period,end}') @> CAST(resource #>> '{effective,dateTime}' AS TIMESTAMP)
			) GROUP BY resource -> 'episodeOfCare') current_pregnancy ON true
WHERE (immutable_tsrange(ct.resource #>> '{period,start}', ct.resource #>> '{period,end}') && immutable_tsrange('2020-01-01', '2020-11-12T23:59:59'))
GROUP BY org.id

