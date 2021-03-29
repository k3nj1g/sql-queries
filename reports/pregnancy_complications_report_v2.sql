--EXPLAIN
WITH observations AS (
	SELECT 
		eoc.id eoc_id
		,eoc.resource eoc_resource
		,max(jsonb_path_query_first(obs.resource, '$ ? (exists (@.category [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy" && @.code == "current-pregnancy")) && exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancies"))).value.integer') #>> '{}') AS pregnancies
		, max(jsonb_path_query_first(obs.resource, '$ ? (exists (@.category [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy" && @.code == "current-pregnancy")) && exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-number"))).value.integer') #>> '{}') AS birth_number
		, max(jsonb_path_query_first(obs.resource, '$ ? (exists (@.category [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy" && @.code == "current-pregnancy")) && exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "newborn-number"))).value.integer') #>> '{}') AS newborn_number
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "1").code') #>> '{}') AS delivery_on_time
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "2").code') #>> '{}') AS premature_birth
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "3").code') #>> '{}') AS medical_abortion_12_weeks
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "4").code') #>> '{}') AS abortion_for_medical_reasons
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "5").code') #>> '{}') AS spontaneous_miscarriage_11_weeks
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "6").code') #>> '{}') AS spontaneous_miscarriage_12_21_weeks
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "7").code') #>> '{}') AS unspecified_miscarriage
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "8").code') #>> '{}') AS delayed_delivery
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "9").code') #>> '{}') AS frozen_pregnancy
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "10").code') #>> '{}') AS ectopic_pregnancy
		, count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:newborn-information" && @.code == "state-after-birth"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:live-state" && @.code == "0").code') #>> '{}') AS newborn
		, string_agg(jsonb_path_query_first(obs.resource, '$ ? (exists (@.code [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-number"))).component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-pathology"))).value.string') #>> '{}', ',') AS pathology_of_pregnancy
		, string_agg(jsonb_path_query_first(obs.resource, '$ ? (exists (@.code [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-number"))).component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-abortion-pathology"))).value.string') #>> '{}', ',') AS pathology_of_childbirth_abortion
	FROM episodeofcare eoc
	JOIN observation obs ON obs.resource @@ logic_revinclude(eoc.resource, eoc.id, 'episodeOfCare', ' and category.#.coding.#(system="urn:CodeSystem:observation-category" and code="pregnancy-information") and category.#.coding.#(system="urn:CodeSystem:pregnancy" and code in ("current-pregnancy", "previous-pregnancy"))')
	WHERE eoc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="PregnantCard") and status in ("active","waitlist")'::jsquery
		AND immutable_tsrange(eoc.resource #>> '{period,start}', COALESCE(eoc.resource #>> '{period,end}', 'infinity')) && immutable_tsrange('2020-05-01', '2020-11-01')
	GROUP BY
		eoc.id
	HAVING
		((count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "1").code') #>> '{}') > 0
			AND ((
						string_agg(jsonb_path_query_first(obs.resource, '$ ? (exists (@.code [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-number"))).component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-pathology"))).value.string') #>> '{}', ',') IS NOT NULL
						OR string_agg(jsonb_path_query_first(obs.resource, '$ ? (exists (@.code [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-number"))).component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "birth-abortion-pathology"))).value.string') #>> '{}', ',') IS NOT NULL
						OR string_agg(jsonb_path_query_first(obs.resource, '$ ? (exists (@.code [*] .coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-number"))).component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "aids-surgeries"))).value.string') #>> '{}', ',') IS NOT NULL
					)
					OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:newborn-information" && @.code == "state-after-birth"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:live-state" && @.code == "0").code') #>> '{}') > 0
				)
			)
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "2").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "3").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "4").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "5").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "6").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "7").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "8").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "9").code') #>> '{}') > 0
			OR count(jsonb_path_query_first(obs.resource, '$.component [*] ? (exists (@.code.coding [*] ? (@.system == "urn:CodeSystem:pregnancy-information" && @.code == "pregnancy-outcome"))).value.CodeableConcept.coding ? (@.system == "urn:CodeSystem:pregnancy-outcome" && @.code == "10").code') #>> '{}') > 0
		)
)
SELECT 
	obs.eoc_id AS eoc_id
	, obs.*
	, obs.eoc_resource #>> '{managingOrganization,display}' AS org
	, concat(p.resource #>> '{name,0,family}', ' ' , p.resource #>> '{name,0,given,0}', ' ' , p.resource #>> '{name,0,given,1}') AS name
	, p.resource ->> 'birthDate' AS birthdate
	, obs.eoc_resource #>> '{period,start}' AS START
	, risk.resource #>> '{prediction,0,qualitativeRisk,coding,0,ordinalValue}' AS risk
	, split_part(obs.eoc_resource #>> '{careManager,display}', ';', 1) AS doctor
FROM
	observations obs
LEFT JOIN patient p ON p.resource @@ logic_include(obs.eoc_resource, 'patient', NULL)
OR p.id = obs.eoc_resource #>> '{patient,id}'
LEFT JOIN riskassessment risk ON risk.resource @@ logic_revinclude(obs.eoc_resource, obs.eoc_id, 'episodeOfCare')


