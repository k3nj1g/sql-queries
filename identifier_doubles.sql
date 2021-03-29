SELECT
	DISTINCT identifiers.p_id, count(*)
FROM
	(
	SELECT
		patient.id AS p_id,
		jsonb_array_elements(resource -> 'identifier') AS identifier
	FROM
		patient)
	identifiers
GROUP BY
	identifiers.p_id,
	identifiers.identifier ->> 'system',
	identifiers.identifier ->> 'value',
	identifiers.identifier #>> '{period,start}'
HAVING
	count(*) > 1
