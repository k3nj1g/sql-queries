EXPLAIN analyze
SELECT
	parent_org.id AS parent_org_id,
	parent_org.resource#>>'{alias,0}' AS parent_org_display,
	jsonb_agg(consult.*) AS consult,
	sum(consult.app_total) AS mo_total
FROM
	(
	SELECT
		app.resource#>>'{authorOrganization, id}' AS parent_org_id,
		app.resource#>>'{mainOrganization, id}' AS child_org_id,
		org.resource#>>'{alias, 0}' AS child_org_display,
		count(app.*) AS app_total
	FROM
		appointment app
	INNER JOIN organization org ON
		org.id = app.resource#>>'{mainOrganization, id}'
	WHERE
		((knife_extract_min_timestamptz(app.resource, '[["start"]]') >= knife_date_bound('2020-01-01', 'min')
		AND knife_extract_min_timestamptz(app.resource,	'[["start"]]') <= knife_date_bound('2020-12-23', 'max'))
		AND app.resource#>>'{mainOrganization, id}' <> app.resource#>>'{authorOrganization, id}')
	GROUP BY
		parent_org_id,
		child_org_id,
		child_org_display
	ORDER BY
		child_org_display) consult
INNER JOIN organization parent_org ON
	parent_org.id = consult.parent_org_id
WHERE
	parent_org.id = 'ced757c2-5bf6-402a-9cf0-57ad59b9573a'
GROUP BY
	parent_org.id
ORDER BY
	parent_org_display