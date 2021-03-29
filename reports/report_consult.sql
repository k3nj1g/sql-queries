SELECT 	main_org.id as main_org_id, main_org.resource#>>'{alias,0}' AS org, 
		jsonb_agg(author_org.*) AS consult,
		SUM(author_org.app_total) 
FROM (	SELECT 	
	  			app.resource#>>'{mainOrganization, id}' AS mo_id,
	  			app.resource#>>'{authorOrganization, id}' AS author_id, 
		  	  	org.resource#>>'{alias, 0}' AS author_display,
	  			COUNT(app.*) AS app_total
		FROM appointment app
		JOIN organization AS org 
			ON 	org.id = app.resource#>>'{authorOrganization, id}'
		WHERE app.resource#>>'{mainOrganization, id}' <> app.resource#>>'{authorOrganization, id}'
	 	GROUP BY mo_id, author_id, author_display   
	 ) AS author_org
JOIN organization AS main_org 
		ON 	main_org.id=author_org.mo_id
join organizationinfo as main_org_info
        ON 	main_org_info.id=main_org.id			
where jsonb_path_exists(main_org_info.resource, '$.identifier[*] ? (@.system == "urn:identity:reg-id:OrganizationInfo" && @.value starts with "24")')
GROUP BY main_org.id
