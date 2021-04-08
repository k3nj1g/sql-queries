SELECT s.resource, sec.resource, o.resource org, mo.resource main_org, oi.resource org_info, l.resource "location", h.resource, prr.resource prr, pr.id pr_id, pr.resource pr
FROM schedulerule s 
JOIN sector sec ON sec.id = s.resource #>> '{sector,id}'
JOIN organization o ON o.id = s.resource #>> '{organization,id}'
JOIN organization mo ON mo.id = s.resource #>> '{mainOrganization,id}'
JOIN organizationinfo oi ON oi.id = mo.id
JOIN "location" l ON l.id = s.resource #>> '{location,id}'
JOIN healthcareservice h ON h.id = s.resource #>> '{healthcareService,0,id}'
JOIN practitionerrole prr ON prr.id = s.resource #>> '{actor,0,id}'
JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
WHERE s.id = 'aaef213a-5219-4418-8a62-288d8f8bbcab'