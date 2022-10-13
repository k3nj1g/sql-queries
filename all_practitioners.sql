SELECT DISTINCT main_org.resource #>> '{alias,0}' org_name
  , prr.resource #>> '{code,0,text}' frmr_position
  , patient_fio(pr.resource) doctor
  , identifier_value(pr.resource, 'urn:identity:snils:Practitioner') snils
FROM practitionerrole prr
JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
JOIN organization main_org ON main_org.resource @@ logic_include(org.resource, 'mainOrganization')
WHERE prr.resource @@ 'active=true'::jsquery
--GROUP BY org_name, frmr_position, doctor, snils
ORDER BY org_name, frmr_position, doctor;