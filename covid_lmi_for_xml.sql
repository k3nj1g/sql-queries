SELECT --sr.id, dr.id
	subject.resource AS resource_7
	, performer_practitioner.resource AS resource_9
    , requester_main_info.resource AS resource_6
    , performer_main.resource AS resource_3
    , dr.resource AS resource_2
    , dr.id AS id_2
    , sr.resource AS resource
    , performer_main_info.resource AS resource_4
FROM organization performer_main
JOIN servicerequest sr ON sr.resource @@ logic_revinclude(performer_main."resource", performer_main."id", 'performer.#', ' and category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")')
INNER JOIN diagnosticreport dr ON dr.resource @@ logic_revinclude(sr.resource, sr.id, 'basedOn.#') 
    AND dr.resource #>> '{effective,dateTime}' BETWEEN '2021-01-31T00:00:00' AND '2021-01-31T23:59:59'
JOIN organizationinfo performer_main_info ON performer_main_info.id = performer_main.id
JOIN organization requester_main ON (requester_main.resource @@ logic_include(sr.resource, 'managingOrganization', NULL) OR requester_main.id = any(array((SELECT jsonb_path_query(sr.resource, '$.managingOrganization.id') #>> '{}'))))
JOIN organizationinfo requester_main_info ON requester_main.id = requester_main_info.id
JOIN patient subject ON (subject.resource @@ logic_include(sr.resource, 'subject', NULL) OR subject.id = any(array((SELECT jsonb_path_query(sr.resource, '$.subject.id') #>> '{}'))))
LEFT JOIN practitionerrole performer_practitionerrole ON (performer_practitionerrole.resource @@ logic_include(sr.resource, 'performer', NULL) OR performer_practitionerrole.id = any(array((SELECT jsonb_path_query(sr.resource, '$.performer.id') #>> '{}'))))
LEFT JOIN practitioner performer_practitioner ON (performer_practitioner.resource @@ logic_include(performer_practitionerrole.resource, 'practitioner', NULL) OR performer_practitioner.id = any(array((SELECT jsonb_path_query(performer_practitionerrole.resource, '$.practitioner.id') #>> '{}'))))    
WHERE performer_main.resource @@ 'identifier.#(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1548")'::jsquery

--- working ---
SELECT 
	subject.resource AS resource_7
	, performer_practitioner.resource AS resource_9
    , requester_main_info.resource AS resource_6
    , performer_main.resource AS resource_3
    , dr.resource AS resource_2
    , dr.id AS id_2
    , sr.resource AS resource
    , performer_main_info.resource AS resource_4
FROM diagnosticreport dr 
JOIN servicerequest sr ON (sr.resource @@ logic_include(dr.resource, 'basedOn')
	OR sr.id = any(array((SELECT jsonb_path_query(dr.resource, '$.basedOn.id') #>> '{}'))))
--JOIN LATERAL (
--	SELECT *
--	FROM (
--		SELECT *
--		FROM servicerequest sr
--	   	WHERE sr.resource @@ logic_include(dr.resource, 'basedOn')
--	   	UNION ALL
--	   	SELECT *
--		FROM servicerequest sr
--	   	WHERE sr.id = any(array((SELECT jsonb_path_query(dr.resource, '$.basedOn.id') #>> '{}')))) sr_refs		
----   	WHERE resource @@ 'subject.identifier.value = "2155300897000132"'::jsquery
--) sr ON true
JOIN organization performer_main ON (performer_main.resource @@ logic_include(sr.resource, 'performer[*]') OR performer_main.id = any(array((SELECT jsonb_path_query(sr.resource, '$.performer[*].id') #>> '{}'))))
	AND performer_main.resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1548")'::jsquery
JOIN organizationinfo performer_main_info ON performer_main_info.id = performer_main.id
JOIN organization requester_main ON (requester_main.resource @@ logic_include(sr.resource, 'managingOrganization', NULL) OR requester_main.id = any(array((SELECT jsonb_path_query(sr.resource, '$.managingOrganization.id') #>> '{}'))))
JOIN organizationinfo requester_main_info ON requester_main.id = requester_main_info.id
JOIN patient subject ON (subject.resource @@ logic_include(sr.resource, 'subject', NULL) OR subject.id = any(array((SELECT jsonb_path_query(sr.resource, '$.subject.id') #>> '{}'))))
LEFT JOIN practitionerrole performer_practitionerrole ON (performer_practitionerrole.resource @@ logic_include(dr.resource, 'performer', NULL) OR performer_practitionerrole.id = any(array((SELECT jsonb_path_query(dr.resource, '$.performer.id') #>> '{}'))))
LEFT JOIN practitioner performer_practitioner ON (performer_practitioner.resource @@ logic_include(performer_practitionerrole.resource, 'practitioner', NULL) OR performer_practitioner.id = any(array((SELECT jsonb_path_query(performer_practitionerrole.resource, '$.practitioner.id') #>> '{}'))))
WHERE CAST(dr.resource #>> '{effective,dateTime}' AS timestamp) BETWEEN CAST('2021-01-29T:00:00' AS timestamp) AND '2021-01-31T23:59:59'           
    AND dr.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services") and status = final'::jsquery
    
--- 1.2.643.5.1.13.13.12.2.21.1548 - ÖÑÏÈÄ 
--- 1.2.643.5.1.13.13.12.2.21.1525 - ÃÊÁ ¹1
    
--- final ---
SELECT report.resource AS report_resource,
       report.id AS report_id,
       request.resource AS request_resource,
       request.id AS request_id,
       performer_main.resource AS performer_main_resource,
       performer_main.id AS performer_main_id,
       performer_main_info.resource AS performer_main_info_resource,
       performer_main_info.id AS performer_main_info_id,
       requester_main.resource AS requester_main_resource,
       requester_main.id AS requester_main_id,
       requester_main_info.resource AS requester_main_info_resource,
       requester_main_info.id AS requester_main_info_id,
       request_subject.resource AS subject_resource,
       request_subject.id AS subject_id,
       performer_practitionerrole.resource AS performer_practitionerrole_resource,
       performer_practitionerrole.id AS performer_practitionerrole_id,
       performer_practitioner.resource AS performer_practitioner_resource,
       performer_practitioner.id AS performer_practitioner_id
FROM diagnosticreport report
  INNER JOIN servicerequest request
          ON (request.resource @@ logic_include (report.resource,'basedOn')
          OR request.id = ANY (ARRAY ( (SELECT jsonb_path_query(report.resource,'$.basedOn.id') #>> '{}'))))
  INNER JOIN organization performer_main
          ON ( (performer_main.resource @@ logic_include (request.resource,'performer')
          OR performer_main.id = ANY (ARRAY ( (SELECT jsonb_path_query(request.resource,'$.performer.id') #>> '{}')))) AND performer_main.resource @@ 'identifier.#(value="1.2.643.5.1.13.13.12.2.21.1548" and system="urn:identity:oid:Organization")'::jsquery)
  INNER JOIN organizationinfo performer_main_info ON performer_main_info.id = performer_main.id
  INNER JOIN organization requester_main
          ON (requester_main.resource @@ logic_include (request.resource,'managingOrganization')
          OR requester_main.id = ANY (ARRAY ( (SELECT jsonb_path_query(request.resource,'$.managingOrganization.id') #>> '{}'))))
  INNER JOIN organizationinfo requester_main_info ON requester_main_info.id = requester_main.id
  INNER JOIN patient request_subject
          ON (request_subject.resource @@ logic_include (request.resource,'subject')
          OR request_subject.id = ANY (ARRAY ( (SELECT jsonb_path_query(request.resource,'$.subject.id') #>> '{}'))))
  LEFT JOIN practitionerrole performer_practitionerrole
         ON (performer_practitionerrole.resource @@ logic_include (report.resource,'performer')
         OR performer_practitionerrole.id = ANY (ARRAY ( (SELECT jsonb_path_query(report.resource,'$.performer.id') #>> '{}'))))
  LEFT JOIN practitioner performer_practitioner
         ON (performer_practitioner.resource @@ logic_include (performer_practitionerrole.resource,'practitioner')
         OR performer_practitioner.id = ANY (ARRAY ( (SELECT jsonb_path_query(performer_practitionerrole.resource,'$.practitioner.id') #>> '{}'))))
WHERE (report.resource @@ 'category.#.coding.#(code="Referral-LMI" and system="urn:CodeSystem:servicerequest-category") and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services") and status="final"'::jsquery 
	AND cast(report.resource #>> '{effective,dateTime}' AS timestamp) BETWEEN cast('2021-02-16' AS timestamp) AND cast('2021-02-19' AS timestamp))