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
FROM diagnosticreport AS report
  INNER JOIN servicerequest AS request
          ON ( (request.resource -> 'identifier') @@ LOGIC_INCLUDE_IDF (report.resource,'basedOn'))
          OR (request.id = ANY (ARRAY ( (SELECT (jsonb_path_query(report.resource,'$.basedOn.id') #>> '{}')))))
  INNER JOIN organization AS performer_main
          ON ( (performer_main.resource @@ LOGIC_INCLUDE (request.resource,'performer'))
          OR (performer_main.id = ANY (ARRAY ( (SELECT (jsonb_path_query(request.resource,'$.performer.id') #>> '{}'))))))
         AND (performer_main.resource @@ 'identifier.#(system="urn:identity:oid:Organization" and value="1.2.643.5.1.13.13.12.2.21.1548")'::jsquery)
  INNER JOIN organizationinfo AS performer_main_info ON performer_main_info.id = performer_main.id
  INNER JOIN organization AS requester_main
          ON (requester_main.resource @@ LOGIC_INCLUDE (request.resource,'managingOrganization'))
          OR (requester_main.id = ANY (ARRAY ( (SELECT (jsonb_path_query(request.resource,'$.managingOrganization.id') #>> '{}')))))
  INNER JOIN organizationinfo AS requester_main_info ON requester_main_info.id = requester_main.id
  INNER JOIN patient AS request_subject
          ON ( (request_subject.resource @@ LOGIC_INCLUDE (request.resource,'subject'))
          OR (request_subject.id = ANY (ARRAY ( (SELECT (jsonb_path_query(request.resource,'$.subject.id') #>> '{}'))))))
         AND (coalesce ( (request_subject.resource ->> 'active'),'true') = 'true')
         AND CASE WHEN (request.resource #>> '{subject,identifier,system}') = 'urn:identity:enp:Patient' THEN ENP_VALID ( (request.resource #>> '{subject,identifier,value}')) WHEN (request.resource #>> '{subject,identifier,system}') = 'urn:identity:snils:Patient' THEN SNILS_VALID ( (request.resource #>> '{subject,identifier,value}')) WHEN NOT ( (request.resource #>> '{subject,identifier,system}') IN ('urn:identity:enp:Patient', 'urn:identity:snils:Patient')) THEN NOT ( (request.resource #>> '{subject,identifier,value}') ILIKE '%0000%') ELSE TRUE END
  LEFT JOIN practitionerrole AS performer_practitionerrole
         ON (performer_practitionerrole.resource @@ LOGIC_INCLUDE (report.resource,'performer'))
         OR (performer_practitionerrole.id = ANY (ARRAY ( (SELECT (jsonb_path_query(report.resource,'$.performer.id') #>> '{}')))))
  LEFT JOIN practitioner AS performer_practitioner
         ON (performer_practitioner.resource @@ LOGIC_INCLUDE (performer_practitionerrole.resource,'practitioner'))
         OR (performer_practitioner.id = ANY (ARRAY ( (SELECT (jsonb_path_query(performer_practitionerrole.resource,'$.practitioner.id') #>> '{}')))))
WHERE ((report.resource -> 'category') @@ '#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="Referral-LMI")'::jsquery)
AND report.resource -> 'code' @@ 'coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")'::jsquery
  AND report.resource ->> 'status' = 'final'
  AND immutable_ts(report.resource #>> '{effective,dateTime}') BETWEEN cast( '2022-04-14T11:00:00' AS timestamp) AND cast( '2022-04-14T23:00:00' AS timestamp)