WITH bundle AS
(
  SELECT *
  FROM outboundqueue
  WHERE (resource @@ 'status="pending" and queueType="send-lmi-payment" and payload.organization-oid="1.2.643.5.1.13.13.12.2.21.1548"'::jsquery)
  LIMIT 1000
)
SELECT DISTINCT ON (dr.*) to_jsonb(dr.*) AS report,
       to_jsonb(sr.*) AS request,
       to_jsonb(performer_org.*) AS performer_org,
       to_jsonb(performer_org_info.*) AS performer_org_info,
       to_jsonb(requester_org.*) AS requester_org,
       to_jsonb(requester_org_info.*) AS requester_org_info,
       to_jsonb(p.*) AS patient,
       to_jsonb(prr.*) AS doctor_role,
       to_jsonb(pr.*) AS doctor
FROM bundle
  INNER JOIN diagnosticreport AS dr ON dr.id = (bundle.resource #>> '{payload,diagnosticreport-id}')
  LEFT JOIN servicerequest AS sr ON sr.id = (bundle.resource #>> '{payload,servicerequest-id}')
  LEFT JOIN organization AS performer_org
         ON (performer_org.resource @@ LOGIC_INCLUDE (sr.resource,'performer'))
         OR (performer_org.id = ANY (ARRAY ( (SELECT (jsonb_path_query(sr.resource,'$.performer.id') #>> '{}')))))
  LEFT JOIN organizationinfo AS performer_org_info ON performer_org_info.id = performer_org.id
  LEFT JOIN organization AS requester_org
         ON (requester_org.resource @@ LOGIC_INCLUDE (sr.resource,'managingOrganization'))
         OR (requester_org.id = ANY (ARRAY ( (SELECT (jsonb_path_query(sr.resource,'$.managingOrganization.id') #>> '{}')))))
  LEFT JOIN organizationinfo AS requester_org_info ON requester_org_info.id = requester_org.id
  LEFT JOIN patient AS p ON p.id = (bundle.resource #>> '{payload,patient-id}')
  LEFT JOIN practitionerrole AS prr
         ON (prr.resource @@ LOGIC_INCLUDE (dr.resource,'performer'))
         OR (prr.id = ANY (ARRAY ( (SELECT (jsonb_path_query(dr.resource,'$.performer.id') #>> '{}')))))
  LEFT JOIN practitioner AS pr
         ON ( (pr.resource @@ LOGIC_INCLUDE (dr.resource,'performer'))
         OR (pr.id = ANY (ARRAY ( (SELECT (jsonb_path_query(dr.resource,'$.performer.id') #>> '{}'))))))
         OR ( (pr.resource @@ LOGIC_INCLUDE (prr.resource,'practitioner'))
         OR (pr.id = ANY (ARRAY ( (SELECT (JSONB_PATH_QUERY(prr.resource, '$.practitioner.id') #>> '{}'))))));
