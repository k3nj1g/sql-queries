SELECT "appointment".*
FROM "appointment"
WHERE (NOT "appointment".resource @> '{"serviceType":[{"coding":[{"system":"urn:CodeSystem:service","code":"153"}]}]}'
    AND NOT "appointment".resource @> '{"status":"cancelled"}'
    AND "appointment".resource @> '{"mainOrganization":{"id":"1150e915-f639-4234-a795-1767e0a0be5f"}}'
    AND "appointment".resource @> '{"participant":[{"actor":{"resourceType":"Location","id":"679fb0cc-d60e-495b-954f-853894a62825"}}]}'
    AND (immutable_tstz(appointment.resource ->> 'start') BETWEEN knife_date_bound('2021-09-01', 'min') AND knife_date_bound('2021-10-01', 'max')));


--- working
SELECT *
FROM "appointment" app
LEFT JOIN patient p ON p.id = jsonb_path_query_first(app.resource, '$.participant[*].actor ? (@.resourceType == "Patient").id') #>> '{}'
LEFT JOIN "user" u ON u.id = app.resource #>> '{author,id}'
LEFT JOIN servicerequest s ON s.id = jsonb_path_query_first(app.resource, '$.basedOn ? (@.resourceType == "ServiceRequest").id') #>> '{}'
WHERE (NOT app.resource @> '{"serviceType":[{"coding":[{"system":"urn:CodeSystem:service","code":"153"}]}]}'
    AND NOT app.resource @> '{"status":"cancelled"}'
    AND app.resource @> '{"mainOrganization":{"id":"28c86343-0331-459f-b7fb-23df806cec1c"}, "participant":[{"actor":{"resourceType":"PractitionerRole","id":"13e0e42a-e209-4e11-b192-027fb861afd1"}}]}'
    AND (immutable_tstz(app.resource ->> 'start') BETWEEN knife_date_bound('2021-11-23', 'min') AND knife_date_bound('2021-11-23', 'max')));

--- from sector
SELECT *
FROM sector s
JOIN practitioner pr ON pr.resource @@ logic_include(s.resource,'doctor')
JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(pr.resource,pr.id,'practitioner')
JOIN appointment app ON app.resource @@ cast(concat('participant.#(actor.id=',prr.id,')') AS jsquery)
LEFT JOIN patient p ON p.id = jsonb_path_query_first(app.resource, '$.participant[*].actor ? (@.resourceType == "Patient").id') #>> '{}'
LEFT JOIN "user" u ON u.id = app.resource #>> '{author,id}'
LEFT JOIN servicerequest sr ON sr.id = jsonb_path_query_first(app.resource, '$.basedOn ? (@.resourceType == "ServiceRequest").id') #>> '{}'
WHERE s.id = '24f25316-dad9-4146-974b-7d7f27ef6912'
    AND NOT app.resource @> '{"serviceType":[{"coding":[{"system":"urn:CodeSystem:service","code":"153"}]}]}'
    AND NOT app.resource @> '{"status":"cancelled"}'
    AND app.resource @> '{"mainOrganization":{"id":"28c86343-0331-459f-b7fb-23df806cec1c"}}'
    AND (immutable_tstz(app.resource ->> 'start') BETWEEN knife_date_bound('2021-06-01', 'min') AND knife_date_bound('2021-07-01', 'max'));

SELECT *
FROM practitionerrole, sector
LIMIT 10

CREATE OR REPLACE FUNCTION immutable_tstz(date text)
RETURNS timestamp WITH time ZONE 
LANGUAGE plpgsql
IMMUTABLE
AS $function$
  BEGIN
    RETURN cast(date AS timestamptz);
  END;
$function$;

SELECT *
FROM appointment a 
WHERE resource @@ 'basedOn.#.identifier = *'::jsquery
LIMIT 1