SELECT appointment.*
FROM sector sector
INNER JOIN practitioner practitioner ON
	practitioner.resource @@ logic_include(sector.resource, 'doctor')
INNER JOIN practitionerrole practitionerrole ON
	practitionerrole.resource @@ logic_revinclude(practitioner.resource, practitioner.id, 'practitioner')
INNER JOIN appointment appointment ON
	appointment.resource @@ CAST(concat('participant.#.actor.id="', practitionerrole.id, '"') AS jsquery)
WHERE sector.id = 'c118b0dd-f308-4e1a-8828-9b5d8ed9efc3'
	NOT "appointment".resource @> '{"serviceType":[{"coding":[{"system":"urn:CodeSystem:service", "code":"153"}]}]}'
	AND NOT "appointment".resource @> '{"status":"cancelled"}'
	AND "appointment".resource @> '{"mainOrganization":{"id":"1150e915-f639-4234-a795-1767e0a0be5f"}}'
	AND "appointment".resource @> '{"participant":[{"actor":{"resourceType":"PractitionerRole", "id":"2600bbdf-bc83-4ec8-be3f-46e5494bca34"}}]}'
	AND immutable_tsrange(appointment.resource #>> '{start}', appointment.resource #>> '{end}') && immutable_tsrange('2020-07-21', '2020-07-28')
    AND appointment.resource #>> '{start}' >= '2020-06-25'