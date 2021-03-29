SELECT
	"appointment" .*
FROM
	"appointment"
WHERE
	(NOT "appointment".resource @> '{"serviceType":[{"coding":[{"system":"urn:CodeSystem:service","code":"153"}]}]}'
	AND NOT "appointment".resource @> '{"status":"cancelled"}'
	AND "appointment".resource @> '{"mainOrganization":{"id":"96ec3268-9e6d-4d32-9275-1c5d905c4194"}}'
	AND "appointment".resource @> '{"participant":[{"actor":{"resourceType":"PractitionerRole","id":"1ed354b3-a158-4633-9f5b-8ab65ae4b0cb"}}]}'
	AND (CAST(appointment.resource ->> 'start' AS date) >= knife_date_bound('2020-12-15', 'min')
	AND CAST(appointment.resource ->> 'start' AS date) <= knife_date_bound('2020-12-15', 'max')))
	
	
