EXPLAIN (ANALYZE,buffers)
SELECT 
  app_before AS app_before,
  (p.resource -> 'name') AS fio,
  (app.resource #>> '{requestedPeriod,0,end}') AS datetime_end,
  (app.resource ->> 'start') AS datetime_start,
  coalesce((app.resource ->> 'comment'),((jsonb_path_query_first(p.resource,'$.telecom ? (@.system=="phone" || @.use=="mobile").value') #>> '{}'))) AS COMMENT,
  (p.resource ->> 'birthDate') AS patient_birth_date,
  (app.resource #>> '{reasonCode,0,text}') AS reason_code,
  ((SELECT (value #>> '{author,display}')
    FROM jsonb_array_elements((app.resource -> 'statusHistory'))
    WHERE (value ->> 'status') IN ('booked','cancelled')
    ORDER BY cast((value ->> 'date') AS timestamp) DESC LIMIT 1)) AS booked_practitioner,
  (jsonb_path_query_first(app.resource,'$.serviceCategory.coding ? (@.system=="urn:CodeSystem:region-result-is-in-the-emergency-card").display') #>> '{}') AS result,
  jsonb_path_query_first(enc.resource,'$.contained.code.coding ? (@.system=="urn:CodeSystem:icd-10")') AS icd_10,
  ((SELECT (value ->> 'value') 
    FROM jsonb_array_elements((p.resource -> 'identifier')) 
    ORDER BY ARRAY_POSITION(ARRAY['urn:identity:insurance-gov:Patient','urn:identity:insurance-gov-temporary:Patient','urn:identity:insurance-gov-legacy:Patient'],(value ->> 'system')) ASC, cast((value ->> 'date') AS timestamp) DESC 
    LIMIT 1)) AS polis,
  (jsonb_path_query_first(enc.resource,'$.participant.individual ? (@.type=="PractitionerRole" || @.resourceType=="PractitionerRole").display') #>> '{}') AS practitioner_role,
  (jsonb_path_query_first(app.resource,'$.participant ? (@.type.text=="PART" || !(@.actor.resourceType<>null)).actor.display') #>> '{}') AS address 
FROM appointment AS app 
INNER JOIN patient AS p ON p.id = (jsonb_path_query_first(app.resource,'$.participant ? (@.actor.resourceType=="Patient").actor.id') #>> '{}') 
INNER JOIN organization AS o ON o.id = '1150e915-f639-4234-a795-1767e0a0be5f' 
LEFT JOIN LATERAL (
  SELECT e.resource
  FROM encounter AS e
  WHERE e.resource @@ LOGIC_REVINCLUDE(p.resource,p.id,'subject','and type.#.coding.#(code="2" and system="urn:CodeSystem:mz.place-of-medical-care") and class.code="AMB"')
    AND ((e.resource -> 'serviceProvider') @@ LOGIC_REVINCLUDE(o.resource,o.id))
    AND (cast((e.resource #>> '{period,start}') AS date) = cast((app.resource ->> 'start') AS date))
  LIMIT 1) AS enc ON TRUE 
LEFT JOIN LATERAL(
  SELECT TRUE
  FROM appointment
  WHERE (appointment.resource @@ cast(CONCAT('serviceType.#.coding.#(system="urn:CodeSystem:service" and code="153") and ','participant.#(actor.id=',p.id,')') AS jsquery))
    AND ((IMMUTABLE_TSTZ((resource ->> 'start')) >(current_timestamp- cast('30 day' AS interval))) AND (IMMUTABLE_TSTZ((resource ->> 'start')) < cast((app.resource ->> 'start') AS timestamp)))
  LIMIT 1) AS app_before ON TRUE 
WHERE (app.resource @@ 'serviceType.#.coding.#.code="153" and mainOrganization.id="1150e915-f639-4234-a795-1767e0a0be5f"'::jsquery) 
  AND (app.resource #>> '{start}') BETWEEN '2022-07-01' AND '2022-07-31T23:59:59'