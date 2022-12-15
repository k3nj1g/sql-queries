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
    ORDER BY ARRAY_POSITION(ARRAY['urn:identity:insurance-gov:Patient','urn:identity:insurance-gov-temporary:Patient','urn:identity:insurance-gov-legacy:Patient'],(value ->> 'system')) ASC,
      cast((value ->> 'date') AS timestamp) DESC LIMIT 1)) AS polis,
      (jsonb_path_query_first(enc.resource,'$.participant.individual ? (@.type=="PractitionerRole" || @.resourceType=="PractitionerRole").display') #>> '{}') AS practitioner_role,
      (jsonb_path_query_first(app.resource,'$.participant ? (@.type.text=="PART" || !(@.actor.resourceType<>null)).actor.display') #>> '{}') AS address 
FROM appointment AS app
INNER JOIN patient AS p 
  ON p.id = (jsonb_path_query_first(app.resource,'$.participant ? (@.actor.resourceType=="Patient").actor.id') #>> '{}') 
INNER JOIN organization AS o 
  ON o.id = '36483c69-e82a-4c5b-9b1d-f133fb9d2503' 
LEFT JOIN LATERAL(
  SELECT e.resource
  FROM encounter AS e
  WHERE ((e.resource -> 'subject') @@ LOGIC_REVINCLUDE(p.resource,p.id))
    AND   ((e.resource -> 'serviceProvider') @@ LOGIC_REVINCLUDE(o.resource,o.id))
    AND   ((e.resource -> 'type') @@ cast('#.coding.#(code="2" and system="urn:CodeSystem:mz.place-of-medical-care")' AS jsquery))
    AND   ((e.resource #>> '{class.code}') = 'AMB')
    AND   (cast((e.resource #>> '{period,start}') AS date) = cast((app.resource ->> 'start') AS date))
  LIMIT 1) AS enc ON TRUE 
LEFT JOIN LATERAL(
  SELECT TRUE
  FROM appointment
  WHERE ((appointment.resource -> 'serviceType') @@ cast('#.coding.#(code="153" and system="urn:CodeSystem:service")' AS jsquery))
    AND   ((jsonb_path_query_first(appointment.resource,'$.participant.actor ? (@.resourceType=="Patient")') #>> '{id}') = p.id)
    AND   ((IMMUTABLE_TS((resource ->> 'start')) >(cast(now() AS timestamp) - cast('30 day' AS interval))) AND (IMMUTABLE_TS((resource ->> 'start')) < cast((app.resource ->> 'start') AS timestamp)))
    LIMIT 1) AS app_before ON TRUE 
WHERE (app.resource @@ 'serviceType.#.coding.#.code="153" and mainOrganization.id="36483c69-e82a-4c5b-9b1d-f133fb9d2503" and not status="cancelled"'::jsquery) 
  AND (IMMUTABLE_TSTZ ((app.resource #>> '{start}')) > KNIFE_DATE_BOUND ('2022-12-07','min'))
  AND (IMMUTABLE_TSTZ ((app.resource #>> '{start}')) <= KNIFE_DATE_BOUND ('2022-12-07','max'))
  
CREATE INDEX CONCURRENTLY appointment_patient_start 
  ON appointment 
    ((jsonb_path_query_first(resource, '$."participant"."actor"?(@."resourceType" == "Patient")'::jsonpath) #>> '{id}'::text[]) 
      , immutable_ts((resource ->> 'start'::text)));