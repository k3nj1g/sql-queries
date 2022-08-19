WITH  "patient_context" AS MATERIALIZED 
(
  SELECT  "practitionerrole". "id" AS  "practitionerrole_id",
          "practitionerrole". "resource" AS  "practitionerrole_resource",
          "organization". "id" AS  "organization_id",
          "organization". "resource" AS  "organization_resource",
          "location". "id" AS  "location_id",
          "location". "resource" AS  "location_resource",
          "personbinding". "id" AS  "personbinding_id",
          "personbinding". "resource" AS  "personbinding_resource",
          "patient". "id" AS  "patient_id",
          "patient". "resource" AS  "patient_resource",
          "encounter". "id" AS  "encounter_id",
          "encounter". "resource" AS  "encounter_resource"
  FROM  "practitionerrole"
    INNER JOIN  "organization" ON ( "organization". "resource" @@ LOGIC_INCLUDE ( "practitionerrole". "resource",'organization'))
    INNER JOIN  "location"
            ON (IDENTIFIER_VALUE ( "location". "resource",'urn:identity:oid:Location') = (jsonb_path_query_first ( "organization". "resource",'$.identifier ? (@.system=="urn:identity:oid:Organization").value') #>> '{}'))
           AND ( ( "location". "resource" -> 'type') @@ cast ('#.coding.#(system="urn:CodeSystem:feldsher-office-type" and code="fap")' AS  "jsquery"))
    INNER JOIN  "personbinding" ON ( "personbinding". "resource" @@ LOGIC_REVINCLUDE ( "location". "resource", "location". "id",'location'))
    INNER JOIN  "patient"
            ON (IDENTIFIER_VALUE ( "patient". "resource",'urn:source:tfoms:Patient') = REF_IDENTIFIER_VALUE ( "personbinding". "resource",'urn:source:tfoms:Patient','subject'))
           AND (coalesce ( ( "patient". "resource" ->> 'active'),'true') = 'true')
           AND ( ( "patient". "resource" #>> '{deceased,dateTime}') IS NULL)
    INNER JOIN  "encounter"
            ON ( ( "encounter". "resource" -> 'subject') @@ LOGIC_REVINCLUDE ( "patient". "resource", "patient". "id"))
           AND ( ( "encounter". "resource" -> 'class') @@ cast ('code in ("NONAC","IMP","BirthIMP")' AS  "jsquery"))
           AND ( "encounter". "resource" #>> '{period,end}') BETWEEN '2022-01-01' AND '2022-02-28'
  WHERE  "practitionerrole". "id" = 'e27c16f7-f9cd-4a5f-92b8-68c11576ec31'
),
 "output" AS
(
  SELECT  "patient_context". *AS  "patient_context",
         'under-one-year' AS  "category"
  FROM  "patient_context"
  WHERE (( "patient_context". "patient_resource" ->> 'birthDate') IS NOT NULL)
  AND   (age(cast(( "patient_context". "patient_resource" ->> 'birthDate') AS  "timestamp")) < cast('1 year' AS  "interval"))
)
SELECT  "output". "category" AS  "category",
       jsonb_build_object('resourceType','Encounter','id', "output". "encounter_id") ||  "output". "encounter_resource" AS  "encounter",
       jsonb_build_object('resourceType','Patient','id', "output". "patient_id") ||  "output". "patient_resource" AS  "patient"
FROM  "output"
ORDER BY ( "output". "encounter_resource" #>> '{period,end}') DESC LIMIT 50