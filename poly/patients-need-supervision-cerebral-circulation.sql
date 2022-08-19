WITH "patient_context" AS MATERIALIZED 
(
  SELECT "practitionerrole"."id" AS "practitionerrole_id",
         "practitionerrole"."resource" AS "practitionerrole_resource",
         "organization"."id" AS "organization_id",
         "organization"."resource" AS "organization_resource",
         "location"."id" AS "location_id",
         "location"."resource" AS "location_resource",
         "personbinding"."id" AS "personbinding_id",
         "personbinding"."resource" AS "personbinding_resource",
         "patient"."id" AS "patient_id",
         "patient"."resource" AS "patient_resource",
         "encounter"."id" AS "encounter_id",
         "encounter"."resource" AS "encounter_resource"
  FROM "practitionerrole"
    INNER JOIN "organization" ON ("organization"."resource" @@ LOGIC_INCLUDE ("practitionerrole"."resource",'organization'))
    INNER JOIN "location"
            ON (IDENTIFIER_VALUE ("location"."resource",'urn:identity:oid:Location') = (jsonb_path_query_first ("organization"."resource",'$.identifier ? (@.system=="urn:identity:oid:Organization").value') #>> '{}'))
           AND ( ("location"."resource" -> 'type') @@ cast ('#.coding.#(system="urn:CodeSystem:feldsher-office-type" and code="fap")' AS "jsquery"))
    INNER JOIN "personbinding" ON ("personbinding"."resource" @@ LOGIC_REVINCLUDE ("location"."resource","location"."id",'location'))
    INNER JOIN "patient"
            ON (IDENTIFIER_VALUE ("patient"."resource",'urn:source:tfoms:Patient') = REF_IDENTIFIER_VALUE ("personbinding"."resource",'urn:source:tfoms:Patient','subject'))
           AND (coalesce ( ("patient"."resource" ->> 'active'),'true') = 'true')
           AND ( ("patient"."resource" #>> '{deceased,dateTime}') IS NULL)
    INNER JOIN "encounter"
            ON ( ("encounter"."resource" -> 'subject') @@ LOGIC_REVINCLUDE ("patient"."resource","patient"."id"))
           AND ( ("encounter"."resource" -> 'class') @@ cast ('code in ("NONAC","IMP","BirthIMP")' AS "jsquery"))
           AND ("encounter"."resource" #>> '{period,end}') BETWEEN '2022-02-01' AND '2022-02-28'
  WHERE "practitionerrole"."id" = 'e27c16f7-f9cd-4a5f-92b8-68c11576ec31'
),
"output" AS
(
  SELECT "patient_context". *AS "patient_context",
         'cerebral-circulation' AS "category"
  FROM "patient_context"
  WHERE ("patient_context"."encounter_resource" @@ cast('contained.#.code.coding.#.code in ("I60.0","I60.1","I60.2","I60.3","I60.4","I60.5","I60.6","I60.7","I60.8","I60.9","I61.0","I61.1","I61.2","I61.3","I61.4","I61.5","I61.6","I61.8","I61.9","I62.0","I62.1","I62.9","I63.0","I63.1","I63.2","I63.3","I63.4","I63.5","I63.6","I63.8","I63.9","I64","I69","I69.0","I69.1","I69.2","I69.3","I69.4","I69.8","G45.0","G45.1","G45.2","G45.3","G45.4","G45.8","G45.9","G46.0","G46.1","G46.2","G46.3","G46.4","G46.5","G46.6","G46.7","G46.8")' AS "jsquery"))
)
SELECT "output"."category" AS "category",
       jsonb_build_object('resourceType','Encounter','id',"output"."encounter_id") || "output"."encounter_resource" AS "encounter",
       jsonb_build_object('resourceType','Patient','id',"output"."patient_id") || "output"."patient_resource" AS "patient"
FROM "output"
ORDER BY ("output"."encounter_resource" #>> '{period,end}') DESC LIMIT 50