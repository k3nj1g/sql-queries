--EXPLAIN ANALYZE 
SELECT 
       to_jsonb("p".*) AS "patient",
       to_jsonb("l".*) AS "location",
       to_jsonb("eoc".*) AS "episodeofcare",
       to_jsonb("cp".*) AS "careplan",
       ((SELECT jsonb_agg(to_jsonb("appointment".*)) AS "appointments"
         FROM "appointment"
         WHERE ("appointment"."resource" @@ cast(CONCAT('serviceType.#.coding.#(system="urn:CodeSystem:service" and code in ("185","186","187","188","189")) and ','participant.#(actor.id=',"p"."id",')') AS "jsquery"))
           AND   cast(("resource" ->> 'start') AS "timestamp") BETWEEN '2022-03-01' AND '2022-03-31'
           AND   (("resource" ->> 'status') <> 'cancelled'))) 
FROM "practitionerrole" AS "prr" 
INNER JOIN "organization" AS "org" ON ("org"."resource" @@ LOGIC_INCLUDE("prr"."resource",'organization')) 
INNER JOIN "location" AS "l" ON (IDENTIFIER_VALUE("l"."resource",'urn:identity:oid:Location') = (jsonb_path_query_first("org"."resource",'$.identifier ? (@.system=="urn:identity:oid:Organization").value') #>> '{}')) 
    AND (("l"."resource" -> 'type') @@ '#.coding.#(system="urn:CodeSystem:feldsher-office-type")'::jsquery) 
INNER JOIN "personbinding" AS "pb" ON ("pb"."resource" @@ LOGIC_REVINCLUDE("l"."resource","l"."id",'location')) 
INNER JOIN "patient" AS "p" ON (IDENTIFIER_VALUE("p"."resource",'urn:source:tfoms:Patient') = REF_IDENTIFIER_VALUE("pb"."resource",'urn:source:tfoms:Patient','subject')) 
    AND (coalesce(("p"."resource" ->> 'active'),'true') = 'true') AND (("p"."resource" #>> '{deceased,dateTime}') IS NULL) 
INNER JOIN "episodeofcare" AS "eoc" ON ("eoc"."resource" #> '{patient}' @@ LOGIC_REVINCLUDE("p"."resource","p"."id")
    AND eoc.resource -> 'type' @@ '#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="CardDN") and not period.end=*'::jsquery)
INNER JOIN "careplan" AS "cp" ON ("cp"."resource" @@ LOGIC_REVINCLUDE("eoc"."resource","eoc"."id",'supportingInfo.#')) 
    AND EXISTS (SELECT 1    
                FROM (SELECT (jsonb_array_elements_text(("activities"."value" #> '{detail,scheduled,Timing,event}'))) AS "date"
                      FROM jsonb_array_elements(("cp"."resource" #> '{activity}')) AS "activities"
                      WHERE jsonb_array_length(("activities"."value" #> '{detail,scheduled,Timing,event}')) > 0) AS "events"
                WHERE "events"."date" BETWEEN '2022-03-01' AND '2022-03-31') 
WHERE ("prr"."id" = 'e27c16f7-f9cd-4a5f-92b8-68c11576ec31') AND ("p" IS NOT NULL)
LIMIT 50
