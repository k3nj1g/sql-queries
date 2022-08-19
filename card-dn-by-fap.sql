SELECT TO_JSONB("p".*) AS "patient",
       TO_JSONB("l".*) AS "location",
       TO_JSONB("eoc".*) AS "episodeofcare",
       TO_JSONB("cp".*) AS "careplan",
       ((SELECT JSONB_AGG(TO_JSONB ("appointment".*)) AS "appointments"
         FROM "appointment"
         WHERE ("appointment"."resource" @@ cast(concat('serviceType.#.coding.#(system="urn:CodeSystem:service" and code in ("185","186","187","188","189")) and ','participant.#(actor.id=',"p"."id",')') AS "jsquery"))
            AND (cast(("resource" ->> 'start') AS "timestamp") > DATE_TRUNC('day',current_timestamp))
            AND (("resource" ->> 'status') <> 'cancelled'))) 
FROM "practitionerrole" AS "prr" 
JOIN "organization" AS "org" 
    ON ("org"."resource" @@ LOGIC_INCLUDE("prr"."resource",'organization')) 
JOIN "location" AS "l" 
    ON (IDENTIFIER_VALUE("l"."resource",'urn:identity:oid:Location') = (JSONB_PATH_QUERY_FIRST("org"."resource",'$.identifier ? (@.system=="urn:identity:oid:Organization").value') #>> '{}')) 
        AND (("l"."resource" -> 'type') @@ '#.coding.#(system="urn:CodeSystem:feldsher-office-type")'::jsquery) 
JOIN "personbinding" AS "pb" 
    ON ("pb"."resource" @@ LOGIC_REVINCLUDE("l"."resource", "l"."id", 'location')) 
JOIN "patient" AS "p" ON (IDENTIFIER_VALUE("p"."resource", 'urn:source:tfoms:Patient') = REF_IDENTIFIER_VALUE("pb"."resource", 'urn:source:tfoms:Patient', 'subject')) 
    AND (COALESCE(("p"."resource" ->> 'active'), 'true') = 'true') 
    AND (("p"."resource" #>> '{deceased,dateTime}') IS NULL) 
JOIN "episodeofcare" AS "eoc" 
    ON ("eoc"."resource" @@ LOGIC_REVINCLUDE("p"."resource", "p"."id", 'patient', ' and type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="CardDN") and not period.end=*')) 
JOIN "careplan" AS "cp" 
    ON ("cp"."resource" @@ LOGIC_REVINCLUDE("eoc"."resource", "eoc"."id", 'supportingInfo.#')) 
WHERE ("prr"."id" = 'db0171e9-6921-46ec-9c4c-92dd29420abe') 
    AND ("p" IS NOT NULL) 
    AND ((AIDBOX_TEXT_SEARCH(KNIFE_EXTRACT_TEXT("eoc"."resource", '[["diagnosis","condition","code","coding","code"],["diagnosis","condition","code","coding","display"]]')) ILIKE '%i22%') 
        OR (AIDBOX_TEXT_SEARCH(KNIFE_EXTRACT_TEXT("p"."resource", '[["name","family"],["name","given",0],["name","given",1],["birthDate"],["identifier","value"]]')) ILIKE '%Diagn%')) 
LIMIT 50