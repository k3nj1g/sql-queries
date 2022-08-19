EXPLAIN ANALYZE 
SELECT to_jsonb(p.*) AS patient,
       to_jsonb(l.*) AS location,
       to_jsonb(eoc.*) AS episodeofcare,
       to_jsonb(cp.*) AS careplan
       ((SELECT id
         FROM appointment
         WHERE resource @@ CAST(CONCAT('participant.#(actor.id=', p.id, ')') AS jsquery)
            AND (cast((resource ->> 'start') AS timestamp) > date_trunc('day',current_timestamp))
            AND ((resource ->> 'status') <> 'cancelled'))) 
FROM practitionerrole AS prr 
JOIN organization AS org ON (org.resource @@ LOGIC_INCLUDE(prr.resource,'organization')) 
JOIN location AS l ON (l.resource @@ cast(CONCAT('type.#.coding.#(system="urn:CodeSystem:feldsher-office-type") and ','identifier.#(system="urn:identity:oid:Location" and value=',jsonb_path_query_first(org.resource,'$.identifier ? (@.system=="urn:identity:oid:Organization").value'),')') AS jsquery)) 
JOIN personbinding AS pb ON (pb.resource @@ LOGIC_REVINCLUDE(l.resource,l.id,'location')) 
JOIN patient AS p ON (IDENTIFIER_VALUE(p.resource,'urn:source:tfoms:Patient') = REF_IDENTIFIER_VALUE(pb.resource,'urn:source:tfoms:Patient','subject')) 
    AND (coalesce((p.resource ->> 'active'),'true') = 'true') 
    AND ((p.resource #>> '{deceased,dateTime}') IS NULL) 
JOIN episodeofcare AS eoc ON (eoc.resource @@ LOGIC_REVINCLUDE(p.resource,p.id,'patient',' and type.#.coding.#(system="urn:CodeSystem:episodeofcare-type" and code="CardDN") and not period.end=*')) 
JOIN careplan AS cp ON (cp.resource @@ LOGIC_REVINCLUDE(eoc.resource,eoc.id,'supportingInfo.#')) 
WHERE (prr.id = 'db0171e9-6921-46ec-9c4c-92dd29420abe') 
    AND (p IS NOT NULL)
LIMIT 50

CREATE INDEX careplan_resource__gin_jsquery ON careplan USING gin (resource jsonb_path_value_ops);

VACUUM ANALYSE careplan;

SELECT *
FROM pg_indexes
WHERE tablename = 'careteam'

CREATE INDEX careplan_resource__gin_jsquery ON careplan USING gin (resource jsonb_path_value_ops)

VACUUM ANALYSE careplan;