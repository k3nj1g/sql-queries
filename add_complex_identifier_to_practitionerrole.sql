SELECT prr.resource #>> '{practitioner,identifier,value}' snils
       , jsonb_path_query_first(org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' org_oid
       , prr.resource #>> '{period,start}' period_start
       , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') frmr_code
FROM practitionerrole prr
JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
WHERE prr.resource @@ 'identifier.#(system = "urn:source:1c:PractitionerRole" and value = *) and roleCategory.code = doctor'::jsquery
      AND COALESCE(prr.resource ->> 'active', 'true') = 'true'
      AND tsrange((prr.resource #>> '{period,start}')::timestamp, (prr.resource #>> '{period,end}')::timestamp) @> current_date::timestamp
      AND jsonb_path_query_first(org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' IS NOT NULL 

---
   SELECT prr.id
          , jsonb_path_query_first(prr.resource, '$.code.text') #>> '{}'      
          , prr.resource 
     FROM practitionerrole prr
LEFT JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
    WHERE jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') IS NULL
          AND prr.resource @@ 'identifier.#(system = "urn:source:1c:PractitionerRole" and value = *) and roleCategory.code = doctor'::jsquery
          AND COALESCE(prr.resource ->> 'active', 'true') = 'true'
          AND tsrange((prr.resource #>> '{period,start}')::timestamp, (prr.resource #>> '{period,end}')::timestamp) @> current_date::timestamp
---
          
WITH no_org AS (
       SELECT org.id 
         FROM practitionerrole prr
    LEFT JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
        WHERE jsonb_path_query_first(org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' IS NULL 
              AND prr.resource @@ 'identifier.#(system = "urn:source:1c:PractitionerRole" and value = *) and roleCategory.code = doctor'::jsquery
              AND COALESCE(prr.resource ->> 'active', 'true') = 'true'
     GROUP BY 1)
SELECT mo.id main_org_id, mo.resource ->> 'name' main_org_name, org.id org_id, org.resource ->> 'name' org_name 
  FROM organization org
  JOIN organization mo
    ON mo.resource @@ logic_include(org.resource, 'mainOrganization')
  JOIN no_org 
    ON no_org.id = org.id
    
--- update
WITH update_info AS (
    SELECT prr.id 
           , prr.resource #>> '{practitioner,identifier,value}' snils
           , jsonb_path_query_first(org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' org_oid
           , prr.resource #>> '{period,start}' period_start
           , jsonb_path_query_first(prr.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code') #>> '{}' frmr_code
    FROM practitionerrole prr
    JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization')
    WHERE prr.resource @@ 'identifier.#(system = "urn:source:1c:PractitionerRole" and value = *) and roleCategory.code = doctor'::jsquery
          AND COALESCE(prr.resource ->> 'active', 'true') = 'true'
          AND tsrange((prr.resource #>> '{period,start}')::timestamp, (prr.resource #>> '{period,end}')::timestamp) @> current_date::timestamp
          AND jsonb_path_query_first(org.resource, '$.identifier ? (@.system == "urn:identity:oid:Organization").value') #>> '{}' IS NOT NULL 
)
, complex_idf AS (
    SELECT id, concat(snils, '_', org_oid, '_', period_start, '_', frmr_code) idf
    FROM update_info
)
SELECT jsonb_set(prr.resource, '{identifier}', prr.resource -> 'identifier' || jsonb_build_object('value', c.idf, 'system', 'urn:identity:mis-rmis:PractitionerRole')) 
FROM practitionerrole prr
JOIN complex_idf c ON c.id = prr.id