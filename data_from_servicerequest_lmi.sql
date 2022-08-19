SELECT s.resource #>> '{subject,display}' patient
    , jsonb_path_query_first(p.resource, '$.identifier ? (@.system=="urn:identity:snils:Patient").value') #>> '{}' snils 
    , jsonb_path_query_first(p.resource, '$.identifier ? (@.system=="urn:identity:enp:Patient").value') #>> '{}' enp
    , jsonb_path_query_first(p.resource, '$.address ? (@.use=="temp" || @.use=="home").text') #>> '{}' address
    , s.resource #>> '{managingOrganization,display}' org
    , jsonb_path_query_first(s.resource, '$.identifier? (@.system=="urn:identity:Serial:ServiceRequest").value') #>> '{}' serial
    , jsonb_path_query_first(s.resource, '$.reasonCode.coding ? (@.system=="urn:CodeSystem:icd-10").code') #>> '{}' diagnosis
FROM servicerequest s 
LEFT JOIN patient p ON p.resource @@ logic_include(s.resource, 'subject')
WHERE s.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="Referral-LMI") 
                     and (performerInfo.requestStatus=completed or status=completed)
                     and code.coding.#(code="A26.08.008.001" and system="urn:CodeSystem:Nomenclature-medical-services")
                     and performer.#(type="Organization" and identifier.value="1.2.643.5.1.13.13.12.2.21.1527")'::jsquery
    AND s.resource ->> 'authoredOn' BETWEEN '2021-12-01' AND '2022-01-01'