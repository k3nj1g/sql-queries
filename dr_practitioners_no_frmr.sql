--- no frmr   
WITH doctors AS (
    SELECT DISTINCT prr.resource #>> '{practitioner,identifier,value}' snils, prr.resource #>> '{organization,identifier,value}' org
    FROM diagnosticreport dr
    JOIN practitionerrole prr ON prr.resource @@ format('practitioner.identifier.value="%s"', jsonb_path_query_first(dr.resource, '$.performer ? (@.type == "Practitioner").identifier.value')  #>> '{}')::jsquery
        AND NOT prr.resource @@ 'identifier.#.system = "urn:identity:frmr:PractitionerRole"'::jsquery
    WHERE dr.resource @@ 'performer.#.type = "Practitioner"'::jsquery
        AND jsonb_array_length(dr.resource -> 'performer') = 1
)
SELECT DISTINCT 
    main_org.resource #>> '{alias,0}' org_name
    , concat(pr.resource #>> '{name,0,family}', ' ', pr.resource #>> '{name,0,given,0}', ' ', pr.resource #>> '{name,0,given,1}') doctor
    , ds.snils snils
FROM doctors ds
JOIN practitioner pr ON pr.resource @@ format('identifier.#.value = "%s"', ds.snils)::jsquery
JOIN organization o ON o.resource @@ format('identifier.#.value = "%s"', ds.org)::jsquery
JOIN organization main_org ON main_org.resource @@ logic_include(o.resource, 'mainOrganization')

--- practitioners with no frmr
WITH doctors AS (
    SELECT DISTINCT prr.resource #>> '{practitioner,identifier,value}' snils, prr.resource #>> '{organization,identifier,value}' org 
    FROM practitionerrole prr 
    WHERE prr.resource @@ 'roleCategory.code = "doctor"'::jsquery
        AND NOT prr.resource @@ 'identifier.#.system = "urn:identity:frmr:PractitionerRole"'::jsquery
        AND COALESCE (resource ->> 'active', 'true') = 'true'
)
SELECT DISTINCT 
    main_org.resource #>> '{alias,0}' org_name
    , concat(pr.resource #>> '{name,0,family}', ' ', pr.resource #>> '{name,0,given,0}', ' ', pr.resource #>> '{name,0,given,1}') doctor
    , ds.snils snils
FROM doctors ds    
JOIN practitioner pr ON pr.resource @@ format('identifier.#.value = "%s"', ds.snils)::jsquery
JOIN organization o ON o.resource @@ format('identifier.#.value = "%s"', ds.org)::jsquery
JOIN organization main_org ON main_org.resource @@ logic_include(o.resource, 'mainOrganization')