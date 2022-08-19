WITH resources AS MATERIALIZED (
    SELECT app.resource app_resource, dr.resource
         , prr.id prr_id
    FROM patient p 
    JOIN appointment app
        ON app.resource @@ logic_revinclude(p.resource, p.id, 'participant.#.actor')
        AND tsrange((app.resource ->> 'start')::timestamp, (app.resource ->> 'end')::timestamp) && tsrange('2020-01-01'::timestamp, '2022-03-01'::timestamp)
    JOIN documentreference dr
        ON dr.resource @@ logic_revinclude(p.resource, p.id, 'subject')
    JOIN practitioner pr 
        ON pr.resource @@ logic_include(dr.resource, 'author')
    JOIN practitionerrole prr
        ON prr.resource @@ logic_revinclude(pr.resource, pr.id, 'practitioner')
    WHERE jsonb_path_query_first(p.resource, '$."identifier"[*]?(@."system" == "urn:identity:snils:Patient")."value"') = '"143-284-227 46"'::jsonb
)
, clear_resources AS (
    SELECT *
--        resources.app_resource
--        , org.resource org_resource
--        , prr_id
    FROM resources
    JOIN organization org
        ON org.id = resources.app_resource #>> '{mainOrganization,id}'
--    WHERE jsonb_path_query_first(resources.app_resource, '$.participant.actor ? (@.resourceType=="PractitionerRole").id') #>> '{}' = resources.prr_id
)
SELECT *
--org_resource #>> '{alias,0}'
--    , to_char(to_date(app_resource ->> 'start', 'YYYY-MM-DD'), 'DD.MM.YYYY') app_start
--    , jsonb_path_query_first(app_resource, '$.participant.actor ? (@.resourceType=="PractitionerRole").display') #>> '{}' practitioner
--    , app_resource ->> 'status' app_status
--    , jsonb_path_query_first(app_resource, '$.participant.actor ? (@.resourceType=="PractitionerRole").id') #>> '{}' = prr_id
FROM clear_resources
ORDER by app_resource ->> 'start' DESC 






WITH resources AS (
    SELECT app.resource app_resource
        , org.resource org_resource 
        , p.resource pat_resource
        , p.id pat_id
    FROM patient p 
    JOIN appointment app
        ON app.resource @@ logic_revinclude(p.resource, p.id, 'participant.#.actor')
        AND jsonb_path_exists(app.resource, '$.participant.actor ? (@.resourceType=="PractitionerRole")')
        AND tsrange((app.resource ->> 'start')::timestamp, (app.resource ->> 'end')::timestamp) && tsrange('2020-01-01'::timestamp, '2022-03-01'::timestamp)
    JOIN organization org
        ON org.id = app.resource #>> '{mainOrganization,id}'
    WHERE jsonb_path_query_first(p.resource, '$."identifier"[*]?(@."system" == "urn:identity:snils:Patient")."value"') #>> '{}' = '113-739-456 58'
)
SELECT org_resource #>> '{alias,0}'
    , to_char(to_date(app_resource ->> 'start', 'YYYY-MM-DD'), 'DD.MM.YYYY') app_start
    , jsonb_path_query_first(app_resource, '$.participant.actor ? (@.resourceType=="PractitionerRole").display') #>> '{}' practitioner
    , app_resource ->> 'status' app_status
    , COALESCE(
      (SELECT true
       FROM documentreference dr       
       JOIN practitioner pr 
           ON pr.resource @@ logic_include(dr.resource, 'author')
       JOIN practitionerrole prr
           ON prr.resource @@ logic_revinclude(pr.resource, pr.id, 'practitioner')
       WHERE dr.resource @@ logic_revinclude(pat_resource, pat_id, 'subject')
           AND jsonb_path_query_first(app_resource, '$.participant.actor ? (@.resourceType=="PractitionerRole").id') #>> '{}' = prr.id
           AND date((app_resource ->> 'start')::timestamp) =  date((dr.resource ->> 'date')::timestamp)
       LIMIT 1),
       FALSE)
FROM resources
ORDER by app_resource ->> 'start' DESC 



WITH resources AS MATERIALIZED (
    SELECT app.resource app_resource
        , org.resource org_resource 
    FROM patient p 
    JOIN appointment app
        ON app.resource @@ logic_revinclude(p.resource, p.id, 'participant.#.actor')
    JOIN organization org
        ON org.id = app.resource #>> '{mainOrganization,id}'
    WHERE jsonb_path_query_first(p.resource, '$."identifier"[*]?(@."system" == "urn:identity:snils:Patient")."value"') = concat('"', '143-284-227 46', '"')::jsonb
)
SELECT 
    org_resource #>> '{alias,0}'
    , to_char(to_date(app_resource ->> 'start', 'YYYY-MM-DD'), 'DD.MM.YYYY') app_start
    , jsonb_path_query_first(app_resource, '$.participant.actor ? (@.resourceType=="PractitionerRole").display') #>> '{}' practitioner
    , app_resource ->> 'status' app_status
    , app_resource ->> 'status' = 'arrived'
FROM resources
ORDER by app_resource ->> 'start' DESC 