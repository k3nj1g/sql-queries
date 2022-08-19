select *
from integrationqueue
where resource @@ 'payload.identifier.#.system="urn:source:lis:DiagnosticReport"'::jsquery
    and ts>'2021-10-17'
    
WITH to_update AS (
    SELECT d.id, s.resource s_resource
    FROM servicerequest s 
    JOIN diagnosticreport d ON d.resource @@ logic_revinclude(s.resource, s.id, 'basedOn.#')
    WHERE s.resource @@ 'identifier.#.system = "urn:source:icl:ServiceRequest"'::jsquery
        AND NOT d.resource @@ 'basedOn.#.identifier = *'::jsquery
)
UPDATE diagnosticreport d
SET resource = jsonb_set(d.resource
                         , '{basedOn,0}'
                        , d.resource #> '{basedOn,0}' || jsonb_build_object('type', 'ServiceRequest'
                                                                          , 'identifier', jsonb_path_query_first(tu.s_resource, '$.identifier ? (@.system == "urn:source:icl:ServiceRequest")')))
FROM to_update tu 
WHERE d.id = tu.id
RETURNING d.id

select * from diagnosticreport d
where d.resource @@ 'basedOn.#.identifier.system = "urn:identity:Serial:ServiceRequest"'::jsquery

WITH to_update AS (
    SELECT d.id,
           s.resource s_resource
    FROM diagnosticreport d 
    JOIN servicerequest s
       ON s.resource @@ logic_include (d.resource,'basedOn')
    WHERE s.resource @@ 'identifier.#.system = "urn:source:icl:ServiceRequest"'::jsquery
        AND d.resource @@ 'basedOn.#.identifier.system = "urn:identity:Serial:ServiceRequest"'::jsquery
)
SELECT jsonb_set(d.resource
                         , '{basedOn,0}'
                         , d.resource #> '{basedOn,0}' || jsonb_build_object('type', 'ServiceRequest',
                                                                             'identifier', jsonb_path_query_first(tu.s_resource, '$.identifier ? (@.system == "urn:source:icl:ServiceRequest")')))
FROM diagnosticreport d
JOIN to_update tu ON d.id = tu.id


WITH to_update AS (
    SELECT d.id,
           s.resource s_resource
    FROM diagnosticreport d 
    JOIN servicerequest s
       ON s.resource @@ logic_include (d.resource,'basedOn')
    WHERE s.resource @@ 'identifier.#.system = "urn:source:icl:ServiceRequest"'::jsquery
        AND d.resource @@ 'basedOn.#.identifier.system = "urn:identity:Serial:ServiceRequest"'::jsquery
)
UPDATE diagnosticreport d
SET resource = jsonb_set(d.resource
                         , '{basedOn,0}'
                         , d.resource #> '{basedOn,0}' || jsonb_build_object('type', 'ServiceRequest',
                                                                             'identifier', jsonb_path_query_first(tu.s_resource, '$.identifier ? (@.system == "urn:source:icl:ServiceRequest")')))
FROM to_update tu 
WHERE d.id = tu.id
RETURNING d.id