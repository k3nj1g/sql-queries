SELECT d.id, d.cts, d.resource -> 'basedOn', jsonb_set(d.resource, '{basedOn,0}', jsonb_build_object('id', d.resource #>> '{basedOn,0,identifier,value}', 'resourceType', 'ServiceRequest'))
FROM diagnosticreport d
LEFT JOIN servicerequest s ON s.resource @@ logic_include(d.resource, 'basedOn[*]')
WHERE d.resource @@ 'identifier.#.system="urn:source:lis:DiagnosticReport"'::jsquery
    AND d.resource #>> '{basedOn,0,identifier}' IS NOT NULL 
    
    
SELECT d.id, d.cts, d.resource -> 'basedOn', jsonb_path_query_first(s.resource, '$.identifier ? (@.system == "urn:identity:Serial:ServiceRequest")')
    , jsonb_set(d.resource, '{basedOn,0}', jsonb_build_object('type', 'ServiceRequest'
                                                            , 'identifier', jsonb_path_query_first(s.resource, '$.identifier ? (@.system == "urn:source:icl:ServiceRequest")')))
FROM diagnosticreport d
JOIN servicerequest s ON s.id = d.resource #>> '{basedOn,0,id}'
WHERE d.resource @@ 'identifier.#.system="urn:source:lis:DiagnosticReport"'::jsquery
    
--- set to direct ref
WITH to_update AS (
       SELECT d.id
         FROM diagnosticreport d
        WHERE d.resource @@ 'identifier.#.system="urn:source:lis:DiagnosticReport"'::jsquery
              AND d.resource #>> '{basedOn,0,identifier}' IS NOT NULL 
)
UPDATE diagnosticreport d
SET resource = jsonb_set(d.resource, '{basedOn,0}', jsonb_build_object('id', d.resource #>> '{basedOn,0,identifier,value}', 'resourceType', 'ServiceRequest'))
FROM to_update tu 
WHERE d.id = tu.id
RETURNING d.id

--- set all to logical icl ref
WITH to_update AS (
    SELECT d.id
      FROM diagnosticreport d
      JOIN servicerequest s ON s.id = d.resource #>> '{basedOn,0,id}'
     WHERE d.resource @@ 'identifier.#.system="urn:source:lis:DiagnosticReport"'::jsquery 
)
UPDATE diagnosticreport d
SET resource = jsonb_set(d.resource, '{basedOn,0}', jsonb_build_object('type', 'ServiceRequest'
                                                                     , 'identifier', jsonb_path_query_first(s.resource, '$.identifier ? (@.system == "urn:source:icl:ServiceRequest")')))
FROM to_update tu 
WHERE d.id = tu.id
RETURNING d.id

--- set effective datatime
WITH to_update AS (
    SELECT d.id, o.resource -> 'effective' effective
      FROM diagnosticreport d
      JOIN observation o ON o.resource @@ logic_include(d.resource, 'result')
     WHERE d.resource @@ 'identifier.#.system="urn:source:lis:DiagnosticReport"'::jsquery  
)
UPDATE diagnosticreport d
SET resource = jsonb_set(d.resource, '{effective}', tu.effective)
FROM to_update tu 
WHERE d.id = tu.id
RETURNING d.id
