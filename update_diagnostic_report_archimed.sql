WITH to_update AS (
    SELECT d.id d_id, s.resource s_resource
    FROM diagnosticreport d 
    JOIN servicerequest s ON s.resource @@ logic_include(d.resource, 'basedOn')
    WHERE d.resource @@ 'identifier.#.system = "urn:source:archimed:DiagnosticReport"'::jsquery
        AND d.resource #>> '{effective,dateTime}' > '2022-01-01'
)
UPDATE diagnosticreport d   
SET resource = jsonb_set(d.resource , '{code}', tu.s_resource -> 'code')
FROM to_update tu 
WHERE d.id = tu.id
RETURNING d.id;

--SELECT tu.s_resource, jsonb_set(d.resource , '{code}', tu.s_resource -> 'code')
--FROM to_update tu
--JOIN diagnosticreport d
--    ON d.id = tu.d_id ;
