CREATE TEMP TABLE app_prr AS (
    WITH dups AS (
    SELECT 
        concat(jsonb_path_query_first(app.resource, '$.participant.actor ? (@.resourceType == "Patient").id'), CAST((app.resource ->> 'start') AS date), app.resource #>> '{mainOrganization,id}') uni
        , jsonb_path_query_first(app.resource, '$.participant.actor ? (@.resourceType == "PractitionerRole").id') prr_id
        , ROW_NUMBER() OVER(PARTITION BY jsonb_path_query_first(app.resource, '$.participant.actor ? (@.resourceType == "Patient").id')
                                        , app.resource #>> '{mainOrganization,id}'
                                        , CAST((app.resource ->> 'start') AS date)
                           ) row_num
    FROM appointment app
    WHERE resource @@ 'not status = "cancelled"'::jsquery
    )
    SELECT dups.uni, dups.prr_id
    FROM dups
    WHERE dups.row_num > 1
);

DELETE FROM app_prr
WHERE prr_id IS NULL;

SELECT uni
    , jsonb_path_query_first(p.resource, '$.code.coding ? (@.system == "urn:CodeSystem:frmr.position").code')
    , count(*)
FROM app_prr
JOIN practitionerrole p 
 ON p.id = prr_id #>> '{}'
GROUP BY 1, 2
HAVING count(*)  > 1

