WITH to_update AS (
    SELECT DISTINCT s.id
    FROM servicerequest s 
    JOIN diagnosticreport d ON d.resource @@ logic_revinclude(s.resource, s.id, 'basedOn.#')
    WHERE s.resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-category" and code="Referral-LMI") and not performerInfo.requestStatus=completed'::jsquery
)
UPDATE servicerequest s 
SET resource = jsonb_set(s.resource, '{performerInfo,requestStatus}', '"completed"'::jsonb)
FROM to_update tu
WHERE s.id = tu.id
RETURNING s.id