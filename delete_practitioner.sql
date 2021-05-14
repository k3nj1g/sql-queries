--- delete practitioner withour role
WITH to_delete AS (
    SELECT pr.id
    FROM practitioner pr
    LEFT JOIN practitionerrole prr ON prr.resource @@ logic_revinclude(pr.resource, pr.id, 'practitioner')
    WHERE prr IS NULL)
DELETE FROM practitioner p 
USING to_delete
WHERE p.id = to_delete.id
RETURNING p.id;

--- delete janitor
WITH pr_janitor AS (
    SELECT pr.*
      FROM practitionerrole prr
      JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
     WHERE prr.resource #>> '{code,0,text}' ILIKE '%уборщик%')
, to_delete AS (
    SELECT to_jsonb(prj) resource
      FROM pr_janitor prj
      JOIN practitionerrole prr
        ON prr.resource @@ logic_revinclude(prj.resource, prj.id, 'practitioner')
  GROUP BY prj
    HAVING count(*) = 1
)
DELETE FROM practitioner pr
USING to_delete
WHERE pr.id = to_delete.id
RETURNING p.id;

--- delete cloakroom attendant ---
WITH pr_tech AS (
    SELECT pr.*
      FROM practitionerrole prr
      JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
     WHERE prr.resource #>> '{code,0,text}' ILIKE '%гардероб%')
, to_delete AS (
    SELECT prt.id
      FROM pr_tech prt
      JOIN practitionerrole prr
        ON prr.resource @@ logic_revinclude(prt.resource, prt.id, 'practitioner')
  GROUP BY prt.id
    HAVING count(*) = 1
)
SELECT *
FROM to_delete
JOIN practitioner pr ON pr.id = to_delete.id
--DELETE FROM practitioner pr
--USING to_delete
--WHERE pr.id = to_delete.id
--RETURNING pr.id;

--- delete kitchen worker ---
WITH pr_tech AS (
    SELECT pr.*
      FROM practitionerrole prr
      JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')
     WHERE prr.resource #>> '{code,0,text}' ILIKE '%кухонный%')
, to_delete AS (
    SELECT prt.id
      FROM pr_tech prt
      JOIN practitionerrole prr
        ON prr.resource @@ logic_revinclude(prt.resource, prt.id, 'practitioner')
  GROUP BY prt.id
    HAVING count(*) = 1
)
--SELECT *
--FROM to_delete
--JOIN practitioner pr ON pr.id = to_delete.id
DELETE FROM practitioner pr
USING to_delete
WHERE pr.id = to_delete.id
RETURNING pr.id;

DELETE FROM practitionerrole
WHERE resource #>> '{code,0,text}' ILIKE '%гардероб%' OR resource #>> '{code,0,text}' ILIKE '%кухонный%'
RETURNING id;