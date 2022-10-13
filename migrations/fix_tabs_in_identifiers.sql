WITH idfs AS (
  SELECT id, jsonb_array_elements(resource->'identifier') ->> 'value' value
  FROM organization obroken)
, broken AS (
  SELECT *
  FROM idfs
  WHERE value LIKE '%'||chr(9)||'%')
, fix AS (
  SELECT o.id, jsonb_set(o.resource, '{identifier}', (SELECT jsonb_agg(jsonb_set(value,'{value}',to_jsonb(regexp_replace(value->>'value', chr(9), '', 'g')))) FROM jsonb_array_elements(resource->'identifier'))) resource
  FROM broken, organization o 
  WHERE o.id = broken.id)
UPDATE organization 
SET resource = fix.resource
FROM fix
WHERE organization.id = fix.id
RETURNING * 