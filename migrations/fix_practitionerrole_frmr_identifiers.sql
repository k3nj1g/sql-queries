WITH to_update AS
(
  SELECT id
    , jsonb_set_lax(resource, '{identifier}'
      , (SELECT jsonb_agg(
           CASE WHEN idf->>'system'='urn:identity:frmr:PractitionerRole'
                THEN jsonb_set_lax(idf, '{value}'
                       , (SELECT to_jsonb(STRING_AGG(CASE WHEN row_num = 2 THEN resource #>> '{organization,identifier,value}' ELSE value END,'_'))
                          FROM (SELECT ROW_NUMBER() OVER () row_num, value
                                FROM UNNEST(REGEXP_SPLIT_TO_ARRAY((JSONB_PATH_QUERY_FIRST(resource,'$.identifier ? (@.system=="urn:identity:frmr:PractitionerRole").value') #>> '{}'),'_')) arr (value)) arr_rows))
                ELSE idf
           END)
         FROM jsonb_array_elements(resource->'identifier') idfs(idf))) AS resource
  FROM practitionerrole
  WHERE JSONB_PATH_QUERY_FIRST(resource,'$.identifier ? (@.system=="urn:identity:frmr:PractitionerRole").value') #>> '{}' ~ '[0-9]{3}-[0-9]{3}-[0-9]{3} [0-9]{2}\_{2}'
  AND   resource @@ 'organization.identifier.system="urn:identity:oid:Organization"'::jsquery
)
UPDATE public.practitionerrole prr
SET resource = tu.resource
FROM to_update tu
WHERE prr.id = tu.id;