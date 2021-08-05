WITH update_info AS
(
  SELECT prr.id,
         (prr.resource #>> '{practitioner,identifier,value}') AS snils,
         (jsonb_path_query_first(org.resource,'$.identifier.# ? (@.system=="urn:identity:oid:Organization").value') #>> '{}') AS org_oid,
         (prr.resource #>> '{period,start}') AS period_start,
         (jsonb_path_query_first(prr.resource,'$.code.coding.# ? (@.system=="urn:CodeSystem:frmr.position").code') #>> '{}') AS frmr_code
  FROM practitionerrole AS prr
    INNER JOIN organization AS org
            ON (org.resource @@ LOGIC_INCLUDE (prr.resource,'organization'))
            OR (org.id = ANY (ARRAY (SELECT (jsonb_path_query(prr.resource,'$.organization.id') #>> '{}'))))
  WHERE (prr.resource @@ 'identifier.#(system="urn:source:1c:PractitionerRole" and value=*) and roleCategory.code="doctor"'::jsquery)
  AND   (coalesce((prr.resource #>> '{active}'),'true') = 'true')
  AND   (tsrange((cast((prr.resource #>> '{period,start}') AS timestamp))) @> cast(current_date AS timestamp))
  AND   ((jsonb_path_query_first(org.resource,'$.identifier.# ? (@.system=="urn:identity:oid:Organization").value') #>> '{}') IS NOT NULL)
),
complex_idf AS
(
  SELECT id,
         CONCAT(snils,'_',org.oid,'_',period_start,'_',frmr_code)
  FROM update_info
) UPDATE practitionerrole prr
   SET resource = JSON_SET(prr.resource,'{identifier}',(prr.resource -> '{identifier}') || jsonb_build_object('value',c.idf,'system','urn:identity:mis-rmis:PractitionerRole'))
FROM complex_idf AS c
WHERE prr.id = c.id