SELECT pd.id
  FROM organization org
  JOIN plandefinition pd
    ON org.id = pd.resource #>> '{mainOrganization,id}'
       AND pd.resource @@ 'type.coding.#(system="urn:Lab-catalog-type" and code="KDL")'::jsquery
  JOIN activitydefinition ad
    ON ad.id = any(array(SELECT split_part(jsonb_path_query(pd.resource, '$.action.definition.canonical') #>> '{}', '/', 2)))
       AND ad.resource @@ 'code.coding.#(system="urn:CodeSystem:Nomenclature-medical-services" and code="A26.08.008.001")'::jsquery
 WHERE org.resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1525")'::jsquery
 
SELECT jsonb_path_query_first(ad.resource, '$.code.coding ? (@.system=="urn:CodeSystem:Nomenclature-medical-services").code') #>> '{}'
  , string_agg(jsonb_path_query_first(od.resource, '$.code.coding ? (@.system=="urn:CodeSystem:Laboratory-Research-and-Test").code') #>> '{}', ';')
FROM organization org
JOIN plandefinition pd
  ON org.id = pd.resource #>> '{mainOrganization,id}'
    AND pd.resource @@ 'type.coding.#(system="urn:Lab-catalog-type" and code="KDL")'::jsquery
JOIN activitydefinition ad
  ON ad.id = any(array(SELECT split_part(jsonb_path_query(pd.resource, '$.action.definition.canonical') #>> '{}', '/', 2)))
JOIN observationdefinition od
  ON (od.resource @@ logic_include(ad.resource,'observationResultRequirement')
    OR od.id = ANY (array ( (SELECT jsonb_path_query(ad.resource, '$.observationResultRequirement.id') #>> '{}')))) 
WHERE org.resource @@ 'identifier.#(system = "urn:identity:oid:Organization" and value = "1.2.643.5.1.13.13.12.2.21.1525")'::jsquery
GROUP BY 1