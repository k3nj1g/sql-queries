SELECT resource #>> '{managingOrganization,display}'
    , count(*) FILTER (WHERE resource #>> '{performerInfo,requestStatus}' = 'completed') AS "completed"
    , count(*) AS "all"
FROM servicerequest s 
WHERE resource @@ 'code.coding.#(system="urn:CodeSystem:Nomenclature-medical-services" and code="A26.08.008.001")'::jsquery
    AND cts BETWEEN '2021-05-24' AND '2021-05-31'
GROUP BY 1