SELECT resource #> '{code,coding}', 
      jsonb_set(
        jsonb_set(resource, '{orderDetail,0,coding}', 
          (SELECT jsonb_agg(value)
           FROM (SELECT jsonb_set(value, '{code}', '"A11.20.002"'::jsonb) AS value
                 FROM jsonb_array_elements(resource #> '{orderDetail,0,coding}')
                 WHERE (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')
                 UNION 
                 SELECT *
                 FROM jsonb_array_elements(resource #> '{orderDetail,0,coding}')
                 WHERE NOT (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')) orders))
        , '{code,coding}',
        (SELECT jsonb_agg(value)
         FROM (SELECT jsonb_set(value, '{code}', '"A11.20.002"'::jsonb) AS value
               FROM jsonb_array_elements(resource #> '{code,coding}')
               WHERE (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')
               UNION 
               SELECT *
               FROM jsonb_array_elements(resource #> '{code,coding}')
               WHERE NOT (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')) codes))
FROM servicerequest s
WHERE resource @@ 'code.coding.#.code="A08.20.017.001" and orderDetail=*'::jsquery
AND   resource ->> 'authoredOn' > '2022-01-01'

UPDATE servicerequest 
SET resource =  
      jsonb_set(
        jsonb_set(resource, '{orderDetail,0,coding}', 
          (SELECT jsonb_agg(value)
           FROM (SELECT jsonb_set(value, '{code}', '"A11.20.002"'::jsonb) AS value
                 FROM jsonb_array_elements(resource #> '{orderDetail,0,coding}')
                 WHERE (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')
                 UNION 
                 SELECT *
                 FROM jsonb_array_elements(resource #> '{orderDetail,0,coding}')
                 WHERE NOT (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')) orders))
        , '{code,coding}',
        (SELECT jsonb_agg(value)
         FROM (SELECT jsonb_set(value, '{code}', '"A11.20.002"'::jsonb) AS value
               FROM jsonb_array_elements(resource #> '{code,coding}')
               WHERE (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')
               UNION 
               SELECT *
               FROM jsonb_array_elements(resource #> '{code,coding}')
               WHERE NOT (value ->> 'system' = 'urn:CodeSystem:Nomenclature-medical-services' AND value ->> 'code' = 'A08.20.017.001')) codes))
WHERE resource @@ 'code.coding.#.code="A08.20.017.001" and orderDetail=*'::jsquery
    AND resource ->> 'authoredOn' > '2022-01-01'
RETURNING id