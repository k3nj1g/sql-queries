SELECT id, jsonb_set_lax(
         resource
         , '{locationCode}'
         , (SELECT jsonb_agg(
             jsonb_set_lax(
                codeable_concept
                , '{coding}'
                , (SELECT jsonb_agg(
                     CASE 
                       WHEN coding->>'system'='urn:CodeSystem:mis.medical-help-type' 
                       THEN jsonb_set_lax(jsonb_set_lax(coding, '{display}', coding->'code'), '{code}', coding->'display')
                       ELSE coding
                     END
                     )
                   FROM jsonb_array_elements(codeable_concept->'coding') codings(coding))))
            FROM jsonb_array_elements(resource->'locationCode') location_code(codeable_concept)))
FROM public.servicerequest sr
WHERE sr.resource @> '{ "locationCode": [{"coding":[{"system":"urn:CodeSystem:mis.medical-help-type", "code": "Амбулаторн"}]}]}'
LIMIT 1000;

SELECT *
FROM public.servicerequest
WHERE resource @> '{"locationCode": [{"coding":[{"system":"urn:CodeSystem:mis.medical-help-type", "code": "Амбулаторн"}]}]}';

WITH to_update AS (
  SELECT id, jsonb_set_lax(
           resource
           , '{locationCode}'
           , (SELECT jsonb_agg(
               jsonb_set_lax(
                  codeable_concept
                  , '{coding}'
                  , (SELECT jsonb_agg(
                       CASE 
                         WHEN coding->>'system'='urn:CodeSystem:mis.medical-help-type' 
                         THEN jsonb_set_lax(jsonb_set_lax(coding, '{display}', coding->'code'), '{code}', coding->'display')
                         ELSE coding
                       END
                       )
                     FROM jsonb_array_elements(codeable_concept->'coding') codings(coding))))
              FROM jsonb_array_elements(resource->'locationCode') location_code(codeable_concept))) resource
  FROM public.servicerequest sr
  WHERE sr.resource @> '{ "locationCode": [{"coding":[{"system":"urn:CodeSystem:mis.medical-help-type", "code": "Амбулаторн"}]}]}'
  LIMIT 1000)
UPDATE public.servicerequest sr
SET resource = tu.resource
FROM to_update tu
WHERE tu.id = sr.id;