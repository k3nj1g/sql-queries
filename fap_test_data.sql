SELECT *
FROM patient p 
WHERE resource::TEXT ILIKE ''

SELECT *
FROM organization o 
JOIN sector s ON s.resource @@ logic_revinclude(o.resource, o.id, 'organization') 
WHERE o.resource @@ 'identifier.#.value = "1.2.643.5.1.13.13.12.2.21.1517"'::jsquery
    AND s.resource::TEXT ILIKE '%педиатрический%'
    
SELECT *
FROM "location" l 
WHERE id = 'b3f5daf1-724a-4801-af0b-48107d703880'

SELECT *
FROM personbinding p 
WHERE resource @@ 'location.identifier.value = "2611"'::jsquery

UPDATE personbinding
SET resource = resource || '{"location": {
    "identifier": {
      "value": "2611",
      "system": "urn:source:tfoms:Location"
    }
  }}'::jsonb
WHERE resource @@ 'sector.identifier.value = "3646"'::jsquery