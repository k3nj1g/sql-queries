SELECT resource -> 'performer'
       , (SELECT jsonb_agg(performer) 
          FROM (SELECT jsonb_set(value, '{identifier,system}', '"urn:identity:snils:Practitioner"') AS performer
                FROM jsonb_array_elements(resource -> 'performer') 
                WHERE value @@ 'type = "Practitioner" and identifier.system="urn:identity:snils:Doctor"'::jsquery
                UNION
                SELECT value AS performer
                FROM jsonb_array_elements(resource -> 'performer') 
                WHERE NOT value @@ 'type = "Practitioner" and identifier.system="urn:identity:snils:Doctor"'::jsquery) performers)
FROM diagnosticreport
WHERE resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-type" and code="Referral-IMI")
                      and performer.#(type = "Practitioner" and identifier.system="urn:identity:snils:Doctor")'::jsquery
                      
UPDATE diagnosticreport
SET resource = jsonb_set(resource, '{performer}', 
                        (SELECT jsonb_agg(performer) 
                         FROM (SELECT jsonb_set(value, '{identifier,system}', '"urn:identity:snils:Practitioner"') AS performer
                               FROM jsonb_array_elements(resource -> 'performer') 
                               WHERE value @@ 'type = "Practitioner" and identifier.system="urn:identity:snils:Doctor"'::jsquery
                               UNION
                               SELECT value AS performer
                               FROM jsonb_array_elements(resource -> 'performer') 
                               WHERE NOT value @@ 'type = "Practitioner" and identifier.system="urn:identity:snils:Doctor"'::jsquery) performers))
WHERE resource @@ 'category.#.coding.#(system="urn:CodeSystem:servicerequest-type" and code="Referral-IMI")
                   and performer.#(type = "Practitioner" and identifier.system="urn:identity:snils:Doctor")'::jsquery
RETURNING id
