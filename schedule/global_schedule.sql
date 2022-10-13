SELECT id,
       jsonb_set(
         jsonb_set(resource
                   ,'{replacement}'::text[]
                   ,coalesce((SELECT jsonb_agg(replacement) AS replacement 
                              FROM jsonb_array_elements(resource -> 'replacement') replacement 
                              WHERE cast(replacement ->> 'date' AS timestamp) >= current_timestamp::timestamp)
                             ,'[]'))
         ,'{notAvailable}'::text[]
         ,coalesce((SELECT jsonb_agg(not_available) AS not_available 
                    FROM jsonb_array_elements(resource -> 'notAvailable') not_available 
                    WHERE coalesce(cast(not_available #>> '{during,end}' AS timestamp),'infinity') >= current_timestamp::timestamp)
                   ,'[]')) AS resource
FROM scheduleruleglobal
LIMIT 1