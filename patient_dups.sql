SELECT concat(resource#>>'{name,0,family}', ' ', resource#>>'{name,0,given,0}', ' ', resource#>>'{name,0,given,1}') fio
       , resource ->> 'birthDate'
       , jsonb_path_query_first(resource,'$.identifier[*] ? (@.system=="urn:identity:enp:Patient")') #>> '{value}'
       , count(*)
FROM patient 
WHERE resource ->> 'birthDate' IS NOT NULL
      AND jsonb_path_query_first(resource,'$.identifier[*] ? (@.system=="urn:identity:enp:Patient")') #>> '{value}' IS NOT NULL
      AND COALESCE((resource ->> 'active'), 'true') = 'true'
GROUP BY 1,2,3
HAVING count(*) > 1