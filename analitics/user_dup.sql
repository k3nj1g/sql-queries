WITH dups AS (SELECT resource ->> 'userName' username, count(*), array_agg(id) ids
FROM "user"
WHERE resource ->> 'active' = 'true'
GROUP BY 1
HAVING count(*) > 1
ORDER BY 1 DESC)
SELECT username, count, id
FROM dups
JOIN LATERAL 
  (WITH unnested AS (
   SELECT id
   FROM UNNEST(ids) v(id))
   , with_user AS (
   SELECT u.*
   FROM unnested
   JOIN "user" u ON u.id = unnested.id
   )   
   SELECT id
   FROM with_user
   WHERE resource->>'userName' = username
   ) id ON TRUE 
ORDER BY 1;