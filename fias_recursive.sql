WITH RECURSIVE addr AS
(
  (SELECT name AS addr_name,
         address_type AS addr_type,
         aoguid AS addr_guid,
         level AS addr_level,
         parentguid AS parent_guid
  FROM fias cur
  WHERE (cur.aoguid = '80CEA267-4246-4DD2-9A84-EFF5D0E39867' AND cur.is_actual))
  UNION
  (SELECT name AS addr_name,
         address_type AS addr_type,
         aoguid AS addr_guid,
         level AS addr_level,
         parentguid AS parent_guid
  FROM addr
    INNER JOIN fias
            ON (fias.aoguid = parent_guid
           AND fias.is_actual))
)
SELECT *
FROM addr


SELECT *
FROM fias f
WHERE "level" = 3
LIMIT 100