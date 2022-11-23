-- show tables in tablespace
select relname from pg_class
where reltablespace=(select oid from pg_tablespace where spcname='tblspc1');

-- tablespace location
SELECT spcname AS "Name",
  pg_catalog.pg_get_userbyid(spcowner) AS "Owner",
  pg_catalog.pg_tablespace_location(oid) AS "Location"
FROM pg_catalog.pg_tablespace
ORDER BY 1;
