ALTER TABLE integrationqueue 
    RENAME TO integrationqueue_default;
CREATE TABLE integrationqueue (
    LIKE integrationqueue_default INCLUDING ALL)
PARTITION BY RANGE(id);
ALTER TABLE integrationqueue ATTACH PARTITION integrationqueue_default
    DEFAULT;

-- по годам
CREATE TABLE integrationqueue_y23 PARTITION OF integrationqueue
    FOR VALUES FROM ('x23') TO ('x24');

-- для недель
DO
$do$
BEGIN
  FOR i IN 1..52 LOOP
     EXECUTE
       'CREATE TABLE public.integrationqueue_y24w' || to_char(i, 'FM00') || ' PARTITION OF public.integrationqueue
          FOR VALUES FROM (''x24' || to_char(i, 'FM00') || ''') TO (''x24' || to_char(i+1, 'FM00') || ''')
     ';
   END LOOP;
END;
$do$;

SELECT to_char('2023-01-01'::date, 'YYMMDD');
