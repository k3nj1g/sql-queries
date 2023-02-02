SELECT count(*) filter (where (resource ->> 'from') = 'web') "epgu"
  , count(*) filter (where not (resource ->> 'from') = 'web') "other"
FROM appointment
WHERE jsonb_path_exists(resource, '$.serviceType.coding ? (@.code=="153")')
  and immutable_tsrange(
        (resource#>>'{start}'),
        (resource#>>'{end}')
    ) && immutable_tsrange('2022-01-01', '2023-01-01');

SELECT *
FROM pg_indexes 
WHERE tablename='appointment';

