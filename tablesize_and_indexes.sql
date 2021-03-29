SELECT
    tablename,
    pg_size_pretty(table_size) AS table_size,
    pg_size_pretty(indexes_size) AS indexes_size,
    pg_size_pretty(total_size) AS total_size
FROM (
    SELECT
        tablename,
        pg_table_size(TABLE_NAME) AS table_size,
        pg_indexes_size(TABLE_NAME) AS indexes_size,
        pg_total_relation_size(TABLE_NAME) AS total_size
    FROM (
        SELECT ('"' || table_schema || '"."' || TABLE_NAME || '"') AS TABLE_NAME, TABLE_NAME AS tablename
        FROM information_schema.tables
    ) AS all_tables
    ORDER BY total_size DESC
) AS pretty_sizes
WHERE tablename = 'encounter';

SELECT pg_size_pretty(pg_relation_size('appointment__resource_gin_jsquery'));

SELECT pg_catalog.pg_relation_size

SELECT *, pg_size_pretty(pg_relation_size(indexname::text))
FROM pg_indexes 
WHERE tablename = 'episodeofcare'
ORDER BY indexname;

--- статистика использования индексов
SELECT
    idstat.relname AS TABLE_NAME,
    indexrelname AS index_name,
    idstat.idx_scan AS index_scans_count,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    tabstat.idx_scan AS table_reads_index_count,
    tabstat.seq_scan AS table_reads_seq_count,
    tabstat.seq_scan + tabstat.idx_scan AS table_reads_count,
    n_tup_upd + n_tup_ins + n_tup_del AS table_writes_count,
    pg_size_pretty(pg_relation_size(idstat.relid)) AS table_size
FROM
    pg_stat_user_indexes AS idstat
JOIN
    pg_indexes
    ON
    indexrelname = indexname
    AND
    idstat.schemaname = pg_indexes.schemaname
JOIN
    pg_stat_user_tables AS tabstat
    ON
    idstat.relid = tabstat.relid
WHERE
    indexdef !~* 'unique'
    AND idstat.relname = 'encounter' 
ORDER BY
    idstat.idx_scan DESC,
    pg_relation_size(indexrelid) DESC
    
SELECT sum(pg_relation_size(relid))
FROM pg_stat_user_indexes
GROUP BY relname