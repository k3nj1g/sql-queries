SELECT 'REINDEX TABLE ' || quote_ident(relname) || ' /*' || pg_size_pretty(pg_total_relation_size(C.oid)) || '*/;'
FROM pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE nspname = 'public'
  AND C.relkind = 'r'
  AND nspname !~ '^pg_toast'
ORDER BY pg_total_relation_size(C.oid) ASC;