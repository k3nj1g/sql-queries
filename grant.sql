ALTER DEFAULT PRIVILEGES FOR ROLE reader IN SCHEMA public GRANT SELECT ON TABLES TO reader;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;
