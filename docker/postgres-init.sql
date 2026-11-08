-- Extra databases for the orchestration and BI tools (the warehouse lives in c360)
CREATE DATABASE airflow;
CREATE DATABASE superset;
-- Read-only role for dashboards: BI tools never need write access
CREATE ROLE bi_reader LOGIN PASSWORD 'bi_reader';
\connect c360
CREATE SCHEMA IF NOT EXISTS dwh;
GRANT USAGE ON SCHEMA dwh TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE c360 IN SCHEMA dwh GRANT SELECT ON TABLES TO bi_reader;
