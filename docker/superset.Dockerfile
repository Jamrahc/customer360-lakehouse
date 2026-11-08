# Superset with the PostgreSQL driver for the warehouse connection
FROM apache/superset:4.1.2

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
USER superset
