# Airflow with Java and the c360 package, so BashOperator tasks can run Spark steps
FROM apache/airflow:3.1.8-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless util-linux \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY --chown=airflow:root pyproject.toml README.md /opt/c360/
COPY --chown=airflow:root src /opt/c360/src
RUN pip install --no-cache-dir -e /opt/c360
