# Pipeline runtime: Python 3.12 + Java 17 for Spark
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl util-linux \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    C360_LAKE_PATH=/data/lake \
    C360_LOG_DIR=/data/logs

WORKDIR /opt/c360
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"
COPY . .

CMD ["sleep", "infinity"]
