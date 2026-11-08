"""Bronze layer: land raw source data in Delta Lake, unchanged, with lineage columns.

* Core banking (PostgreSQL, via JDBC): reference tables as daily snapshots, the
  transactions table incrementally by timestamp watermark.
* CRM (MongoDB): documents changed since the last watermark, stored as raw JSON
  strings (schema-on-read, so a new CRM field never breaks ingestion).
* App events (Kafka): Spark Structured Streaming with a checkpoint, run as an
  'available now' batch, so every event lands exactly once even across restarts.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Settings
from ..log import get_logger
from ..state import get_watermark, set_watermark

log = get_logger("bronze")

SNAPSHOT_TABLES = ["customers", "accounts", "loans"]


def jdbc_options(settings: Settings) -> dict[str, str]:
    u = urlparse(settings.pg_dsn)
    return {
        "url": f"jdbc:postgresql://{u.hostname}:{u.port or 5432}{u.path}",
        "user": u.username or "",
        "password": u.password or "",
        "driver": "org.postgresql.Driver",
    }


def _with_lineage(df: DataFrame, source: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source", F.lit(source))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_core(spark: SparkSession, settings: Settings, batch_id: str) -> dict[str, int]:
    opts = jdbc_options(settings)
    counts: dict[str, int] = {}
    for table in SNAPSHOT_TABLES:
        df = spark.read.format("jdbc").options(**opts, dbtable=f"core.{table}").load()
        df = _with_lineage(df, f"postgres.core.{table}", batch_id)
        df.write.format("delta").mode("append").save(os.path.join(settings.bronze, f"core_{table}"))
        counts[table] = df.count()

    wm = get_watermark(settings, "core.transactions")
    query = f"(SELECT * FROM core.transactions WHERE txn_ts > TIMESTAMP '{wm}') t"
    tx = spark.read.format("jdbc").options(
        **opts,
        dbtable=query,
        fetchsize="10000",
    ).load()
    tx = _with_lineage(tx, "postgres.core.transactions", batch_id).cache()
    n = tx.count()
    if n:
        tx.write.format("delta").mode("append").save(os.path.join(settings.bronze, "core_transactions"))
        new_wm = tx.agg(F.max("txn_ts")).first()[0]
        set_watermark(settings, "core.transactions", str(new_wm))
    tx.unpersist()
    counts["transactions"] = n
    for k, v in counts.items():
        log.info("core.%-14s -> bronze  %8d rows", k, v)
    return counts


def ingest_crm(spark: SparkSession, settings: Settings, batch_id: str) -> int:
    from pymongo import MongoClient

    wm = get_watermark(settings, "crm.interactions")
    since = datetime.fromisoformat(wm)
    client = MongoClient(settings.mongo_uri)
    coll = client[settings.mongo_db]["interactions"]
    rows = [
        (str(d["_id"]), json.dumps(d, default=str), d["updated_at"])
        for d in coll.find({"updated_at": {"$gt": since}}).sort("updated_at", 1)
    ]
    client.close()
    if not rows:
        log.info("crm.interactions   -> bronze  %8d documents", 0)
        return 0
    df = spark.createDataFrame(rows, "doc_id string, raw string, updated_at timestamp")
    df = _with_lineage(df, "mongodb.crm.interactions", batch_id)
    df.write.format("delta").mode("append").save(os.path.join(settings.bronze, "crm_interactions"))
    set_watermark(settings, "crm.interactions", str(max(r[2] for r in rows)))
    log.info("crm.interactions   -> bronze  %8d documents", len(rows))
    return len(rows)


def ingest_app_events(spark: SparkSession, settings: Settings) -> int:
    target = os.path.join(settings.bronze, "app_events")
    before = spark.read.format("delta").load(target).count() if os.path.exists(target) else 0
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("_source", F.lit(f"kafka.{settings.kafka_topic}"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", os.path.join(settings.state, "checkpoints", "app_events"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start(target)
    )
    query.awaitTermination()
    after = spark.read.format("delta").load(target).count() if os.path.exists(target) else 0
    log.info("kafka.%-12s -> bronze  %8d events", settings.kafka_topic, after - before)
    return after - before


def run_bronze(spark: SparkSession, settings: Settings) -> str:
    batch_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    log.info("Bronze batch %s", batch_id)
    ingest_core(spark, settings, batch_id)
    ingest_crm(spark, settings, batch_id)
    ingest_app_events(spark, settings)
    return batch_id
