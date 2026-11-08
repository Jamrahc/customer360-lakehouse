"""Write the synthetic data into the three source systems:
PostgreSQL (core banking), MongoDB (CRM) and Kafka (mobile-app events)."""

from __future__ import annotations

import io
import json
from importlib import resources

import pandas as pd
import psycopg

from ..config import Settings
from ..log import get_logger
from .app_events import generate_app_events
from .core_banking import generate_core, simulate_customer_changes
from .crm import generate_crm

log = get_logger("generate")


def _copy_frame(cur: psycopg.Cursor, table: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cols = ", ".join(df.columns)
    with cur.copy(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
        cp.write(buf.read())


def load_core(settings: Settings, core) -> None:
    ddl = resources.files("c360.generate").joinpath("core_schema.sql").read_text()
    with psycopg.connect(settings.pg_dsn, autocommit=False) as conn, conn.cursor() as cur:
        cur.execute(ddl)
        for table, df in [
            ("core.customers", core.customers),
            ("core.accounts", core.accounts),
            ("core.loans", core.loans),
            ("core.transactions", core.transactions),
        ]:
            _copy_frame(cur, table, df)
            log.info("PostgreSQL %-18s %8d rows", table, len(df))
        conn.commit()


def load_crm(settings: Settings, docs: list[dict]) -> None:
    from pymongo import MongoClient

    client = MongoClient(settings.mongo_uri)
    coll = client[settings.mongo_db]["interactions"]
    coll.drop()
    if docs:
        coll.insert_many(docs)
    coll.create_index("updated_at")
    log.info("MongoDB    %-18s %8d documents", f"{settings.mongo_db}.interactions", len(docs))
    client.close()


def produce_events(settings: Settings, events: list[dict]) -> None:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
    try:
        admin.create_topics([NewTopic(settings.kafka_topic, num_partitions=3, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        key_serializer=lambda k: (k or "anonymous").encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20,
        batch_size=256 * 1024,
        acks="all",
    )
    for ev in events:
        # key by device so one device's events stay ordered within a partition
        producer.send(settings.kafka_topic, key=ev["device_id"], value=ev)
    producer.flush()
    producer.close()
    log.info("Kafka      %-18s %8d events", settings.kafka_topic, len(events))


def generate_all(settings: Settings, skip_events: bool = False) -> None:
    log.info("Generating %d customers, %d days of history to %s",
             settings.n_customers, settings.history_days, settings.run_date)
    core = generate_core(settings.n_customers, settings.run_date, settings.history_days, settings.seed)
    load_core(settings, core)
    load_crm(settings, generate_crm(core, settings.run_date, settings.history_days, settings.seed))
    if not skip_events:
        produce_events(settings, generate_app_events(core, settings.run_date, settings.history_days, settings.seed))


def apply_customer_changes(settings: Settings, fraction: float = 0.02) -> int:
    """Update a few customers in place in core banking, to show SCD Type 2 history
    in the warehouse on the next run."""
    with psycopg.connect(settings.pg_dsn) as conn:
        customers = pd.read_sql("SELECT * FROM core.customers", conn)
        changed = simulate_customer_changes(
            customers, fraction, settings.seed + int(pd.Timestamp.now().timestamp()) % 1000,
            pd.Timestamp.now().floor("s"),
        )
        diff = changed[changed["updated_at"] != customers["updated_at"]]
        with conn.cursor() as cur:
            cur.executemany(
                "UPDATE core.customers SET district=%s, kyc_risk=%s, updated_at=%s WHERE customer_id=%s",
                list(diff[["district", "kyc_risk", "updated_at", "customer_id"]].itertuples(index=False, name=None)),
            )
        conn.commit()
    log.info("Changed %d customers in core banking", len(diff))
    return len(diff)
