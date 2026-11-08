"""Silver layer: cleaned, typed, de-duplicated and conformed data.

* Reference tables: the latest bronze snapshot, standardised (phones in E.164,
  lower-case e-mails); loans get a days-past-due bucket and an IFRS 9-style stage.
* Transactions and app events: de-duplicated and MERGEd into Delta on their keys,
  so re-running a batch or receiving a late event never double-counts.
* CRM: nested JSON parsed with an explicit schema, flattened, and resolved to a
  core banking customer by ID, phone or e-mail (identity resolution). Rebuilt in
  full each run so late identity matches are picked up.
"""

from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..config import Settings
from ..log import get_logger

log = get_logger("silver")

CRM_SCHEMA = T.StructType(
    [
        T.StructField("_id", T.StringType()),
        T.StructField(
            "customer_ref",
            T.StructType(
                [
                    T.StructField("core_id", T.StringType()),
                    T.StructField("phone", T.StringType()),
                    T.StructField("email", T.StringType()),
                ]
            ),
        ),
        T.StructField("channel", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("created_at", T.StringType()),
        T.StructField("resolved_at", T.StringType()),
        T.StructField("sentiment", T.DoubleType()),
        T.StructField("agent", T.StructType([T.StructField("id", T.StringType()),
                                             T.StructField("team", T.StringType())])),
        T.StructField("tags", T.ArrayType(T.StringType())),
    ]
)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("device_id", T.StringType()),
        T.StructField("session_id", T.StringType()),
        T.StructField("os", T.StringType()),
        T.StructField("app_version", T.StringType()),
        T.StructField("properties", T.MapType(T.StringType(), T.DoubleType())),
    ]
)


# ---------------------------------------------------------------- helpers ----
def normalize_phone(col: Column) -> Column:
    """Any Bangladeshi mobile format -> +8801XXXXXXXXX; anything else -> NULL."""
    digits = F.regexp_replace(col, r"[^0-9]", "")
    e164 = (
        F.when(digits.rlike(r"^8801[3-9][0-9]{8}$"), F.concat(F.lit("+"), digits))
        .when(digits.rlike(r"^01[3-9][0-9]{8}$"), F.concat(F.lit("+88"), digits))
        .otherwise(F.lit(None))
    )
    return e164


def dpd_bucket(col: Column) -> Column:
    return (
        F.when(col <= 0, "Current")
        .when(col < 30, "1-29")
        .when(col < 60, "30-59")
        .when(col < 90, "60-89")
        .otherwise("90+")
    )


def ifrs9_stage(col: Column) -> Column:
    """Simplified staging on days past due only (30+ = significant increase in
    credit risk, 90+ = credit-impaired)."""
    return F.when(col >= 90, "Stage 3").when(col >= 30, "Stage 2").otherwise("Stage 1")


def latest_snapshot(df: DataFrame) -> DataFrame:
    last = df.agg(F.max("_batch_id")).first()[0]
    return df.filter(F.col("_batch_id") == last)


def dedupe_latest(df: DataFrame, key: str, order_col: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def _merge_insert_new(spark: SparkSession, df: DataFrame, path: str, key: str) -> None:
    """Insert rows whose key isn't in the target yet; create the table on first run."""
    if DeltaTable.isDeltaTable(spark, path):
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(path)


# --------------------------------------------------------------- builders ----
def build_customers(spark: SparkSession, s: Settings) -> DataFrame:
    df = latest_snapshot(_read(spark, os.path.join(s.bronze, "core_customers")))
    df = dedupe_latest(df, "customer_id", "updated_at")
    return df.select(
        "customer_id",
        F.initcap(F.trim("full_name")).alias("full_name"),
        "date_of_birth",
        F.trim("segment").alias("segment"),
        F.trim("district").alias("district"),
        F.trim("kyc_risk").alias("kyc_risk"),
        "onboarding_date",
        normalize_phone(F.col("phone")).alias("phone"),
        F.lower(F.trim("email")).alias("email"),
        "updated_at",
    )


def build_accounts(spark: SparkSession, s: Settings) -> DataFrame:
    df = latest_snapshot(_read(spark, os.path.join(s.bronze, "core_accounts")))
    return df.select("account_id", "customer_id", "product", "open_date", "status",
                     "closed_date", F.col("balance").cast("decimal(18,2)").alias("balance"))


def build_loans(spark: SparkSession, s: Settings) -> DataFrame:
    df = latest_snapshot(_read(spark, os.path.join(s.bronze, "core_loans")))
    return df.select(
        "loan_id", "customer_id", "product", "sanction_date",
        F.col("sanctioned_amount").cast("decimal(18,2)").alias("sanctioned_amount"),
        F.col("outstanding").cast("decimal(18,2)").alias("outstanding"),
        "interest_rate", "tenor_months", "days_past_due", "as_of_date",
        dpd_bucket(F.col("days_past_due")).alias("dpd_bucket"),
        ifrs9_stage(F.col("days_past_due")).alias("ifrs9_stage"),
    )


def build_transactions(spark: SparkSession, s: Settings, accounts: DataFrame) -> DataFrame:
    df = _read(spark, os.path.join(s.bronze, "core_transactions"))
    df = dedupe_latest(df, "txn_id", "_ingested_at")
    return (
        df.join(accounts.select("account_id", "customer_id"), "account_id", "left")
        .select(
            "txn_id", "account_id", "customer_id", "txn_ts",
            F.to_date("txn_ts").alias("txn_date"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            "direction",
            F.when(F.col("direction") == "CR", F.col("amount")).otherwise(-F.col("amount"))
            .cast("decimal(18,2)").alias("signed_amount"),
            "channel", "merchant_category",
        )
    )


def build_crm(spark: SparkSession, s: Settings, customers: DataFrame) -> DataFrame:
    raw = _read(spark, os.path.join(s.bronze, "crm_interactions"))
    raw = dedupe_latest(raw, "doc_id", "updated_at")
    doc = raw.select("updated_at", F.from_json("raw", CRM_SCHEMA).alias("d"))
    flat = doc.select(
        F.col("d._id").alias("interaction_id"),
        F.col("d.customer_ref.core_id").alias("ref_core_id"),
        normalize_phone(F.col("d.customer_ref.phone")).alias("ref_phone"),
        F.lower(F.trim(F.col("d.customer_ref.email"))).alias("ref_email"),
        F.col("d.channel").alias("channel"),
        F.col("d.type").alias("interaction_type"),
        F.col("d.category").alias("category"),
        F.col("d.status").alias("status"),
        F.to_timestamp(F.col("d.created_at")).alias("created_at"),
        F.to_timestamp(F.col("d.resolved_at")).alias("resolved_at"),
        F.col("d.sentiment").alias("sentiment"),
        F.col("d.agent.team").alias("agent_team"),
        F.array_join(F.col("d.tags"), ",").alias("tags"),
        "updated_at",
    )

    ids = customers.select(F.col("customer_id").alias("id_match"))
    phones = customers.select(F.col("customer_id").alias("phone_match"), F.col("phone").alias("p"))
    emails = customers.select(F.col("customer_id").alias("email_match"), F.col("email").alias("e"))
    resolved = (
        flat.join(ids, flat.ref_core_id == ids.id_match, "left")
        .join(phones, flat.ref_phone == phones.p, "left")
        .join(emails, flat.ref_email == emails.e, "left")
        .withColumn("customer_id", F.coalesce("id_match", "phone_match", "email_match"))
        .withColumn(
            "resolution_method",
            F.when(F.col("id_match").isNotNull(), "core_id")
            .when(F.col("phone_match").isNotNull(), "phone")
            .when(F.col("email_match").isNotNull(), "email")
            .otherwise("unresolved"),
        )
        .withColumn(
            "resolution_hours",
            (F.unix_timestamp("resolved_at") - F.unix_timestamp("created_at")) / 3600.0,
        )
        .drop("id_match", "phone_match", "email_match", "p", "e")
    )
    return resolved


def build_app_events(spark: SparkSession, s: Settings) -> DataFrame:
    raw = _read(spark, os.path.join(s.bronze, "app_events"))
    ev = raw.select(F.from_json("raw", EVENT_SCHEMA).alias("e"), "kafka_ts").select("e.*", "kafka_ts")
    ev = ev.withColumn("event_ts", F.to_timestamp("event_ts")).dropDuplicates(["event_id"])
    return ev.select(
        "event_id", "event_type", "event_ts", F.to_date("event_ts").alias("event_date"),
        "customer_id", "device_id", "session_id", "os", "app_version",
        F.col("properties").getItem("amount").alias("amount"),
        # how late the event reached Kafka (late arrivals are kept, not dropped)
        ((F.unix_timestamp("kafka_ts") - F.unix_timestamp("event_ts")) / 60.0).alias("arrival_delay_min"),
    )


def run_silver(spark: SparkSession, s: Settings) -> None:
    out = lambda name: os.path.join(s.silver, name)  # noqa: E731

    customers = build_customers(spark, s)
    customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out("customers"))
    accounts = build_accounts(spark, s)
    accounts.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out("accounts"))
    loans = build_loans(spark, s)
    loans.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out("loans"))

    customers = _read(spark, out("customers"))
    accounts = _read(spark, out("accounts"))
    _merge_insert_new(spark, build_transactions(spark, s, accounts), out("transactions"), "txn_id")
    # CRM is rebuilt in full from bronze each run, so interactions that couldn't be
    # matched yesterday get re-resolved once the customer appears in core banking.
    build_crm(spark, s, customers).write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(out("crm_interactions"))
    _merge_insert_new(spark, build_app_events(spark, s), out("app_events"), "event_id")

    for name in ["customers", "accounts", "loans", "transactions", "crm_interactions", "app_events"]:
        log.info("silver.%-18s %8d rows", name, _read(spark, out(name)).count())
