"""Gold layer: business-ready tables.

* customer_360       one row per customer: profile, products, balances, credit
                     quality, transaction behaviour, service history, app engagement
* kpi_daily          daily KPIs for management reporting
* portfolio_quality  loan book by product and IFRS 9 stage

`customer_features(as_of)` is point-in-time correct: it only uses data that existed
on `as_of`, so the churn model can be trained on a past date without leaking the
future into its features.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Settings
from ..log import get_logger

log = get_logger("gold")


def _silver(spark: SparkSession, s: Settings, name: str) -> DataFrame:
    return spark.read.format("delta").load(os.path.join(s.silver, name))


def customer_features(spark: SparkSession, s: Settings, as_of: date) -> DataFrame:
    asof = F.lit(as_of).cast("date")
    d30, d60, d90 = (F.lit(as_of - timedelta(days=n)).cast("date") for n in (30, 60, 90))

    cust = _silver(spark, s, "customers").filter(F.col("onboarding_date") <= asof)
    acc = _silver(spark, s, "accounts").filter(F.col("open_date") <= asof)
    loans = _silver(spark, s, "loans")
    tx = _silver(spark, s, "transactions").filter(F.col("txn_date") <= asof)
    crm = _silver(spark, s, "crm_interactions").filter(
        F.col("customer_id").isNotNull() & (F.to_date("created_at") <= asof)
    )
    ev = _silver(spark, s, "app_events").filter(
        F.col("customer_id").isNotNull() & (F.col("event_date") <= asof)
    )

    acc_f = acc.groupBy("customer_id").agg(
        F.count("*").alias("n_accounts"),
        F.sum(
            F.when(F.col("closed_date").isNull() | (F.col("closed_date") > asof), 1).otherwise(0)
        ).alias("n_active_accounts"),
        F.countDistinct("product").alias("n_deposit_products"),
    )

    loan_f = loans.groupBy("customer_id").agg(
        F.count("*").alias("n_loans"),
        F.sum("outstanding").cast("decimal(18,2)").alias("loan_outstanding"),
        F.max("days_past_due").alias("max_dpd"),
        F.max("ifrs9_stage").alias("worst_stage"),
    )

    tx_f = tx.groupBy("customer_id").agg(
        F.sum(F.when(F.col("txn_date") > d30, 1).otherwise(0)).alias("txn_count_30d"),
        F.sum(F.when(F.col("txn_date") > d90, 1).otherwise(0)).alias("txn_count_90d"),
        F.sum(F.when((F.col("txn_date") > d30) & (F.col("direction") == "CR"), F.col("amount")).otherwise(0))
        .cast("double").alias("credit_amount_30d"),
        F.sum(F.when((F.col("txn_date") > d30) & (F.col("direction") == "DR"), F.col("amount")).otherwise(0))
        .cast("double").alias("debit_amount_30d"),
        F.countDistinct(F.when(F.col("txn_date") > d90, F.col("channel"))).alias("channels_used_90d"),
        F.avg(F.when(F.col("txn_date") > d90, (F.col("channel") == "App").cast("int"))).alias("app_txn_share_90d"),
        F.max("txn_date").alias("last_txn_date"),
    )

    crm_f = crm.groupBy("customer_id").agg(
        F.sum(F.when(F.to_date("created_at") > d90, 1).otherwise(0)).alias("interactions_90d"),
        F.sum(F.when((F.to_date("created_at") > d90) & (F.col("interaction_type") == "Complaint"), 1)
              .otherwise(0)).alias("complaints_90d"),
        F.sum(F.when((F.col("interaction_type") == "Complaint")
                     & (F.col("resolved_at").isNull() | (F.to_date("resolved_at") > asof)), 1)
              .otherwise(0)).alias("open_complaints"),
        F.avg(F.when(F.to_date("created_at") > d90, F.col("sentiment"))).alias("avg_sentiment_90d"),
    )

    ev_f = ev.groupBy("customer_id").agg(
        F.countDistinct(F.when(F.col("event_date") > d30, F.col("session_id"))).alias("app_sessions_30d"),
        F.countDistinct(F.when((F.col("event_date") > d60) & (F.col("event_date") <= d30), F.col("session_id")))
        .alias("app_sessions_prev_30d"),
        F.sum(F.when((F.col("event_date") > d30) & (F.col("event_type") == "error"), 1).otherwise(0))
        .alias("app_errors_30d"),
        F.max(F.when(F.col("event_type") == "login", F.col("event_date"))).alias("last_login_date"),
    )

    out = (
        cust.join(acc_f, "customer_id", "left")
        .join(loan_f, "customer_id", "left")
        .join(tx_f, "customer_id", "left")
        .join(crm_f, "customer_id", "left")
        .join(ev_f, "customer_id", "left")
        .fillna(0, subset=["n_accounts", "n_active_accounts", "n_deposit_products", "n_loans", "max_dpd",
                           "txn_count_30d", "txn_count_90d", "credit_amount_30d", "debit_amount_30d",
                           "channels_used_90d", "app_txn_share_90d", "interactions_90d", "complaints_90d",
                           "open_complaints", "app_sessions_30d", "app_sessions_prev_30d", "app_errors_30d"])
        .withColumn("as_of_date", asof)
        .withColumn("age", F.floor(F.datediff(asof, "date_of_birth") / 365.25).cast("int"))
        .withColumn("tenure_days", F.datediff(asof, "onboarding_date"))
        .withColumn("days_since_last_txn", F.coalesce(F.datediff(asof, "last_txn_date"), F.lit(999)))
        .withColumn("days_since_last_login", F.coalesce(F.datediff(asof, "last_login_date"), F.lit(999)))
        .withColumn("is_digital", (F.col("last_login_date").isNotNull()).cast("int"))
        # activity trend: last 30 days vs the 90-day monthly average (1.0 = steady)
        .withColumn("txn_trend", F.col("txn_count_30d") / F.greatest(F.col("txn_count_90d") / 3.0, F.lit(1.0)))
        .withColumn("session_trend",
                    F.col("app_sessions_30d") / F.greatest(F.col("app_sessions_prev_30d"), F.lit(1)))
        .withColumn("is_active", (F.col("n_active_accounts") > 0).cast("int"))
        .withColumn("loan_outstanding", F.coalesce("loan_outstanding", F.lit(0)).cast("decimal(18,2)"))
        .withColumn("worst_stage", F.coalesce("worst_stage", F.lit("No Loan")))
    )
    return out


def build_customer_360(spark: SparkSession, s: Settings) -> DataFrame:
    feats = customer_features(spark, s, s.run_date)
    balances = _silver(spark, s, "accounts").groupBy("customer_id").agg(
        F.sum("balance").cast("decimal(18,2)").alias("deposit_balance")
    )
    return (
        feats.join(balances, "customer_id", "left")
        .withColumn(
            "value_tier",
            F.when(F.col("deposit_balance") >= 5_000_000, "Platinum")
            .when(F.col("deposit_balance") >= 1_000_000, "Gold")
            .when(F.col("deposit_balance") >= 100_000, "Silver")
            .otherwise("Standard"),
        )
        .withColumn(
            "engagement_status",
            F.when(F.col("is_active") == 0, "Closed")
            .when(F.col("days_since_last_txn") > 60, "Dormant")
            .when(F.col("txn_trend") < 0.5, "Declining")
            .otherwise("Engaged"),
        )
    )


def build_kpi_daily(spark: SparkSession, s: Settings) -> DataFrame:
    tx = _silver(spark, s, "transactions")
    ev = _silver(spark, s, "app_events")
    crm = _silver(spark, s, "crm_interactions")
    cust = _silver(spark, s, "customers")
    acc = _silver(spark, s, "accounts")

    tx_d = tx.groupBy(F.col("txn_date").alias("kpi_date")).agg(
        F.count("*").alias("transactions"),
        F.sum("amount").cast("decimal(20,2)").alias("transaction_value"),
        F.countDistinct("customer_id").alias("transacting_customers"),
        F.avg((F.col("channel") == "App").cast("int")).alias("digital_share"),
    )
    ev_d = ev.filter("event_type = 'login'").groupBy(F.col("event_date").alias("kpi_date")).agg(
        F.countDistinct("customer_id").alias("app_daily_active_users")
    )
    crm_d = crm.groupBy(F.to_date("created_at").alias("kpi_date")).agg(
        F.sum((F.col("interaction_type") == "Complaint").cast("int")).alias("complaints_opened"),
        F.count("*").alias("crm_interactions"),
    )
    new_d = cust.groupBy(F.col("onboarding_date").alias("kpi_date")).agg(F.count("*").alias("new_customers"))
    closed_d = acc.filter(F.col("closed_date").isNotNull()).groupBy(F.col("closed_date").alias("kpi_date")) \
        .agg(F.count("*").alias("accounts_closed"))

    start = s.run_date - timedelta(days=s.history_days)
    return (
        tx_d.join(ev_d, "kpi_date", "full").join(crm_d, "kpi_date", "full")
        .join(new_d, "kpi_date", "left").join(closed_d, "kpi_date", "left")
        .filter((F.col("kpi_date") >= F.lit(start)) & (F.col("kpi_date") <= F.lit(s.run_date)))
        .fillna(0)
        .orderBy("kpi_date")
    )


def build_portfolio_quality(spark: SparkSession, s: Settings) -> DataFrame:
    loans = _silver(spark, s, "loans")
    total = loans.agg(F.sum("outstanding")).first()[0]
    return (
        loans.groupBy("product", "ifrs9_stage", "dpd_bucket")
        .agg(
            F.count("*").alias("loans"),
            F.sum("outstanding").cast("decimal(20,2)").alias("outstanding"),
            F.avg("interest_rate").alias("avg_interest_rate"),
        )
        .withColumn("share_of_book", F.col("outstanding") / F.lit(float(total)))
        .withColumn("as_of_date", F.lit(s.run_date).cast("date"))
    )


def run_gold(spark: SparkSession, s: Settings) -> None:
    for name, df in [
        ("customer_360", build_customer_360(spark, s)),
        ("kpi_daily", build_kpi_daily(spark, s)),
        ("portfolio_quality", build_portfolio_quality(spark, s)),
    ]:
        path = os.path.join(s.gold, name)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        log.info("gold.%-18s %8d rows", name, spark.read.format("delta").load(path).count())
