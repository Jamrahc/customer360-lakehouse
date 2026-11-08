"""Load gold data into the PostgreSQL reporting warehouse.

* dim_customer is maintained as SCD Type 2 (history of segment / district / KYC risk).
* Facts are reloaded for the whole rolling window inside one transaction, so a
  rerun produces exactly the same result (idempotent) and readers never see a
  half-loaded table.
* Fact rows join to the customer version that was valid on the fact's date
  (point-in-time surrogate key lookup).
"""

from __future__ import annotations

import io
import os
from datetime import date, timedelta
from decimal import Decimal
from importlib import resources

import pandas as pd
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..config import Settings
from ..log import get_logger

log = get_logger("warehouse")


def _sql(name: str) -> str:
    return resources.files("c360.warehouse").joinpath("sql", name).read_text()


def _pg_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    sample = series.dropna()
    if not sample.empty:
        v = sample.iloc[0]
        if isinstance(v, Decimal):
            return "NUMERIC(20, 2)"
        if isinstance(v, date):
            return "DATE"
    return "TEXT"


def copy_df(cur: psycopg.Cursor, table: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with cur.copy(f"COPY {table} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
        cp.write(buf.read())


def replace_table(cur: psycopg.Cursor, table: str, df: pd.DataFrame) -> None:
    """Rebuild a mart table from a DataFrame (types inferred)."""
    cols = ", ".join(f"{c} {_pg_type(df[c])}" for c in df.columns)
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(f"CREATE TABLE {table} ({cols})")
    copy_df(cur, table, df)


def _date_dim(start: date, end: date) -> pd.DataFrame:
    days = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date_key": days.strftime("%Y%m%d").astype(int),
            "full_date": days.date,
            "day_of_week": days.day_name(),
            "is_weekend": days.dayofweek.isin([4, 5]),  # Fri, Sat
            "month_start": days.to_period("M").start_time.date,
            "month_name": days.strftime("%b %Y"),
            "quarter": days.quarter,
            "year": days.year,
        }
    )


def _point_in_time_insert(target: str, cols: str, select_cols: str, stage: str) -> str:
    return f"""
        INSERT INTO {target} ({cols})
        SELECT {select_cols}
        FROM {stage} s
        CROSS JOIN LATERAL (
            SELECT d.customer_sk
            FROM dwh.dim_customer d
            WHERE d.customer_id = s.customer_id
            ORDER BY (s.day >= d.valid_from::date AND s.day < d.valid_to::date) DESC, d.is_current DESC
            LIMIT 1
        ) c
    """


def run_load(spark: SparkSession, s: Settings) -> None:
    rd = lambda layer, name: spark.read.format("delta").load(os.path.join(layer, name))  # noqa: E731
    start = s.run_date - timedelta(days=s.history_days)

    c360 = rd(s.gold, "customer_360").toPandas()
    stg_customer = c360[["customer_id", "full_name", "segment", "district", "kyc_risk", "onboarding_date"]].merge(
        rd(s.silver, "customers").select("customer_id", "updated_at").toPandas(), on="customer_id"
    )

    tx_daily = (
        rd(s.silver, "transactions")
        .groupBy("customer_id", F.col("txn_date").alias("day"), "channel")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(F.when(F.col("direction") == "CR", F.col("amount")).otherwise(0)).cast("decimal(20,2)")
            .alias("credit_amount"),
            F.sum(F.when(F.col("direction") == "DR", F.col("amount")).otherwise(0)).cast("decimal(20,2)")
            .alias("debit_amount"),
        )
        .toPandas()
    )
    app_daily = (
        rd(s.silver, "app_events").filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id", F.col("event_date").alias("day"))
        .agg(
            F.countDistinct("session_id").alias("sessions"),
            F.count("*").alias("events"),
            F.sum((F.col("event_type") == "fund_transfer").cast("int")).alias("transfers"),
            F.sum((F.col("event_type") == "error").cast("int")).alias("errors"),
        )
        .toPandas()
    )
    loans = rd(s.silver, "loans").select(
        "loan_id", "customer_id", "product", "outstanding", "days_past_due", "dpd_bucket", "ifrs9_stage"
    ).toPandas()
    loans.insert(0, "day", s.run_date)

    kpi = rd(s.gold, "kpi_daily").toPandas()
    portfolio = rd(s.gold, "portfolio_quality").toPandas()
    dq_path = os.path.join(s.gold, "dq_results")
    dq = rd(s.gold, "dq_results").toPandas() if os.path.exists(dq_path) else pd.DataFrame()

    with psycopg.connect(s.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(_sql("dwh_ddl.sql"))

        # dim_date: insert any missing days
        cur.execute("CREATE TEMP TABLE stg_date (LIKE dwh.dim_date) ON COMMIT DROP")
        copy_df(cur, "stg_date", _date_dim(min(start, c360["onboarding_date"].min()), s.run_date + timedelta(days=30)))
        cur.execute("INSERT INTO dwh.dim_date SELECT * FROM stg_date ON CONFLICT (date_key) DO NOTHING")

        # dim_customer: SCD Type 2
        cur.execute("TRUNCATE dwh.stg_customer")
        copy_df(cur, "dwh.stg_customer", stg_customer)
        cur.execute(_sql("scd2_customer.sql"))
        cur.execute("SELECT count(*), count(*) FILTER (WHERE is_current) FROM dwh.dim_customer")
        total, current = cur.fetchone()
        log.info("dim_customer: %d versions, %d current", total, current)

        # facts: reload the window atomically
        k0, k1 = int(start.strftime("%Y%m%d")), int(s.run_date.strftime("%Y%m%d"))
        cur.execute("CREATE TEMP TABLE stg_tx (customer_id TEXT, day DATE, channel TEXT, txn_count INT, "
                    "credit_amount NUMERIC(20,2), debit_amount NUMERIC(20,2)) ON COMMIT DROP")
        copy_df(cur, "stg_tx", tx_daily)
        cur.execute("DELETE FROM dwh.fact_transaction_daily WHERE date_key BETWEEN %s AND %s", (k0, k1))
        cur.execute(_point_in_time_insert(
            "dwh.fact_transaction_daily",
            "date_key, customer_sk, channel, txn_count, credit_amount, debit_amount",
            "to_char(s.day, 'YYYYMMDD')::int, c.customer_sk, s.channel, s.txn_count, s.credit_amount, s.debit_amount",
            "stg_tx"))
        log.info("fact_transaction_daily: %d rows", cur.rowcount)

        cur.execute("CREATE TEMP TABLE stg_app (customer_id TEXT, day DATE, sessions INT, events INT, "
                    "transfers INT, errors INT) ON COMMIT DROP")
        copy_df(cur, "stg_app", app_daily)
        cur.execute("DELETE FROM dwh.fact_app_engagement_daily WHERE date_key BETWEEN %s AND %s", (k0, k1))
        cur.execute(_point_in_time_insert(
            "dwh.fact_app_engagement_daily",
            "date_key, customer_sk, sessions, events, transfers, errors",
            "to_char(s.day, 'YYYYMMDD')::int, c.customer_sk, s.sessions, s.events, s.transfers, s.errors",
            "stg_app"))
        log.info("fact_app_engagement_daily: %d rows", cur.rowcount)

        cur.execute("CREATE TEMP TABLE stg_loan (day DATE, loan_id TEXT, customer_id TEXT, product TEXT, "
                    "outstanding NUMERIC(18,2), days_past_due INT, dpd_bucket TEXT, ifrs9_stage TEXT) ON COMMIT DROP")
        copy_df(cur, "stg_loan", loans)
        cur.execute("DELETE FROM dwh.fact_loan_snapshot WHERE snapshot_date_key = %s", (k1,))
        cur.execute(_point_in_time_insert(
            "dwh.fact_loan_snapshot",
            "snapshot_date_key, loan_id, customer_sk, product, outstanding, days_past_due, dpd_bucket, ifrs9_stage",
            "to_char(s.day, 'YYYYMMDD')::int, s.loan_id, c.customer_sk, s.product, s.outstanding, "
            "s.days_past_due, s.dpd_bucket, s.ifrs9_stage",
            "stg_loan"))
        log.info("fact_loan_snapshot: %d rows", cur.rowcount)

        # marts for BI (wide, denormalised, rebuilt each run)
        replace_table(cur, "dwh.mart_customer_360", c360)
        replace_table(cur, "dwh.mart_kpi_daily", kpi)
        replace_table(cur, "dwh.mart_portfolio_quality", portfolio)
        log.info("marts: customer_360=%d, kpi_daily=%d, portfolio_quality=%d rows",
                 len(c360), len(kpi), len(portfolio))

        if not dq.empty:
            cur.execute("TRUNCATE dwh.dq_results")
            copy_df(cur, "dwh.dq_results", dq[["run_at", "layer", "table_name", "check_name", "severity",
                                               "failed_rows", "passed", "detail"]])
        conn.commit()
    log.info("Warehouse load committed")
