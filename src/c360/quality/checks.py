"""Data quality checks, run between layers. A failed 'error' check stops the
pipeline before bad data reaches the warehouse; 'warn' checks are recorded only.
Every result is written to the lake (and later the warehouse) as an audit trail."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Settings
from ..log import get_logger

log = get_logger("quality")


class DataQualityError(RuntimeError):
    pass


@dataclass
class Check:
    name: str
    table: str
    severity: str  # "error" | "warn"
    # returns (number of failing rows or 0/1 for table-level checks, detail text)
    fn: Callable[[DataFrame], tuple[int, str]]


# ------------------------------------------------------------ check types ----
def not_null(col: str) -> Callable[[DataFrame], tuple[int, str]]:
    return lambda df: (df.filter(F.col(col).isNull()).count(), f"{col} IS NULL")


def unique(col: str) -> Callable[[DataFrame], tuple[int, str]]:
    def fn(df: DataFrame) -> tuple[int, str]:
        dupes = df.groupBy(col).count().filter("count > 1").count()
        return dupes, f"duplicate {col} values"
    return fn


def accepted_values(col: str, values: list[str]) -> Callable[[DataFrame], tuple[int, str]]:
    return lambda df: (df.filter(~F.col(col).isin(values)).count(), f"{col} not in {values}")


def expression(sql_condition: str, label: str) -> Callable[[DataFrame], tuple[int, str]]:
    """Rows that violate `sql_condition`."""
    return lambda df: (df.filter(f"NOT ({sql_condition})").count(), label)


def min_ratio(sql_condition: str, threshold: float, label: str) -> Callable[[DataFrame], tuple[int, str]]:
    def fn(df: DataFrame) -> tuple[int, str]:
        total = df.count()
        ok = df.filter(sql_condition).count()
        ratio = ok / total if total else 1.0
        return int(ratio < threshold), f"{label}: {ratio:.1%} (threshold {threshold:.0%})"
    return fn


# ----------------------------------------------------------------- suites ----
def silver_checks(s: Settings) -> list[Check]:
    future = (s.run_date + timedelta(days=1)).isoformat()
    return [
        Check("customer_id not null", "customers", "error", not_null("customer_id")),
        Check("customer_id unique", "customers", "error", unique("customer_id")),
        Check("segment valid", "customers", "error", accepted_values("segment", ["Retail", "SME", "Corporate"])),
        Check("phone in E.164 format", "customers", "warn", not_null("phone")),
        Check("account_id unique", "accounts", "error", unique("account_id")),
        Check("txn_id unique", "transactions", "error", unique("txn_id")),
        Check("amount positive", "transactions", "error", expression("amount > 0", "amount <= 0")),
        Check("no future transactions", "transactions", "error",
              expression(f"txn_ts < TIMESTAMP '{future}'", "txn_ts in the future")),
        Check("transaction has a customer", "transactions", "error", not_null("customer_id")),
        Check("dpd not negative", "loans", "error", expression("days_past_due >= 0", "days_past_due < 0")),
        Check("outstanding within sanction", "loans", "warn",
              expression("outstanding <= sanctioned_amount * 1.2", "outstanding > 120% of sanctioned")),
        Check("CRM identity resolution rate", "crm_interactions", "warn",
              min_ratio("customer_id IS NOT NULL", 0.95, "resolved")),
        Check("event_id unique", "app_events", "error", unique("event_id")),
        Check("only app_open is anonymous", "app_events", "warn",
              expression("customer_id IS NOT NULL OR event_type = 'app_open'", "anonymous non-open event")),
    ]


def gold_checks(s: Settings) -> list[Check]:
    return [
        Check("one row per customer", "customer_360", "error", unique("customer_id")),
        Check("stage valid", "customer_360", "error",
              accepted_values("worst_stage", ["No Loan", "Stage 1", "Stage 2", "Stage 3"])),
        Check("share of book sums to 100%", "portfolio_quality", "error",
              lambda df: (int(abs((df.agg(F.sum("share_of_book")).first()[0] or 0) - 1) > 1e-6),
                          "sum(share_of_book) != 1")),
        Check("KPI dates unique", "kpi_daily", "error", unique("kpi_date")),
    ]


def reconciliation_checks(spark: SparkSession, s: Settings) -> list[tuple[str, str, int, str]]:
    """Source-to-lake reconciliation: row counts must match the core banking system."""
    with psycopg.connect(s.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM core.transactions")
        src_tx = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM core.customers")
        src_cust = cur.fetchone()[0]
    lake_tx = spark.read.format("delta").load(os.path.join(s.silver, "transactions")).count()
    lake_cust = spark.read.format("delta").load(os.path.join(s.silver, "customers")).count()
    return [
        ("transactions reconcile to source", "transactions", int(src_tx != lake_tx),
         f"source={src_tx} lake={lake_tx}"),
        ("customers reconcile to source", "customers", int(src_cust != lake_cust),
         f"source={src_cust} lake={lake_cust}"),
    ]


def run_checks(spark: SparkSession, s: Settings, layer: str) -> list[dict]:
    base = s.silver if layer == "silver" else s.gold
    checks = silver_checks(s) if layer == "silver" else gold_checks(s)
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    results: list[dict] = []
    cache: dict[str, DataFrame] = {}
    for c in checks:
        df = cache.setdefault(c.table, spark.read.format("delta").load(os.path.join(base, c.table)))
        failed, detail = c.fn(df)
        results.append(dict(run_at=run_at, layer=layer, table_name=c.table, check_name=c.name,
                            severity=c.severity, failed_rows=int(failed), passed=failed == 0, detail=detail))
    if layer == "silver":
        for name, table, failed, detail in reconciliation_checks(spark, s):
            results.append(dict(run_at=run_at, layer=layer, table_name=table, check_name=name,
                                severity="error", failed_rows=failed, passed=failed == 0, detail=detail))

    for r in results:
        mark = "PASS" if r["passed"] else ("FAIL" if r["severity"] == "error" else "WARN")
        log.info("[%s] %-6s %-17s %-34s %s", mark, layer, r["table_name"], r["check_name"],
                 "" if r["passed"] else r["detail"])

    spark.createDataFrame(results).write.format("delta").mode("append") \
        .option("mergeSchema", "true").save(os.path.join(s.gold, "dq_results"))

    errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if errors:
        raise DataQualityError(f"{len(errors)} {layer} check(s) failed: " +
                               "; ".join(f"{e['table_name']}.{e['check_name']}" for e in errors))
    return results
