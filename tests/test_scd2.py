"""Integration test for the SCD Type 2 merge (needs PostgreSQL: set C360_PG_DSN).
Everything runs inside one transaction that is rolled back, so it never touches real data."""

from importlib import resources

import psycopg
import pytest

pytestmark = pytest.mark.integration


def _sql(name):
    return resources.files("c360.warehouse").joinpath("sql", name).read_text()


def _stage(cur, rows):
    cur.execute("TRUNCATE dwh.stg_customer")
    cur.executemany(
        "INSERT INTO dwh.stg_customer VALUES (%s, %s, %s, %s, %s, %s, %s)", rows
    )


def test_scd2_keeps_history_and_is_idempotent(pg_dsn):
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dwh CASCADE")
        cur.execute(_sql("dwh_ddl.sql"))
        v1 = [("C1", "Ana Rahman", "Retail", "Dhaka", "Low", "2024-01-01", "2024-01-01"),
              ("C2", "Bo Islam", "SME", "Sylhet", "Medium", "2024-02-01", "2024-02-01")]
        _stage(cur, v1)
        cur.execute(_sql("scd2_customer.sql"))
        cur.execute(_sql("scd2_customer.sql"))  # same data again: no new versions
        cur.execute("SELECT count(*) FROM dwh.dim_customer")
        assert cur.fetchone()[0] == 2

        # C1 moves to Chattogram; C2 only fixes a typo in the name (Type 1)
        v2 = [("C1", "Ana Rahman", "Retail", "Chattogram", "Low", "2024-01-01", "2026-11-01 10:00"),
              ("C2", "Bo Islam Khan", "SME", "Sylhet", "Medium", "2024-02-01", "2026-11-01 10:00")]
        _stage(cur, v2)
        cur.execute(_sql("scd2_customer.sql"))
        cur.execute("SELECT customer_id, district, is_current, full_name FROM dwh.dim_customer "
                    "ORDER BY customer_id, valid_from")
        rows = cur.fetchall()
        assert rows == [
            ("C1", "Dhaka", False, "Ana Rahman"),
            ("C1", "Chattogram", True, "Ana Rahman"),
            ("C2", "Sylhet", True, "Bo Islam Khan"),
        ]
        conn.rollback()
