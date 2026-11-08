-- Reporting warehouse: a star schema plus wide marts for BI.
-- Tables are created once and kept, so customer history (SCD Type 2) survives reruns.
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key      INT PRIMARY KEY,          -- 20261108
    full_date     DATE NOT NULL UNIQUE,
    day_of_week   TEXT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,         -- Friday and Saturday in Bangladesh
    month_start   DATE NOT NULL,
    month_name    TEXT NOT NULL,
    quarter       INT NOT NULL,
    year          INT NOT NULL
);

-- Slowly changing dimension, Type 2: a new row whenever segment, district or KYC risk changes
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_sk      BIGSERIAL PRIMARY KEY,
    customer_id      VARCHAR(10) NOT NULL,
    full_name        TEXT NOT NULL,
    segment          TEXT NOT NULL,
    district         TEXT NOT NULL,
    kyc_risk         TEXT NOT NULL,
    onboarding_date  DATE NOT NULL,
    valid_from       TIMESTAMP NOT NULL,
    valid_to         TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    is_current       BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current
    ON dwh.dim_customer (customer_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS dwh.stg_customer (
    customer_id VARCHAR(10), full_name TEXT, segment TEXT, district TEXT,
    kyc_risk TEXT, onboarding_date DATE, updated_at TIMESTAMP
);

-- Grain: one row per customer, day and channel
CREATE TABLE IF NOT EXISTS dwh.fact_transaction_daily (
    date_key         INT NOT NULL REFERENCES dwh.dim_date,
    customer_sk      BIGINT NOT NULL REFERENCES dwh.dim_customer,
    channel          TEXT NOT NULL,
    txn_count        INT NOT NULL,
    credit_amount    NUMERIC(20, 2) NOT NULL,
    debit_amount     NUMERIC(20, 2) NOT NULL,
    PRIMARY KEY (date_key, customer_sk, channel)
);

-- Grain: one row per loan per snapshot date
CREATE TABLE IF NOT EXISTS dwh.fact_loan_snapshot (
    snapshot_date_key  INT NOT NULL REFERENCES dwh.dim_date,
    loan_id            VARCHAR(10) NOT NULL,
    customer_sk        BIGINT NOT NULL REFERENCES dwh.dim_customer,
    product            TEXT NOT NULL,
    outstanding        NUMERIC(18, 2) NOT NULL,
    days_past_due      INT NOT NULL,
    dpd_bucket         TEXT NOT NULL,
    ifrs9_stage        TEXT NOT NULL,
    PRIMARY KEY (snapshot_date_key, loan_id)
);

-- Grain: one row per customer per day
CREATE TABLE IF NOT EXISTS dwh.fact_app_engagement_daily (
    date_key      INT NOT NULL REFERENCES dwh.dim_date,
    customer_sk   BIGINT NOT NULL REFERENCES dwh.dim_customer,
    sessions      INT NOT NULL,
    events        INT NOT NULL,
    transfers     INT NOT NULL,
    errors        INT NOT NULL,
    PRIMARY KEY (date_key, customer_sk)
);

CREATE TABLE IF NOT EXISTS dwh.dq_results (
    run_at       TIMESTAMP NOT NULL,
    layer        TEXT NOT NULL,
    table_name   TEXT NOT NULL,
    check_name   TEXT NOT NULL,
    severity     TEXT NOT NULL,
    failed_rows  BIGINT NOT NULL,
    passed       BOOLEAN NOT NULL,
    detail       TEXT
);

CREATE TABLE IF NOT EXISTS dwh.customer_churn_scores (
    customer_id     VARCHAR(10) NOT NULL,
    scored_on       DATE NOT NULL,
    churn_score     DOUBLE PRECISION NOT NULL,
    risk_band       TEXT NOT NULL,
    model_version   TEXT NOT NULL,
    PRIMARY KEY (customer_id, scored_on)
);

-- Helpful indexes for dashboard filters
CREATE INDEX IF NOT EXISTS ix_fact_txn_customer ON dwh.fact_transaction_daily (customer_sk);
CREATE INDEX IF NOT EXISTS ix_fact_loan_stage ON dwh.fact_loan_snapshot (snapshot_date_key, ifrs9_stage);
