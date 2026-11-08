-- Source system: a simplified core banking database (fake data only)
CREATE SCHEMA IF NOT EXISTS core;

DROP TABLE IF EXISTS core.transactions, core.loans, core.accounts, core.customers;

CREATE TABLE core.customers (
    customer_id      VARCHAR(10) PRIMARY KEY,
    full_name        TEXT        NOT NULL,
    date_of_birth    DATE        NOT NULL,
    segment          TEXT        NOT NULL,
    district         TEXT        NOT NULL,
    kyc_risk         TEXT        NOT NULL,
    onboarding_date  DATE        NOT NULL,
    phone            TEXT        NOT NULL,
    email            TEXT        NOT NULL,
    updated_at       TIMESTAMP   NOT NULL
);

CREATE TABLE core.accounts (
    account_id   VARCHAR(10) PRIMARY KEY,
    customer_id  VARCHAR(10) NOT NULL REFERENCES core.customers,
    product      TEXT        NOT NULL,
    open_date    DATE        NOT NULL,
    status       TEXT        NOT NULL,
    closed_date  DATE,
    balance      NUMERIC(18, 2) NOT NULL
);

CREATE TABLE core.loans (
    loan_id            VARCHAR(10) PRIMARY KEY,
    customer_id        VARCHAR(10) NOT NULL REFERENCES core.customers,
    product            TEXT        NOT NULL,
    sanction_date      DATE        NOT NULL,
    sanctioned_amount  NUMERIC(18, 2) NOT NULL,
    outstanding        NUMERIC(18, 2) NOT NULL,
    interest_rate      NUMERIC(5, 2)  NOT NULL,
    tenor_months       INT         NOT NULL,
    days_past_due      INT         NOT NULL,
    as_of_date         DATE        NOT NULL
);

CREATE TABLE core.transactions (
    txn_id             VARCHAR(12) PRIMARY KEY,
    account_id         VARCHAR(10) NOT NULL REFERENCES core.accounts,
    txn_ts             TIMESTAMP   NOT NULL,
    amount             NUMERIC(18, 2) NOT NULL,
    direction          CHAR(2)     NOT NULL,
    channel            TEXT        NOT NULL,
    merchant_category  TEXT        NOT NULL
);

-- Incremental extraction reads transactions by timestamp
CREATE INDEX ix_transactions_ts ON core.transactions (txn_ts);
