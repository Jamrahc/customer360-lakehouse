"""Synthetic core banking data: customers, deposit accounts, loans and transactions.

Every number here is fake. A hidden 'churn' and 'stress' profile per customer drives
behaviour (falling activity before an account closure, missed loan payments), so the
downstream analytics and the churn model have real signal to find.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

DISTRICTS = [
    "Dhaka", "Chattogram", "Gazipur", "Narayanganj", "Sylhet", "Khulna",
    "Rajshahi", "Cumilla", "Bogura", "Rangpur", "Mymensingh", "Barishal",
]
SEGMENTS = ["Retail", "SME", "Corporate"]
DEPOSIT_PRODUCTS = ["Savings", "Current", "Fixed Deposit"]
LOAN_PRODUCTS = ["Personal Loan", "Home Loan", "Auto Loan", "SME Term Loan", "Credit Card"]
CHANNELS = ["Branch", "ATM", "App", "POS", "Internet Banking"]
MERCHANT_CATEGORIES = ["Groceries", "Utilities", "Telecom", "Fuel", "Dining", "Travel",
                       "Education", "Healthcare", "Transfers", "Salary"]


@dataclass
class CoreData:
    customers: pd.DataFrame
    accounts: pd.DataFrame
    loans: pd.DataFrame
    transactions: pd.DataFrame
    # Ground truth kept only for tests and the README's evaluation; never loaded
    # into the warehouse, so the model can't peek at it.
    profiles: pd.DataFrame


def _phone(rng: np.random.Generator, n: int) -> np.ndarray:
    operator = rng.choice(list("3456789"), size=n)
    tail = rng.integers(0, 10**8, size=n)
    return np.array([f"+8801{o}{t:08d}" for o, t in zip(operator, tail)])


def generate_core(
    n_customers: int, run_date: date, history_days: int, seed: int = 42
) -> CoreData:
    rng = np.random.default_rng(seed)
    fake = Faker()
    Faker.seed(seed)
    start = run_date - timedelta(days=history_days)

    # ---- customers -------------------------------------------------------------
    ids = np.array([f"C{i:06d}" for i in range(1, n_customers + 1)])
    segment = rng.choice(SEGMENTS, size=n_customers, p=[0.80, 0.15, 0.05])
    kyc = rng.choice(["Low", "Medium", "High"], size=n_customers, p=[0.6, 0.3, 0.1])
    age = rng.integers(21, 70, size=n_customers)
    tenure_days = rng.integers(30, 365 * 8, size=n_customers)
    onboarding = np.array([run_date - timedelta(days=int(t)) for t in tenure_days])
    names = [fake.name() for _ in range(n_customers)]
    emails = [
        f"{n.lower().replace(' ', '.').replace(',', '')}{i % 97}@example.com"
        for i, n in enumerate(names)
    ]

    # Hidden profiles: churn propensity and credit stress
    digital = rng.random(n_customers) < np.where(age < 40, 0.80, 0.50)
    logit = (
        -2.4
        + 0.9 * (kyc == "High")
        + 0.6 * (segment == "Retail")
        + 0.7 * (tenure_days < 365)
        - 0.4 * digital
        + rng.normal(0, 0.6, n_customers)
    )
    churn_p = 1 / (1 + np.exp(-logit))
    churned = rng.random(n_customers) < churn_p
    churn_offset = rng.integers(0, 30, size=n_customers)  # closes in the last 30 days
    churn_date = np.array(
        [run_date - timedelta(days=int(o)) if c else None for o, c in zip(churn_offset, churned)],
        dtype=object,
    )
    stressed = rng.random(n_customers) < np.where(churned, 0.25, 0.08)

    customers = pd.DataFrame(
        {
            "customer_id": ids,
            "full_name": names,
            "date_of_birth": [run_date - timedelta(days=int(a * 365.25)) for a in age],
            "segment": segment,
            "district": rng.choice(DISTRICTS, size=n_customers),
            "kyc_risk": kyc,
            "onboarding_date": onboarding,
            "phone": _phone(rng, n_customers),
            "email": emails,
            "updated_at": pd.to_datetime(onboarding),
        }
    )

    # ---- deposit accounts --------------------------------------------------------
    n_acc = rng.integers(1, 4, size=n_customers)
    acc_cust = np.repeat(np.arange(n_customers), n_acc)
    balance_scale = np.select(
        [segment[acc_cust] == "Retail", segment[acc_cust] == "SME"], [11.0, 13.0], 15.0
    )
    first_of_customer = np.r_[True, acc_cust[1:] != acc_cust[:-1]]
    acc_open = [
        onboarding[c] if first
        else onboarding[c] + timedelta(days=int(rng.integers(0, max(1, (run_date - onboarding[c]).days))))
        for c, first in zip(acc_cust, first_of_customer)
    ]
    closed = churned[acc_cust]
    accounts = pd.DataFrame(
        {
            "account_id": [f"A{i:07d}" for i in range(1, len(acc_cust) + 1)],
            "customer_id": ids[acc_cust],
            "product": rng.choice(DEPOSIT_PRODUCTS, size=len(acc_cust), p=[0.6, 0.3, 0.1]),
            "open_date": acc_open,
            "status": np.where(closed, "Closed", "Active"),
            "closed_date": [churn_date[c] for c in acc_cust],
            "balance": np.round(
                np.where(closed, 0.0, rng.lognormal(balance_scale, 1.0)), 2
            ),
        }
    )

    # ---- loans -----------------------------------------------------------------
    has_loan = rng.random(n_customers) < np.where(segment == "Retail", 0.35, 0.65)
    loan_cust = np.where(has_loan)[0]
    loan_product = np.array(
        [
            rng.choice(["SME Term Loan", "Credit Card"], p=[0.8, 0.2])
            if segment[c] != "Retail"
            else rng.choice(["Personal Loan", "Home Loan", "Auto Loan", "Credit Card"],
                            p=[0.4, 0.15, 0.15, 0.3])
            for c in loan_cust
        ]
    )
    sanctioned = np.round(rng.lognormal(np.where(segment[loan_cust] == "Retail", 13, 15.5), 0.6), -3)
    dpd = np.where(
        stressed[loan_cust],
        rng.choice([15, 35, 65, 95, 140, 200], size=len(loan_cust)),
        np.where(rng.random(len(loan_cust)) < 0.06, rng.integers(1, 30, len(loan_cust)), 0),
    )
    loans = pd.DataFrame(
        {
            "loan_id": [f"L{i:07d}" for i in range(1, len(loan_cust) + 1)],
            "customer_id": ids[loan_cust],
            "product": loan_product,
            "sanction_date": [
                max(onboarding[c], run_date - timedelta(days=int(rng.integers(60, 1500))))
                for c in loan_cust
            ],
            "sanctioned_amount": sanctioned,
            "outstanding": np.round(sanctioned * rng.uniform(0.2, 0.95, len(loan_cust)), 2),
            "interest_rate": np.round(rng.uniform(9.0, 16.0, len(loan_cust)), 2),
            "tenor_months": rng.choice([12, 24, 36, 60, 120, 240], size=len(loan_cust)),
            "days_past_due": dpd.astype(int),
            "as_of_date": run_date,
        }
    )

    # ---- transactions ----------------------------------------------------------
    days = np.array([start + timedelta(days=d) for d in range(history_days + 1)])
    rate = np.select([segment == "Retail", segment == "SME"], [0.6, 1.4], 2.5)
    # activity fades over the 30 days before a churner closes, then stops
    day_idx = np.arange(len(days))[None, :]
    churn_idx = np.array(
        [(cd - start).days if cd is not None else 10**6 for cd in churn_date]
    )[:, None]
    fade = np.clip((churn_idx - day_idx) / 30.0, 0.0, 1.0)
    onboard_idx = np.array([(o - start).days for o in onboarding])[:, None]
    lam = rate[:, None] * fade * (day_idx >= onboard_idx)  # nothing before onboarding
    counts = rng.poisson(lam)
    cust_idx, day_pos = np.nonzero(counts)
    reps = counts[cust_idx, day_pos]
    cust_idx = np.repeat(cust_idx, reps)
    day_pos = np.repeat(day_pos, reps)
    n_tx = len(cust_idx)

    # first deposit account of each customer carries the transactions
    first_acc = accounts.groupby("customer_id")["account_id"].first()
    is_digital = digital[cust_idx]
    channel = np.where(
        is_digital & (rng.random(n_tx) < 0.55),
        "App",
        rng.choice(["Branch", "ATM", "POS", "Internet Banking"], size=n_tx, p=[0.25, 0.35, 0.3, 0.1]),
    )
    seconds = rng.integers(8 * 3600, 22 * 3600, size=n_tx)
    txn_ts = pd.to_datetime(days[day_pos]) + pd.to_timedelta(seconds, unit="s")
    direction = np.where(rng.random(n_tx) < 0.35, "CR", "DR")
    amount = np.round(
        rng.lognormal(np.select([segment[cust_idx] == "Retail", segment[cust_idx] == "SME"],
                                [7.5, 9.5], 11.0), 0.9),
        2,
    )
    transactions = pd.DataFrame(
        {
            "txn_id": [f"T{i:09d}" for i in range(1, n_tx + 1)],
            "account_id": first_acc.loc[ids[cust_idx]].to_numpy(),
            "txn_ts": txn_ts,
            "amount": amount,
            "direction": direction,
            "channel": channel,
            "merchant_category": np.where(
                direction == "CR",
                rng.choice(["Salary", "Transfers"], size=n_tx),
                rng.choice(MERCHANT_CATEGORIES[:-1], size=n_tx),
            ),
        }
    ).sort_values("txn_ts", ignore_index=True)

    profiles = pd.DataFrame(
        {
            "customer_id": ids,
            "churned": churned,
            "churn_date": churn_date,
            "stressed": stressed,
            "digital": digital,
        }
    )
    return CoreData(customers, accounts, loans, transactions, profiles)


def simulate_customer_changes(
    customers: pd.DataFrame, fraction: float, seed: int, changed_at: pd.Timestamp
) -> pd.DataFrame:
    """Return a copy where a small share of customers moved district, changed segment
    or were re-rated for KYC risk. Used to demonstrate SCD Type 2 history."""
    rng = np.random.default_rng(seed + 1)
    out = customers.copy()
    idx = rng.choice(len(out), size=max(1, int(len(out) * fraction)), replace=False)
    out.loc[idx, "district"] = rng.choice(DISTRICTS, size=len(idx))
    out.loc[idx, "kyc_risk"] = rng.choice(["Low", "Medium", "High"], size=len(idx))
    out.loc[idx, "updated_at"] = changed_at
    return out
