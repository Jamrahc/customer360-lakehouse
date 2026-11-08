from datetime import date

import pandas as pd
import pytest

from c360.generate.app_events import generate_app_events
from c360.generate.core_banking import generate_core, simulate_customer_changes
from c360.generate.crm import generate_crm

RUN = date(2026, 11, 8)


@pytest.fixture(scope="module")
def core():
    return generate_core(300, RUN, 60, seed=7)


def test_deterministic_for_a_seed(core):
    again = generate_core(300, RUN, 60, seed=7)
    pd.testing.assert_frame_equal(core.transactions, again.transactions)


def test_keys_are_unique(core):
    assert core.customers["customer_id"].is_unique
    assert core.accounts["account_id"].is_unique
    assert core.transactions["txn_id"].is_unique


def test_no_activity_before_onboarding(core):
    tx = core.transactions.merge(core.accounts[["account_id", "customer_id"]], on="account_id") \
        .merge(core.customers[["customer_id", "onboarding_date"]], on="customer_id")
    assert (tx["txn_ts"].dt.date >= tx["onboarding_date"]).all()


def test_churners_close_all_accounts_and_stop_transacting(core):
    churned = core.profiles.loc[core.profiles["churned"], ["customer_id", "churn_date"]]
    assert len(churned) > 0
    acc = core.accounts[core.accounts["customer_id"].isin(churned["customer_id"])]
    assert (acc["status"] == "Closed").all()
    tx = core.transactions.merge(core.accounts[["account_id", "customer_id"]], on="account_id") \
        .merge(churned, on="customer_id")
    assert (tx["txn_ts"].dt.date <= tx["churn_date"]).all()


def test_crm_identity_is_messy_but_mostly_linkable(core):
    docs = generate_crm(core, RUN, 60, seed=7)
    refs = pd.DataFrame([d["customer_ref"] for d in docs])
    assert refs["core_id"].notna().mean() < 0.8  # not every document has the ID
    assert refs.get("phone", pd.Series(dtype=str)).notna().any()


def test_app_events_include_duplicates(core):
    events = generate_app_events(core, RUN, 30, seed=7)
    ids = [e["event_id"] for e in events]
    assert len(ids) > len(set(ids)), "client retries should create duplicate events"
    assert all(e["customer_id"] is None for e in events if e["event_type"] == "app_open")


def test_customer_changes_touch_only_a_fraction(core):
    changed = simulate_customer_changes(core.customers, 0.05, seed=1, changed_at=pd.Timestamp("2026-11-08"))
    n = (changed["updated_at"] != core.customers["updated_at"]).sum()
    assert n == 15
