"""Synthetic mobile-app clickstream: sessions of app_open, login, feature use and
logout. Like real clickstreams it contains duplicates (client retries) and late
arrivals (events sent when a phone comes back online), which bronze keeps and silver
cleans."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

import numpy as np

from .core_banking import CoreData

ACTIONS = ["view_balance", "fund_transfer", "bill_payment", "mobile_recharge",
           "view_statement", "loan_offer_view", "card_controls", "error"]
ACTION_P = [0.30, 0.18, 0.14, 0.12, 0.10, 0.06, 0.05, 0.05]


def generate_app_events(core: CoreData, run_date: date, history_days: int, seed: int = 42) -> list[dict]:
    rng = np.random.default_rng(seed + 13)
    start = run_date - timedelta(days=history_days)
    prof = core.profiles.merge(core.customers[["customer_id", "onboarding_date"]], on="customer_id")
    prof = prof[prof["digital"]]
    events: list[dict] = []
    for row in prof.itertuples(index=False):
        device = f"DEV-{uuid.UUID(int=int(rng.integers(0, 2**63))).hex[:12]}"
        os_name = str(rng.choice(["Android", "iOS"], p=[0.85, 0.15]))
        churn_day = (row.churn_date - start).days if row.churned else 10**6
        first_day = max(0, (row.onboarding_date - start).days)
        for d in range(first_day, history_days + 1):
            fade = min(1.0, max(0.0, (churn_day - d) / 30.0))
            for _ in range(rng.poisson(0.35 * fade)):
                session = uuid.UUID(int=int(rng.integers(0, 2**63))).hex
                t = datetime.combine(start + timedelta(days=d), datetime.min.time()) + timedelta(
                    seconds=int(rng.integers(6 * 3600, 24 * 3600 - 600))
                )
                steps = ["app_open", "login"] + list(
                    rng.choice(ACTIONS, size=int(rng.integers(1, 5)), p=ACTION_P)
                ) + ["logout"]
                for i, step in enumerate(steps):
                    t = t + timedelta(seconds=int(rng.integers(3, 90)))
                    ev = {
                        "event_id": uuid.UUID(int=int(rng.integers(0, 2**63))).hex,
                        "event_type": step,
                        "event_ts": t.isoformat(),
                        # app_open happens before login, so it isn't tied to a customer yet
                        "customer_id": None if i == 0 else row.customer_id,
                        "device_id": device,
                        "session_id": session,
                        "os": os_name,
                        "app_version": str(rng.choice(["5.2.0", "5.3.1", "5.4.0"])),
                        "properties": {"amount": round(float(rng.lognormal(7, 1)), 2)}
                        if step in ("fund_transfer", "bill_payment", "mobile_recharge") else {},
                    }
                    events.append(ev)
                    if rng.random() < 0.01:  # client retry -> exact duplicate
                        events.append(dict(ev))
    # ~0.5% arrive late: shuffle them to the end of the stream
    late = rng.random(len(events)) < 0.005
    on_time = [e for e, lt in zip(events, late) if not lt]
    return on_time + [e for e, lt in zip(events, late) if lt]
