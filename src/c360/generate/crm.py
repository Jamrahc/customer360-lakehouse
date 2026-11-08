"""Synthetic CRM interactions (calls, emails, branch visits, chat) as nested JSON
documents, the way a CRM or contact-centre system exports them.

Identity is deliberately messy, as it is in real CRMs: most documents carry the core
banking customer ID, some only a phone number (in several formats) or an e-mail
address (in random case), and a few match nobody. The silver layer has to resolve
them to one customer.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from .core_banking import CoreData

CHANNELS = ["Call Centre", "Email", "Branch", "Chat"]
TYPES = ["Complaint", "Query", "Service Request", "Campaign Response"]
CATEGORIES = {
    "Complaint": ["Card Blocked", "Wrong Charge", "App Login Failure", "Delayed Transfer", "Staff Behaviour"],
    "Query": ["Balance", "Loan Rate", "Statement", "Branch Hours"],
    "Service Request": ["Cheque Book", "Card Replacement", "Address Change", "Limit Increase"],
    "Campaign Response": ["Credit Card Offer", "Personal Loan Offer", "Savings Campaign"],
}


def _messy_phone(phone: str, rng: np.random.Generator) -> str:
    local = "0" + phone[4:]  # +8801712345678 -> 01712345678
    style = rng.integers(0, 4)
    if style == 0:
        return local
    if style == 1:
        return f"+880 {phone[4:8]}-{phone[8:]}"
    if style == 2:
        return phone[1:]  # 8801712345678
    return phone


def generate_crm(core: CoreData, run_date: date, history_days: int, seed: int = 42) -> list[dict]:
    rng = np.random.default_rng(seed + 7)
    start = run_date - timedelta(days=history_days)
    c = core.customers.merge(core.profiles, on="customer_id")
    docs: list[dict] = []
    seq = 0
    for row in c.itertuples(index=False):
        # churners and stressed borrowers contact the bank more, and complain more
        rate = 0.25 + 0.6 * row.churned + 0.4 * row.stressed  # per month
        n = rng.poisson(rate * history_days / 30)
        last_day = (row.churn_date - start).days if row.churned else history_days
        first_day = max(0, (row.onboarding_date - start).days)
        for _ in range(n):
            seq += 1
            day = int(rng.integers(first_day, max(first_day + 1, last_day)))
            created = datetime.combine(start + timedelta(days=day), datetime.min.time()) + timedelta(
                seconds=int(rng.integers(9 * 3600, 20 * 3600))
            )
            p_complaint = 0.55 if (row.churned or row.stressed) else 0.2
            itype = rng.choice(TYPES, p=[p_complaint, (1 - p_complaint) * 0.5,
                                         (1 - p_complaint) * 0.35, (1 - p_complaint) * 0.15])
            resolved = rng.random() < (0.6 if itype == "Complaint" else 0.9)
            ident = rng.random()
            if ident < 0.68:
                ref = {"core_id": row.customer_id}
            elif ident < 0.88:
                ref = {"phone": _messy_phone(row.phone, rng)}
            elif ident < 0.98:
                ref = {"email": row.email.upper() if rng.random() < 0.5 else row.email}
            else:
                ref = {"phone": f"01{rng.integers(3, 10)}{rng.integers(0, 10**8):08d}"}  # unknown
            docs.append(
                {
                    "_id": f"INT{seq:08d}",
                    "customer_ref": ref,
                    "channel": str(rng.choice(CHANNELS, p=[0.45, 0.2, 0.2, 0.15])),
                    "type": str(itype),
                    "category": str(rng.choice(CATEGORIES[itype])),
                    "status": "Resolved" if resolved else "Open",
                    "created_at": created,
                    "resolved_at": created + timedelta(hours=float(rng.gamma(2.0, 18.0))) if resolved else None,
                    "sentiment": float(np.clip(rng.normal(-0.4 if itype == "Complaint" else 0.3, 0.35), -1, 1)),
                    "agent": {"id": f"AG{int(rng.integers(1, 60)):03d}",
                              "team": str(rng.choice(["Retail Service", "Cards", "SME Desk"]))},
                    "tags": [str(t) for t in rng.choice(["vip", "repeat", "escalated", "digital"],
                                                        size=int(rng.integers(0, 3)), replace=False)],
                    "updated_at": created,
                }
            )
    return docs


def crm_frame(docs: list[dict]) -> pd.DataFrame:
    """Flat view, used only by tests."""
    return pd.json_normalize(docs)
