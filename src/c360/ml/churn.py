"""Churn propensity model.

Training uses point-in-time features: features are computed as of 30 days before
the run date, and the label is whether the customer closed all accounts in the 30
days that followed. Scoring then uses features as of the run date for customers who
are still active. This avoids the classic leak of training on information from the
period you are trying to predict.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import joblib
import numpy as np
import pandas as pd
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from ..config import Settings
from ..log import get_logger
from ..transform.gold import customer_features
from ..warehouse.load import copy_df

log = get_logger("churn")

HORIZON_DAYS = 30
NUMERIC = [
    "age", "tenure_days", "n_active_accounts", "n_deposit_products", "n_loans", "max_dpd",
    "txn_count_30d", "txn_count_90d", "txn_trend", "credit_amount_30d", "debit_amount_30d",
    "channels_used_90d", "app_txn_share_90d", "days_since_last_txn", "interactions_90d",
    "complaints_90d", "open_complaints", "app_sessions_30d", "session_trend", "app_errors_30d",
    "days_since_last_login", "is_digital",
]
CATEGORICAL = ["segment", "kyc_risk", "worst_stage"]


def _labels(spark: SparkSession, s: Settings, as_of) -> pd.DataFrame:
    """1 if every account the customer held on `as_of` was closed within the horizon."""
    acc = spark.read.format("delta").load(os.path.join(s.silver, "accounts"))
    end = as_of + timedelta(days=HORIZON_DAYS)
    lab = (
        acc.filter((F.col("open_date") <= F.lit(as_of)) &
                   (F.col("closed_date").isNull() | (F.col("closed_date") > F.lit(as_of))))
        .groupBy("customer_id")
        .agg(F.min(F.when(F.col("closed_date").isNotNull() & (F.col("closed_date") <= F.lit(end)), 1)
                   .otherwise(0)).alias("churned"))
    )
    return lab.toPandas()


def _model() -> Pipeline:
    pre = ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL)],
        remainder="passthrough",
    )
    clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=15,
                                         class_weight="balanced", random_state=42)
    return Pipeline([("pre", pre), ("clf", clf)])


def train(spark: SparkSession, s: Settings) -> dict:
    as_of = s.run_date - timedelta(days=HORIZON_DAYS)
    feats = customer_features(spark, s, as_of).filter("is_active = 1").toPandas()
    data = feats.merge(_labels(spark, s, as_of), on="customer_id")
    X, y = data[CATEGORICAL + NUMERIC], data["churned"].astype(int)
    log.info("Training on %d customers as of %s; churn rate %.1f%%", len(y), as_of, 100 * y.mean())

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, stratify=y, random_state=42)
    model = _model().fit(X_tr, y_tr)
    p = model.predict_proba(X_te)[:, 1]

    top = np.argsort(-p)[: max(1, len(p) // 10)]
    precision_top10 = float(y_te.iloc[top].mean())
    imp = permutation_importance(model, X_te, y_te, scoring="roc_auc", n_repeats=5, random_state=42)
    importance = sorted(zip(X.columns, imp.importances_mean), key=lambda t: -t[1])[:8]

    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    metrics = {
        "model_version": version,
        "trained_as_of": str(as_of),
        "horizon_days": HORIZON_DAYS,
        "train_rows": int(len(y_tr)),
        "test_rows": int(len(y_te)),
        "base_churn_rate": round(float(y.mean()), 4),
        "roc_auc": round(float(roc_auc_score(y_te, p)), 4),
        "pr_auc": round(float(average_precision_score(y_te, p)), 4),
        "precision_top_decile": round(precision_top10, 4),
        "lift_top_decile": round(precision_top10 / float(y_te.mean()), 2),
        "top_features": [{"feature": f, "importance": round(float(v), 4)} for f, v in importance],
    }
    out = os.path.join(s.lake_path, "models")
    os.makedirs(out, exist_ok=True)
    joblib.dump({"model": model, "version": version}, os.path.join(out, "churn_model.joblib"))
    with open(os.path.join(out, "churn_metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)
    log.info("ROC-AUC %.3f | PR-AUC %.3f | top-decile precision %.1f%% (lift %.1fx)",
             metrics["roc_auc"], metrics["pr_auc"], 100 * precision_top10, metrics["lift_top_decile"])
    return metrics


def score(spark: SparkSession, s: Settings) -> pd.DataFrame:
    bundle = joblib.load(os.path.join(s.lake_path, "models", "churn_model.joblib"))
    feats = customer_features(spark, s, s.run_date).filter("is_active = 1").toPandas()
    p = bundle["model"].predict_proba(feats[CATEGORICAL + NUMERIC])[:, 1]
    cut_high, cut_med = np.quantile(p, 0.90), np.quantile(p, 0.70)
    scores = pd.DataFrame(
        {
            "customer_id": feats["customer_id"],
            "scored_on": s.run_date,
            "churn_score": np.round(p, 4),
            "risk_band": np.where(p >= cut_high, "High", np.where(p >= cut_med, "Medium", "Low")),
            "model_version": bundle["version"],
        }
    )
    with psycopg.connect(s.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM dwh.customer_churn_scores WHERE scored_on = %s", (s.run_date,))
        copy_df(cur, "dwh.customer_churn_scores", scores)
        conn.commit()
    log.info("Scored %d active customers: %s", len(scores), scores["risk_band"].value_counts().to_dict())
    return scores
