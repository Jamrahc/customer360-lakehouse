"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    mongo_uri: str
    mongo_db: str
    kafka_bootstrap: str
    kafka_topic: str
    lake_path: str
    n_customers: int
    history_days: int
    seed: int
    run_date: date

    # Lakehouse layer paths
    @property
    def bronze(self) -> str:
        return os.path.join(self.lake_path, "bronze")

    @property
    def silver(self) -> str:
        return os.path.join(self.lake_path, "silver")

    @property
    def gold(self) -> str:
        return os.path.join(self.lake_path, "gold")

    @property
    def state(self) -> str:
        """Watermarks and streaming checkpoints."""
        return os.path.join(self.lake_path, "_state")


def get_settings() -> Settings:
    run_date = os.getenv("C360_RUN_DATE")
    return Settings(
        pg_dsn=os.getenv("C360_PG_DSN", "postgresql://c360:c360@localhost:5432/c360"),
        mongo_uri=os.getenv("C360_MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("C360_MONGO_DB", "crm"),
        kafka_bootstrap=os.getenv("C360_KAFKA_BOOTSTRAP", "localhost:9094"),
        kafka_topic=os.getenv("C360_KAFKA_TOPIC", "app_events"),
        lake_path=os.path.abspath(os.getenv("C360_LAKE_PATH", "./lake")),
        n_customers=int(os.getenv("C360_N_CUSTOMERS", "3000")),
        history_days=int(os.getenv("C360_HISTORY_DAYS", "120")),
        seed=int(os.getenv("C360_SEED", "42")),
        run_date=date.fromisoformat(run_date) if run_date else date.today(),
    )
