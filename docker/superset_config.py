import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-production")
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://c360:c360@postgres:5432/superset"
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
