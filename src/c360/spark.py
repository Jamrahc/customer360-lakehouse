"""SparkSession factory with Delta Lake and the Kafka connector."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
POSTGRES_JDBC = "org.postgresql:postgresql:42.7.4"


def get_spark(app_name: str = "customer360", with_kafka: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("C360_SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Small local data: avoid 200 tiny shuffle partitions
        .config("spark.sql.shuffle.partitions", os.getenv("C360_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.getenv("C360_DRIVER_MEMORY", "2g"))
    )
    packages = [POSTGRES_JDBC] + ([KAFKA_PACKAGE] if with_kafka else [])
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
