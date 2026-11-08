from c360.quality.checks import accepted_values, expression, min_ratio, not_null, unique


def test_checks_count_violations(spark):
    df = spark.createDataFrame(
        [("C1", "Retail", 10.0), ("C1", "SME", -5.0), (None, "Unknown", 3.0)],
        "customer_id string, segment string, amount double",
    )
    assert not_null("customer_id")(df)[0] == 1
    assert unique("customer_id")(df)[0] == 1
    assert accepted_values("segment", ["Retail", "SME", "Corporate"])(df)[0] == 1
    assert expression("amount > 0", "amount <= 0")(df)[0] == 1


def test_min_ratio_is_table_level(spark):
    df = spark.createDataFrame([("a",), ("b",), (None,), (None,)], "customer_id string")
    failed, detail = min_ratio("customer_id IS NOT NULL", 0.95, "resolved")(df)
    assert failed == 1 and "50.0%" in detail
    failed, _ = min_ratio("customer_id IS NOT NULL", 0.4, "resolved")(df)
    assert failed == 0
