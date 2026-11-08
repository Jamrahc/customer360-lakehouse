from pyspark.sql import functions as F

from c360.transform.silver import dedupe_latest, dpd_bucket, ifrs9_stage, normalize_phone


def test_normalize_phone_handles_local_formats(spark):
    df = spark.createDataFrame(
        [("01712345678",), ("+880 1712-345678",), ("8801712345678",), ("+8801712345678",),
         ("02-9876543",), (None,)],
        "phone string",
    )
    out = [r[0] for r in df.select(normalize_phone(F.col("phone"))).collect()]
    assert out == ["+8801712345678"] * 4 + [None, None]


def test_dpd_bucket_and_stage_boundaries(spark):
    df = spark.createDataFrame([(d,) for d in [0, 1, 29, 30, 59, 60, 89, 90, 400]], "dpd int")
    rows = df.select(dpd_bucket(F.col("dpd")), ifrs9_stage(F.col("dpd"))).collect()
    assert [r[0] for r in rows] == ["Current", "1-29", "1-29", "30-59", "30-59", "60-89", "60-89", "90+", "90+"]
    assert [r[1] for r in rows] == ["Stage 1"] * 3 + ["Stage 2"] * 4 + ["Stage 3"] * 2


def test_dedupe_latest_keeps_newest_version(spark):
    df = spark.createDataFrame(
        [("C1", "old", 1), ("C1", "new", 2), ("C2", "only", 1)], "id string, v string, ts int"
    )
    out = {r.id: r.v for r in dedupe_latest(df, "id", "ts").collect()}
    assert out == {"C1": "new", "C2": "only"}
