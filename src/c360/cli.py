"""Command line entry point: `python -m c360 <step>`.

Steps run in this order: generate -> bronze -> silver -> dq-silver -> gold ->
dq-gold -> load -> train -> score. `all` runs bronze to score (not generate)."""

from __future__ import annotations

import argparse
import sys
import time

from .config import get_settings
from .log import get_logger

log = get_logger("c360")

STEPS = ["generate", "change-customers", "bronze", "silver", "dq-silver", "gold", "dq-gold",
         "load", "train", "score", "all"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="c360", description=__doc__)
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--skip-events", action="store_true", help="generate: don't produce Kafka events")
    args = parser.parse_args(argv)
    s = get_settings()
    t0 = time.time()

    if args.step == "generate":
        from .generate.load_sources import generate_all
        generate_all(s, skip_events=args.skip_events)
    elif args.step == "change-customers":
        from .generate.load_sources import apply_customer_changes
        apply_customer_changes(s)
    else:
        from .spark import get_spark
        spark = get_spark(f"c360-{args.step}")
        steps = ["bronze", "silver", "dq-silver", "gold", "dq-gold", "load", "train", "score"] \
            if args.step == "all" else [args.step]
        from .quality.checks import DataQualityError

        try:
            _run_steps(spark, s, steps)
        except DataQualityError as e:
            log.error("Data quality gate failed: %s", e)
            spark.stop()
            return 3  # distinct exit code: a data problem, so callers should not retry
        spark.stop()
    log.info("Finished %s in %.1fs", args.step, time.time() - t0)
    return 0


def _run_steps(spark, s, steps: list[str]) -> None:
    for step in steps:
        t = time.time()
        log.info("=== %s ===", step)
        if step == "bronze":
            from .ingest.bronze import run_bronze
            run_bronze(spark, s)
        elif step == "silver":
            from .transform.silver import run_silver
            run_silver(spark, s)
        elif step in ("dq-silver", "dq-gold"):
            from .quality.checks import run_checks
            run_checks(spark, s, step.split("-")[1])
        elif step == "gold":
            from .transform.gold import run_gold
            run_gold(spark, s)
        elif step == "load":
            from .warehouse.load import run_load
            run_load(spark, s)
        elif step == "train":
            from .ml.churn import train
            train(spark, s)
        elif step == "score":
            from .ml.churn import score
            score(spark, s)
        log.info("=== %s done in %.1fs ===", step, time.time() - t)


if __name__ == "__main__":
    sys.exit(main())
