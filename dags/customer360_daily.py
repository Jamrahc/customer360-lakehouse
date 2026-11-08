"""Daily Customer 360 pipeline.

bronze -> silver -> quality gate -> gold -> quality gate -> warehouse -> churn scores
-> notify (n8n distributes the daily summary). The model retrains on Sundays only;
the scoring task still runs every day with the latest model.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.operators.bash import BashOperator

SCRIPT = "bash /opt/c360/scripts/run_pipeline.sh"
# the run date is the end of the data interval, i.e. the day the DAG runs for
ENV = {"C360_RUN_DATE": "{{ data_interval_end | ds }}", "MAX_RETRIES": "0"}

default_args = {
    "owner": "data-engineering",
    "retries": 2,                       # Airflow-level retries for infrastructure hiccups
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="customer360_daily",
    description="Core banking + CRM + app events -> lakehouse -> warehouse -> churn scores",
    schedule="0 6 * * *",               # 06:00 Dhaka time, before the business day starts
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Dhaka"),
    catchup=False,
    max_active_runs=1,                  # runs never overlap
    default_args=default_args,
    tags=["customer360", "lakehouse"],
    doc_md=__doc__,
) as dag:

    def step(name: str, **kwargs) -> BashOperator:
        return BashOperator(
            task_id=name.replace("-", "_"),
            bash_command=f"{SCRIPT} {name}",
            env=ENV,
            append_env=True,
            **kwargs,
        )

    health = BashOperator(
        task_id="check_sources",
        # trailing space: stops Airflow treating a command ending in ".sh" as a template file
        bash_command="bash /opt/c360/scripts/healthcheck.sh ",
        retries=3,
    )
    bronze = step("bronze")
    silver = step("silver")
    dq_silver = step("dq-silver", retries=0)   # a data problem won't fix itself on retry
    gold = step("gold")
    dq_gold = step("dq-gold", retries=0)
    load = step("load")
    train = BashOperator(
        task_id="train_model",
        # exit code 99 marks the task as skipped on every day except Sunday
        bash_command=f'[ "$(date -d {{{{ data_interval_end | ds }}}} +%u)" = "7" ] || exit 99; {SCRIPT} train',
        env=ENV,
        append_env=True,
    )
    score = step("score", trigger_rule="none_failed")
    notify = BashOperator(
        task_id="notify_n8n",
        bash_command=(
            'curl -sf -X POST "$C360_N8N_WEBHOOK" -H "Content-Type: application/json" '
            "-d '{\"dag_run\": \"{{ run_id }}\", \"run_date\": \"{{ data_interval_end | ds }}\"}' "
            '|| echo "n8n not reachable; skipping notification"'
        ),
        trigger_rule="none_failed",
    )

    health >> bronze >> silver >> dq_silver >> gold >> dq_gold >> load >> train >> score >> notify
