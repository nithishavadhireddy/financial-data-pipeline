"""
data_quality_dag.py

Runs Great Expectations validation on the staging transactions table
after each ingestion cycle.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
}

dag = DAG(
    "financial_data_quality",
    default_args=default_args,
    description="Run Great Expectations checks on financial staging tables",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["financial", "data-quality", "great-expectations"],
)


def run_ge_validation(**context):
    import great_expectations as gx

    context_ge = gx.get_context()
    result = context_ge.run_checkpoint(checkpoint_name="transactions_checkpoint")

    if not result["success"]:
        failed = [
            k for k, v in result["run_results"].items()
            if not v["validation_result"]["success"]
        ]
        raise ValueError(f"Data quality checks failed for: {failed}")

    print("All data quality checks passed.")


validate_task = PythonOperator(
    task_id="validate_staging_transactions",
    python_callable=run_ge_validation,
    dag=dag,
)
