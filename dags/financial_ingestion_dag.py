"""
financial_ingestion_dag.py

Airflow DAG: Kafka -> S3 ingestion for financial transactions.
Runs every 30 minutes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "financial_ingestion_pipeline",
    default_args=default_args,
    description="Ingest financial transactions from Kafka into S3",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["financial", "ingestion", "kafka"],
)


def run_ingestion(**context):
    import os
    from src.ingestion import KafkaToS3Ingester

    ingester = KafkaToS3Ingester(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        topic=os.environ["KAFKA_TOPIC_TRANSACTIONS"],
        consumer_group="financial-pipeline-cg",
        s3_bucket=os.environ["S3_BUCKET"],
        s3_prefix="financial/raw/transactions",
        batch_size=500,
    )
    try:
        total = ingester.run()
        context["ti"].xcom_push(key="records_ingested", value=total)
    finally:
        ingester.close()


def run_dbt_staging(**context):
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--select", "staging", "--profiles-dir", "/home/airflow/.dbt"],
        capture_output=True, text=True, cwd="/opt/airflow/dbt_models"
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")


ingest_task = PythonOperator(
    task_id="ingest_kafka_to_s3",
    python_callable=run_ingestion,
    dag=dag,
)

dbt_staging = PythonOperator(
    task_id="run_dbt_staging",
    python_callable=run_dbt_staging,
    dag=dag,
)

ingest_task >> dbt_staging
