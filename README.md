# financial-data-pipeline

Real-time financial data pipeline built with Apache Kafka, Apache Airflow, dbt, AWS S3/Redshift,
and Great Expectations. Designed to replace legacy batch ETL jobs with a low-latency,
quality-validated data platform.

## Architecture

```
Kafka Topics  →  Airflow (ingest DAG)  →  S3 (raw zone)
                                               ↓
                               dbt (staging + mart models)
                                               ↓
                             Redshift (analytics warehouse)
                                               ↓
                        Great Expectations (quality checks)
```

## Tech Stack

| Layer            | Tool                          |
|------------------|-------------------------------|
| Streaming        | Apache Kafka                  |
| Orchestration    | Apache Airflow 2.8            |
| Raw Storage      | AWS S3                        |
| Transformations  | dbt (data build tool) 1.7     |
| Warehouse        | AWS Redshift                  |
| Data Quality     | Great Expectations 0.18       |
| Containerization | Docker / Docker Compose       |

## Project Structure

```
financial-data-pipeline/
├── dags/
│   ├── financial_ingestion_dag.py   # Kafka → S3 ingestion
│   └── data_quality_dag.py          # GE validation DAG
├── dbt_models/
│   ├── staging/
│   │   ├── stg_transactions.sql
│   │   └── stg_accounts.sql
│   └── marts/
│       ├── fct_daily_transactions.sql
│       └── dim_accounts.sql
├── great_expectations/
│   └── transactions_expectations.py
├── src/
│   ├── ingestion.py
│   └── transformations.py
└── config/
    └── pipeline_config.yaml.example
```

## Getting Started

```bash
git clone https://github.com/<your-username>/financial-data-pipeline
cd financial-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Set up config
cp config/pipeline_config.yaml.example config/pipeline_config.yaml
# Fill in your AWS credentials, Kafka brokers, Redshift connection

# Start Airflow locally (dev mode)
airflow db init
airflow webserver -p 8080 &
airflow scheduler &

# Trigger ingestion manually
airflow dags trigger financial_ingestion_pipeline
```

## dbt Setup

```bash
cd dbt_models
dbt deps
dbt run --select staging
dbt run --select marts
dbt test
```

## Data Quality

Great Expectations suites run as a downstream Airflow task after each ingestion cycle.
Checks include:
- Row count above minimum threshold
- No nulls on `transaction_id`, `account_id`, `amount`
- `amount` values within expected range
- Schema consistency across batches

## Notes

- Kafka consumer is configured for at-least-once delivery; deduplication handled in dbt staging layer
- Redshift COPY command used for bulk loads from S3 (much faster than row-by-row inserts)
- dbt models use incremental materialization to avoid full table scans
