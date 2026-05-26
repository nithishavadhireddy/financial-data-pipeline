"""
transactions_expectations.py

Defines the Great Expectations suite for the stg_transactions table.
Run via Airflow or directly: python transactions_expectations.py
"""

import great_expectations as gx
from great_expectations.core.batch import BatchRequest


def build_transactions_suite(context):
    suite_name = "transactions.staging.warning"

    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(
        batch_request=BatchRequest(
            datasource_name="redshift_datasource",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="stg_transactions",
        ),
        expectation_suite_name=suite_name,
    )

    # Row count sanity check
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=10_000_000)

    # No nulls on critical columns
    for col in ["transaction_id", "account_id", "amount", "transaction_ts"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # transaction_id must be unique
    validator.expect_column_values_to_be_unique(column="transaction_id")

    # amount must be positive
    validator.expect_column_values_to_be_between(column="amount", min_value=0.01)

    # currency must be known ISO codes
    validator.expect_column_values_to_be_in_set(
        column="currency",
        value_set=["USD", "EUR", "GBP", "JPY", "CAD", "AUD"],
    )

    # transaction_type whitelist
    validator.expect_column_values_to_be_in_set(
        column="transaction_type",
        value_set=["DEBIT", "CREDIT", "TRANSFER", "FEE", "REVERSAL"],
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"Saved expectation suite: {suite_name}")
    return suite


if __name__ == "__main__":
    ctx = gx.get_context()
    build_transactions_suite(ctx)
