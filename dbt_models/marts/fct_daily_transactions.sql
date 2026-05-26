-- fct_daily_transactions.sql
-- Daily aggregate fact table for financial reporting

{{
    config(
        materialized='incremental',
        unique_key=['transaction_date', 'account_id', 'transaction_type'],
        on_schema_change='fail'
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
        where transaction_date >= (select max(transaction_date) - interval '3 days' from {{ this }})
    {% endif %}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

aggregated as (
    select
        t.transaction_date,
        t.account_id,
        a.account_type,
        t.transaction_type,
        t.currency,
        count(*)                    as transaction_count,
        sum(t.amount)               as total_amount,
        avg(t.amount)               as avg_amount,
        min(t.amount)               as min_amount,
        max(t.amount)               as max_amount
    from transactions t
    left join accounts a using (account_id)
    group by 1, 2, 3, 4, 5
)

select * from aggregated
