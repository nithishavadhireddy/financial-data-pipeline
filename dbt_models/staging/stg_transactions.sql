-- stg_transactions.sql
-- Cleans raw transaction records: casts types, strips whitespace, deduplicates

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='fail'
    )
}}

with source as (
    select * from {{ source('raw', 'transactions') }}

    {% if is_incremental() %}
        where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        trim(transaction_id)                        as transaction_id,
        trim(account_id)                            as account_id,
        cast(amount as decimal(18, 4))              as amount,
        upper(trim(currency))                       as currency,
        upper(trim(transaction_type))               as transaction_type,
        cast(transaction_ts as timestamp)           as transaction_ts,
        date(cast(transaction_ts as timestamp))     as transaction_date,
        loaded_at
    from source
    where transaction_id is not null
      and account_id    is not null
      and amount        is not null
      and amount        > 0
),

deduped as (
    select *,
        row_number() over (
            partition by transaction_id
            order by loaded_at desc
        ) as rn
    from cleaned
)

select
    transaction_id,
    account_id,
    amount,
    currency,
    transaction_type,
    transaction_ts,
    transaction_date,
    loaded_at
from deduped
where rn = 1
