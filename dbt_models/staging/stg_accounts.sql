-- stg_accounts.sql
-- Cleaned and deduped account dimension

{{
    config(materialized='table')
}}

with source as (
    select * from {{ source('raw', 'accounts') }}
),

cleaned as (
    select
        trim(account_id)            as account_id,
        trim(account_name)          as account_name,
        upper(trim(account_type))   as account_type,
        upper(trim(status))         as status,
        cast(opened_date as date)   as opened_date,
        loaded_at
    from source
    where account_id is not null
),

deduped as (
    select *,
        row_number() over (
            partition by account_id
            order by loaded_at desc
        ) as rn
    from cleaned
)

select
    account_id,
    account_name,
    account_type,
    status,
    opened_date,
    loaded_at
from deduped
where rn = 1
