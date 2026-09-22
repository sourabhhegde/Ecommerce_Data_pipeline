with source as (
    select * from {{ source('raw', 'payments') }}
),

cleaned as (
    select
        payment_id,
        order_id,
        customer_id,
        lower(payment_type)         as payment_type,
        coalesce(installments, 1)   as installments,
        amount,
        lower(status)               as payment_status,
        created_at::timestamptz     as created_at,
        loaded_at::timestamptz      as loaded_at
    from source
    where payment_id is not null
      and amount      > 0
)

select * from cleaned
