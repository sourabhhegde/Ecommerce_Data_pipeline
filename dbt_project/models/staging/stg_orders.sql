with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        product_id,
        seller_id,
        product_category,
        quantity,
        unit_price,
        total_amount,
        lower(order_status)                             as order_status,
        lower(payment_method)                           as payment_method,
        created_at::timestamptz                         as created_at,
        date_trunc('hour', created_at::timestamptz)     as created_hour,
        date_trunc('day',  created_at::timestamptz)     as created_date,
        loaded_at::timestamptz                          as loaded_at
    from source
    where order_id     is not null
      and customer_id  is not null
      and total_amount  > 0
)

select * from cleaned
