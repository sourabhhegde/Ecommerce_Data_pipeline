-- fct_orders: one row per order, enriched with its most recent payment.
-- Revenue is recognised only when payment_status = 'approved'.

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

-- Keep only the latest payment per order (handles retries / duplicate events)
latest_payment as (
    select distinct on (order_id)
        order_id,
        payment_id,
        payment_type,
        installments,
        amount          as payment_amount,
        payment_status
    from payments
    order by order_id, created_at desc
),

final as (
    select
        -- order dimensions
        o.order_id,
        o.customer_id,
        o.product_id,
        o.seller_id,
        o.product_category,
        o.order_status,
        o.payment_method,
        o.created_at,
        o.created_hour,
        o.created_date,

        -- order measures
        o.quantity,
        o.unit_price,
        o.total_amount,

        -- payment dimensions
        p.payment_id,
        p.payment_type,
        p.installments,
        p.payment_status,

        -- payment measures
        p.payment_amount,

        -- derived: revenue recognised on approval only
        case
            when p.payment_status = 'approved' then o.total_amount
            else 0
        end as revenue
    from orders o
    left join latest_payment p on o.order_id = p.order_id
)

select * from final
