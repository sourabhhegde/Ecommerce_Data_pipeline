-- fct_revenue_hourly: hourly revenue roll-up by product category.
-- Useful for time-series dashboards and anomaly detection.

with orders as (
    select * from {{ ref('fct_orders') }}
    where order_status != 'cancelled'
)

select
    created_hour                                                        as hour,
    created_date                                                        as date,
    product_category,

    -- volume
    count(distinct order_id)                                            as order_count,
    count(distinct customer_id)                                         as unique_customers,
    sum(quantity)                                                       as total_items_sold,

    -- revenue
    sum(total_amount)                                                   as gross_revenue,
    sum(revenue)                                                        as net_revenue,      -- approved only
    avg(total_amount)                                                   as avg_order_value,

    -- status breakdown
    sum(case when order_status = 'delivered'   then total_amount else 0 end) as delivered_revenue,
    sum(case when payment_status = 'approved'  then 1 else 0 end)            as approved_payments,
    sum(case when payment_status = 'declined'  then 1 else 0 end)            as declined_payments

from orders
group by created_hour, created_date, product_category
order by hour desc, product_category
