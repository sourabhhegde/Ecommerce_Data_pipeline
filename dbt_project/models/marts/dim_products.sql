-- dim_products: product dimension enriched with sales + engagement metrics.
-- Combines order data with click/interaction data to compute conversion rates.

with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status != 'cancelled'
),

clicks as (
    select * from {{ ref('stg_clicks') }}
),

product_sales as (
    select
        product_id,
        product_category,
        count(distinct order_id)            as total_orders,
        count(distinct customer_id)         as unique_buyers,
        sum(quantity)                       as total_units_sold,
        sum(total_amount)                   as total_revenue,
        avg(unit_price)                     as avg_selling_price,
        min(unit_price)                     as min_price,
        max(unit_price)                     as max_price
    from orders
    group by product_id, product_category
),

product_engagement as (
    select
        product_id,
        count(*)                                                    as total_clicks,
        count(distinct session_id)                                  as unique_sessions,
        count(distinct user_id)                                     as unique_viewers,
        count(case when click_action = 'add_to_cart' then 1 end)   as add_to_cart_count,
        count(case when click_action = 'wishlist'    then 1 end)   as wishlist_count
    from clicks
    group by product_id
),

final as (
    select
        -- dimensions
        ps.product_id,
        ps.product_category,

        -- sales metrics
        ps.total_orders,
        ps.unique_buyers,
        ps.total_units_sold,
        ps.total_revenue,
        ps.avg_selling_price,
        ps.min_price,
        ps.max_price,

        -- engagement metrics
        coalesce(pe.total_clicks,     0)   as total_clicks,
        coalesce(pe.unique_viewers,   0)   as unique_viewers,
        coalesce(pe.add_to_cart_count, 0)  as add_to_cart_count,
        coalesce(pe.wishlist_count,   0)   as wishlist_count,

        -- derived: conversion rate (buyers / viewers)
        case
            when coalesce(pe.unique_viewers, 0) = 0 then 0
            else round(ps.unique_buyers::numeric / pe.unique_viewers * 100, 2)
        end as conversion_rate_pct,

        -- derived: add-to-cart rate (add_to_cart clicks / total clicks)
        case
            when coalesce(pe.total_clicks, 0) = 0 then 0
            else round(pe.add_to_cart_count::numeric / pe.total_clicks * 100, 2)
        end as add_to_cart_rate_pct

    from product_sales ps
    left join product_engagement pe on ps.product_id = pe.product_id
)

select * from final
