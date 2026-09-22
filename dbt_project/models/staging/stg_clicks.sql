with source as (
    select * from {{ source('raw', 'clicks') }}
),

cleaned as (
    select
        click_id,
        session_id,
        user_id,
        product_id,
        lower(category)                                      as category,
        lower(action)                                        as click_action,
        lower(device_type)                                   as device_type,
        page_url,
        event_timestamp::timestamptz                         as event_timestamp,
        date_trunc('hour', event_timestamp::timestamptz)     as event_hour,
        date_trunc('day',  event_timestamp::timestamptz)     as event_date,
        loaded_at::timestamptz                               as loaded_at
    from source
    where click_id is not null
      and user_id  is not null
)

select * from cleaned
