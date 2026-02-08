with staging_events as (
    select * from {{ ref('stg_ga4_hits') }}
),

daily_aggregation as (
    select
        event_date,
        utm_source,
        utm_medium,
        device_category,
        count(event_id) as event_count,
        count(distinct user_pseudo_id) as active_users,
        count(distinct concat(user_pseudo_id, ga_session_id)) as session_count,
        count(case when event_name = 'page_view' then 1 end) as page_views
    from staging_events
    group by 1, 2, 3, 4
)

select * from daily_aggregation
order by event_date desc