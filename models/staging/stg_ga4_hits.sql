with source as (
    select * from {{ ref('raw_ga4_export') }}
),

renamed as (
    select
        -- 1. Metadata & IDs
        md5(concat(user_pseudo_id, event_timestamp::text, event_name)) as event_id,
        to_timestamp(event_timestamp / 1000000) as event_at,
        to_date(event_date, 'YYYYMMDD') as event_date,
        event_name,
        user_pseudo_id,
        platform,

        -- 2. Unpacking "Single Object" JSON (Device, Geo, Traffic)
        -- We have to reach into the 'device' key first
        (device::jsonb -> 'device') ->> 'category' as device_category,
        (device::jsonb -> 'device') ->> 'operating_system' as device_os,
        (device::jsonb -> 'device') ->> 'mobile_brand_name' as device_brand,
        
        (geo::jsonb -> 'geo') ->> 'country' as geo_country,
        (geo::jsonb -> 'geo') ->> 'region' as geo_region,
        (geo::jsonb -> 'geo') ->> 'city' as geo_city,

        (traffic_source::jsonb -> 'traffic_source') ->> 'source' as utm_source,
        (traffic_source::jsonb -> 'traffic_source') ->> 'medium' as utm_medium,
        (traffic_source::jsonb -> 'traffic_source') ->> 'name' as utm_campaign,

        -- 3. Unpacking "Array" JSON (event_params)
        -- We reach into the 'event_params' key, then extract elements
        (
            select params -> 'value' ->> 'string_value'
            from jsonb_array_elements(event_params::jsonb -> 'event_params') as params
            where params ->> 'key' = 'page_location'
            limit 1
        ) as page_location,

        (
            select (params -> 'value' ->> 'int_value')::bigint
            from jsonb_array_elements(event_params::jsonb -> 'event_params') as params
            where params ->> 'key' = 'ga_session_id'
            limit 1
        ) as ga_session_id,

        (
            select params -> 'value' ->> 'string_value'
            from jsonb_array_elements(event_params::jsonb -> 'event_params') as params
            where params ->> 'key' = 'page_title'
            limit 1
        ) as page_title

    from source
)

select * from renamed