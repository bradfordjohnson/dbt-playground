{{ config(materialized='table') }}

with FlatEvents as (
    select
        event_date,
        to_timestamp(event_timestamp / 1000000) as event_at,
        event_name,
        user_pseudo_id,
        (geo::jsonb -> 'geo') ->> 'city' as geo_city,
        (geo::jsonb -> 'geo') ->> 'country' as geo_country,
        (geo::jsonb -> 'geo') ->> 'continent' as geo_continent,
        (geo::jsonb -> 'geo') ->> 'region' as geo_region,
        (geo::jsonb -> 'geo') ->> 'sub_continent' as geo_sub_continent,
        (geo::jsonb -> 'geo') ->> 'metro' as geo_metro,
        (device::jsonb -> 'device') ->> 'category' as device_category,
        (device::jsonb -> 'device') ->> 'mobile_brand_name' as device_brand,
        (device::jsonb -> 'device') ->> 'mobile_model_name' as device_model,
        (device::jsonb -> 'device') ->> 'operating_system' as device_os,
        (device::jsonb -> 'device') ->> 'language' as device_language,
        (device::jsonb -> 'device' -> 'web_info') ->> 'browser' as device_browser,
        (traffic_source::jsonb -> 'traffic_source') ->> 'name' as traffic_campaign,
        (traffic_source::jsonb -> 'traffic_source') ->> 'medium' as traffic_medium,
        (traffic_source::jsonb -> 'traffic_source') ->> 'source' as traffic_source,
        concat(user_pseudo_id, '.', {{ unnest_key('event_params', 'ga_session_id', 'int_value') }}) as session_id,
        {{ unnest_key('event_params', 'ga_session_number', 'int_value') }} as session_number,
        {{ unnest_key('event_params', 'engaged_session_event', 'int_value') }} as engaged_session_event,
        {{ unnest_key('event_params', 'page_location', 'string_value') }} as page_location,
        {{ unnest_key('event_params', 'page_referrer', 'string_value') }} as page_referrer,
        {{ unnest_key('event_params', 'page_title', 'string_value') }} as page_title,
        substring({{ unnest_key('event_params', 'page_location', 'string_value') }} from '^(?:https?://)?(?:www\.)?([^/]+)') as page_host,
        regexp_replace(
            regexp_replace(
                split_part({{ unnest_key('event_params', 'page_location', 'string_value') }}, '?', 1), 
                '^https?://[^/]+', ''
            ), 
            '/+', '/', 'g'
        ) as page_path,
        row_number() over (
            partition by user_pseudo_id, {{ unnest_key('event_params', 'ga_session_id', 'int_value') }} 
            order by event_timestamp asc
        ) as event_number_in_session

    from {{ ref("raw_ga4_export") }}
)

select 
    *,
    case 
        when page_path = '' then '/'
        when right(page_path, 1) = '/' then page_path 
        else page_path || '/' 
    end as page_path_formatted
from FlatEvents