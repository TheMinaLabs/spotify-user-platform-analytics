-- stg_auth_events.sql
-- Staging layer: type-cast, rename, and lightly clean raw IAM auth events.
-- One row per raw event. No business logic here.

with source as (
    select * from {{ source('raw', 'raw_auth_events') }}
),

renamed as (
    select
        event_id,
        session_id,
        user_id,

        -- Normalise event taxonomy
        lower(trim(event_type))                             as event_type,

        -- Device classification
        lower(trim(device_type))                            as device_type,
        case
            when lower(device_type) like 'mobile%' then 'mobile'
            when lower(device_type) like 'desktop%' then 'desktop'
            when lower(device_type) = 'web_browser' then 'web'
            when lower(device_type) = 'smart_tv' then 'smart_tv'
            else 'unknown'
        end                                                 as device_category,

        upper(trim(country))                                as country_code,
        lower(trim(subscription_type))                      as subscription_type,
        mfa_enabled,
        error_code,

        -- Timestamps
        cast(event_ts as timestamp)                         as event_ts,
        date_trunc('day',  cast(event_ts as timestamp))     as event_date,
        date_trunc('hour', cast(event_ts as timestamp))     as event_hour,

        -- Derived flags
        error_code is not null                              as is_error_event,
        event_type in (
            'login_failure_bad_password',
            'login_failure_account_locked',
            'mfa_failure',
            'mfa_timeout'
        )                                                   as is_auth_failure,
        event_type = 'login_success'                        as is_successful_login,
        event_type = 'login_attempt'                        as is_login_attempt

    from source
    where event_id is not null
      and user_id  is not null
      and event_ts is not null
)

select * from renamed
