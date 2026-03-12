__dbt__cte__int_user_sessions as (
-- int_user_sessions.sql
-- Reconstructs user sessions from raw event streams.
-- A session = contiguous activity under one session_id.
-- Materialised as ephemeral to avoid storing intermediate state.

with events as (
    select * from "spotify_iam"."main"."stg_auth_events"
),

session_boundaries as (
    select
        session_id,
        user_id,
        country_code,
        device_type,
        device_category,
        subscription_type,
        mfa_enabled,

        min(event_ts)               as session_start_ts,
        max(event_ts)               as session_end_ts,
        min(event_date)             as session_date,

        -- Session outcome
        max(case when is_successful_login  then 1 else 0 end)  as had_successful_login,
        max(case when is_auth_failure      then 1 else 0 end)  as had_auth_failure,
        max(case when event_type = 'mfa_triggered' then 1 else 0 end) as mfa_was_triggered,
        max(case when event_type = 'mfa_success'   then 1 else 0 end) as mfa_succeeded,
        max(case when event_type = 'password_reset_requested' then 1 else 0 end) as had_password_reset,

        -- Failure details
        max(error_code)             as last_error_code,
        count(*)                    as total_events,
        sum(case when is_error_event then 1 else 0 end) as error_event_count

    from events
    group by 1, 2, 3, 4, 5, 6, 7
),

with_duration as (
    select
        *,
        datediff('second', session_start_ts, session_end_ts) as session_duration_seconds,

        -- Classify session result
        case
            when had_successful_login = 1 and had_auth_failure = 0  then 'clean_success'
            when had_successful_login = 1 and had_auth_failure = 1  then 'success_after_retry'
            when had_successful_login = 0 and had_password_reset = 1 then 'password_reset'
            when had_successful_login = 0 and mfa_was_triggered = 1
                 and mfa_succeeded = 0                               then 'mfa_blocked'
            when had_successful_login = 0                            then 'failed'
            else 'other'
        end as session_outcome

    from session_boundaries
)

select * from with_duration
)