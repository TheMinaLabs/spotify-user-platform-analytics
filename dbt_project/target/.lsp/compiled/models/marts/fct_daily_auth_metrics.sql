with __dbt__cte__int_user_sessions as (
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
), __dbt__cte__int_auth_funnel as (
-- int_auth_funnel.sql
-- Builds a per-user, per-day funnel view tracking each step of the auth flow.
-- Used downstream to compute conversion rates and identify drop-off points.

with events as (
    select * from "spotify_iam"."main"."stg_auth_events"
),

daily_user_funnel as (
    select
        user_id,
        event_date,
        country_code,
        subscription_type,
        device_category,
        mfa_enabled,

        -- Step 1: User initiated login
        max(case when event_type = 'login_attempt'              then 1 else 0 end) as step1_login_attempted,

        -- Step 2: Did they hit MFA?
        max(case when event_type = 'mfa_triggered'              then 1 else 0 end) as step2_mfa_triggered,

        -- Step 3: MFA passed (or skipped)
        max(case when event_type = 'mfa_success'                then 1 else 0 end) as step3_mfa_passed,

        -- Step 4: Successful login
        max(case when is_successful_login                       then 1 else 0 end) as step4_login_success,

        -- Step 5: Token issued / active session
        max(case when event_type = 'token_issued'               then 1 else 0 end) as step5_token_issued,

        -- Failure signals
        max(case when event_type = 'login_failure_bad_password'  then 1 else 0 end) as failed_bad_password,
        max(case when event_type = 'login_failure_account_locked' then 1 else 0 end) as failed_account_locked,
        max(case when event_type = 'mfa_failure'                 then 1 else 0 end) as failed_mfa,
        max(case when event_type = 'mfa_timeout'                 then 1 else 0 end) as timed_out_mfa,

        count(distinct session_id)                                                  as session_count

    from events
    group by 1, 2, 3, 4, 5, 6
),

with_conversions as (
    select
        *,
        -- Core conversion: did this user successfully log in today?
        case when step4_login_success = 1 then 1 else 0 end   as converted,

        -- Where did non-converters drop off?
        case
            when step4_login_success = 1                       then 'converted'
            when step2_mfa_triggered = 1 and step3_mfa_passed = 0 then 'dropped_at_mfa'
            when failed_bad_password = 1                       then 'dropped_bad_password'
            when failed_account_locked = 1                     then 'dropped_account_locked'
            when step1_login_attempted = 1                     then 'dropped_unknown'
            else 'no_attempt'
        end as funnel_drop_stage

    from daily_user_funnel
    where step1_login_attempted = 1   -- only include users who tried to log in
)

select * from with_conversions
)
--EPHEMERAL-SELECT-WRAPPER-START
select * from (
-- fct_daily_auth_metrics.sql
-- Daily aggregate of auth performance metrics.
-- Primary table for BI dashboards and executive reporting.

with sessions as (
    select * from __dbt__cte__int_user_sessions
),

funnel as (
    select * from __dbt__cte__int_auth_funnel
),

daily_sessions as (
    select
        session_date                                            as metric_date,
        country_code,
        device_category,
        subscription_type,

        count(distinct session_id)                             as total_sessions,
        count(distinct user_id)                                as unique_users,

        sum(had_successful_login)                              as successful_logins,
        sum(had_auth_failure)                                  as sessions_with_failure,
        sum(case when session_outcome = 'clean_success' then 1 else 0 end)        as clean_successes,
        sum(case when session_outcome = 'success_after_retry' then 1 else 0 end)  as successes_after_retry,
        sum(case when session_outcome = 'failed' then 1 else 0 end)               as failed_sessions,
        sum(case when session_outcome = 'mfa_blocked' then 1 else 0 end)          as mfa_blocked_sessions,
        sum(case when session_outcome = 'password_reset' then 1 else 0 end)       as password_reset_sessions,

        avg(session_duration_seconds)                          as avg_session_duration_secs,
        avg(error_event_count)                                 as avg_errors_per_session

    from sessions
    group by 1, 2, 3, 4
),

daily_funnel as (
    select
        event_date                                             as metric_date,
        country_code,
        device_category,
        subscription_type,

        count(distinct user_id)                                as users_attempted_login,
        sum(step2_mfa_triggered)                               as mfa_triggered_count,
        sum(step3_mfa_passed)                                  as mfa_passed_count,
        sum(step4_login_success)                               as login_success_count,
        sum(failed_bad_password)                               as failed_bad_password_count,
        sum(failed_account_locked)                             as failed_account_locked_count,
        sum(failed_mfa)                                        as failed_mfa_count,

        coalesce(
            round(
                sum(step4_login_success)::float / nullif(count(distinct user_id), 0),
                4
            ), 0
        ) as login_conversion_rate,

        coalesce(
            round(
                sum(step3_mfa_passed)::float / nullif(sum(step2_mfa_triggered), 0),
                4
            ), 0
        ) as mfa_pass_rate

    from funnel
    group by 1, 2, 3, 4
)

select
    s.metric_date,
    s.country_code,
    s.device_category,
    s.subscription_type,
    s.total_sessions,
    s.unique_users,
    f.users_attempted_login,
    s.successful_logins,
    s.clean_successes,
    s.successes_after_retry,
    f.login_conversion_rate,
    s.sessions_with_failure,
    s.failed_sessions,
    f.failed_bad_password_count,
    f.failed_account_locked_count,
    f.mfa_triggered_count,
    f.mfa_passed_count,
    f.failed_mfa_count,
    f.mfa_pass_rate,
    s.password_reset_sessions,
    s.mfa_blocked_sessions,
    round(s.avg_session_duration_secs, 2)  as avg_session_duration_secs,
    round(s.avg_errors_per_session, 4)     as avg_errors_per_session

from daily_sessions s
left join daily_funnel f
    on  s.metric_date       = f.metric_date
    and s.country_code      = f.country_code
    and s.device_category   = f.device_category
    and s.subscription_type = f.subscription_type
--EPHEMERAL-SELECT-WRAPPER-END
)