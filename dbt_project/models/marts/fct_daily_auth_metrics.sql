-- fct_daily_auth_metrics.sql
-- Daily aggregate of auth performance metrics.
-- Primary table for BI dashboards and executive reporting.

with sessions as (
    select * from {{ ref('int_user_sessions') }}
),

funnel as (
    select * from {{ ref('int_auth_funnel') }}
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