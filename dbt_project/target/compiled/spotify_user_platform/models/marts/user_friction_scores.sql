-- user_friction_scores.sql
-- Assigns each user a friction score based on their auth history.
-- Higher score = more friction in the login experience.
-- Used by the User Platform team to identify and prioritise UX improvements.

with  __dbt__cte__int_user_sessions as (
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
), sessions as (
    select * from __dbt__cte__int_user_sessions
),

users as (
    select * from "spotify_iam"."main"."stg_users"
),

user_session_history as (
    select
        user_id,

        count(distinct session_id)                                           as total_sessions,
        count(distinct session_date)                                         as active_days,
        min(session_date)                                                    as first_session_date,
        max(session_date)                                                    as last_session_date,

        -- Friction signals
        sum(had_auth_failure)                                                as total_failed_sessions,
        sum(case when session_outcome = 'mfa_blocked'     then 1 else 0 end) as mfa_blocked_count,
        sum(case when session_outcome = 'password_reset'  then 1 else 0 end) as password_reset_count,
        sum(had_successful_login)                                            as successful_sessions,

        -- Rates
        round(
            sum(had_auth_failure)::float / nullif(count(distinct session_id), 0), 4
        )                                                                    as failure_rate,
        round(
            sum(had_successful_login)::float / nullif(count(distinct session_id), 0), 4
        )                                                                    as success_rate,

        max(device_category)                                                 as most_recent_device,
        max(country_code)                                                    as most_recent_country

    from sessions
    group by 1
),

friction_scored as (
    select
        h.*,
        u.subscription_tier,
        u.mfa_enabled,
        u.email_verified,
        u.cohort_month,

        -- Friction score: weighted sum of negative signals (0–100 scale)
        -- Higher = more friction, higher priority for UX team intervention
        least(100, round(
            (h.failure_rate          * 40)   -- failed sessions are the biggest signal
          + (h.mfa_blocked_count     *  5)   -- MFA blocks add friction
          + (h.password_reset_count  * 10)   -- resets indicate forgotten credentials
          + (case when u.mfa_enabled and h.mfa_blocked_count > 0 then 5 else 0 end)
          + (case when not u.email_verified then 5 else 0 end)
        , 2))                                                                as friction_score,

        -- Segment
        case
            when h.failure_rate >= 0.5  then 'high_friction'
            when h.failure_rate >= 0.2  then 'medium_friction'
            else                             'low_friction'
        end                                                                  as friction_segment

    from user_session_history h
    inner join users u using (user_id)
)

select * from friction_scored
order by friction_score desc