__dbt__cte__int_auth_funnel as (
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