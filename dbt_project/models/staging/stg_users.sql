-- stg_users.sql
-- Staging layer: clean and type-cast raw user account data.

with source as (
    select * from {{ source('raw', 'raw_users') }}
),

renamed as (
    select
        user_id,
        cast(created_at as timestamp)                   as created_at,
        upper(trim(country))                            as country_code,
        lower(trim(subscription_type))                  as subscription_type,
        age_group,
        email_verified,
        mfa_enabled,

        -- Derived
        case
            when lower(subscription_type) = 'free'               then 'free'
            when lower(subscription_type) like 'premium%'        then 'premium'
            else 'unknown'
        end                                             as subscription_tier,

        date_trunc('month', cast(created_at as timestamp)) as cohort_month

    from source
    where user_id is not null
)

select * from renamed