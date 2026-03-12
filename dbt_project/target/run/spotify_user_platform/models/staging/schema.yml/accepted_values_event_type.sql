
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "spotify_iam"."main"."stg_auth_events"
    group by event_type

)

select *
from all_values
where value_field not in (
    'login_attempt','login_success','login_failure_bad_password','login_failure_account_locked','mfa_triggered','mfa_success','mfa_failure','mfa_timeout','token_issued','token_refresh','session_active','session_expired','logout','password_reset_requested','password_reset_email_sent','password_reset_completed','password_reset_failed','password_reset_expired'
)



  
  
      
    ) dbt_internal_test