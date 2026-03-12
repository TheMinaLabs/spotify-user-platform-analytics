
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mfa_pass_rate
from "spotify_iam"."main"."fct_daily_auth_metrics"
where mfa_pass_rate is null



  
  
      
    ) dbt_internal_test