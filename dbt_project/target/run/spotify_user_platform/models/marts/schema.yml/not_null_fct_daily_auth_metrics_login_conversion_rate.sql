
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select login_conversion_rate
from "spotify_iam"."main"."fct_daily_auth_metrics"
where login_conversion_rate is null



  
  
      
    ) dbt_internal_test